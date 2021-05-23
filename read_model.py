from abc import ABC, abstractmethod
from collections import defaultdict
from functools import singledispatchmethod, partialmethod
from typing import Dict, Type, List, Iterable
from uuid import UUID

import attr
from money import Money

from domain import AccountCreated, Event, MoneyWithdrawn
from persistance import EventStore


@attr.s(frozen=True, kw_only=True)
class AccountDTO:
    account_id: UUID = attr.ib()
    balance: Money = attr.ib()


class ReadModel(ABC):
    @abstractmethod
    def handle(self, event) -> None:
        pass


class BalanceView(ReadModel):
    def __init__(self):
        self._storage: Dict[UUID, Money] = {}

    @singledispatchmethod
    def handle(self, event) -> None:
        pass

    @handle.register
    def _(self, event: AccountCreated) -> None:
        self._storage[event.producer_id] = event.deposit

    @handle.register
    def _(self, event: MoneyWithdrawn) -> None:
        self._storage[event.producer_id] -= event.amount

    def for_account(self, account_id: UUID) -> AccountDTO:
        return AccountDTO(account_id=account_id, balance=self._storage[account_id])


class Reader:
    def __init__(self, event_store: EventStore) -> None:
        self._last_position: int = 0
        self._handlers: Dict[Type[Event], List[ReadModel]] = defaultdict(list)
        self._event_store: EventStore = event_store

    def _new_events(self) -> Iterable[Event]:
        return self._event_store.all_streams(start_at=self._last_position)

    def _mark_handled(self) -> None:
        self._last_position += 1

    def _handlers_for(self, event: Event) -> Iterable[ReadModel]:
        return self._handlers[type(event)]

    def _handle_event(self, event: Event) -> None:
        for handler in self._handlers_for(event):
            handler.handle(event)

    def register(self, read_model: ReadModel, *events: Type[Event]) -> None:
        for e in events:
            self._handlers[e].append(read_model)

    def update_all(self) -> None:
        for event in self._new_events():
            self._handle_event(event)
            self._mark_handled()
