from abc import ABC
from collections import deque
from functools import singledispatchmethod
from typing import Deque, Type, Tuple, TypeVar
from uuid import UUID, uuid4

import attr
from money import Money


@attr.s(frozen=True, kw_only=True)
class Event:
    producer_id: UUID = attr.ib()


@attr.s(frozen=True, kw_only=True)
class AccountCreated(Event):
    deposit: Money = attr.ib()


@attr.s(frozen=True, kw_only=True)
class MoneyWithdrawn(Event):
    amount: Money = attr.ib()


class UnknownEvent(Exception):
    pass


class NotEnoughMoney(Exception):
    pass


TEntity = TypeVar("TEntity", bound="Entity")


class Entity(ABC):
    def __init__(self) -> None:
        self._constructor()

    def _constructor(self) -> None:
        self._id: UUID
        self._changes: Deque[Event] = deque()

    @classmethod
    def construct(cls: Type[TEntity]) -> TEntity:
        ag = object.__new__(cls)
        ag._constructor()
        return ag

    @property
    def id(self) -> UUID:
        return self._id

    @property
    def uncommitted_changes(self) -> Tuple[Event, ...]:
        return tuple(self._changes)

    # other good names: apply_change, trigger, handle, etc...
    def _take(self, event: Event) -> None:
        self._apply(event)
        self._changes.append(event)

    def _apply(self, event: Event) -> None:
        raise UnknownEvent(event)

    def hydrate(self, event: Event) -> None:
        self._apply(event)


class Account(Entity):
    def __init__(self, deposit: Money) -> None:
        super().__init__()
        self._take(AccountCreated(producer_id=uuid4(), deposit=deposit))

    @singledispatchmethod
    def _apply(self, event) -> None:
        super()._apply(event)

    @_apply.register
    def _(self, event: AccountCreated) -> None:
        self._id = event.producer_id
        self._balance = event.deposit

    def withdraw(self, amount: Money) -> None:
        if self._balance >= amount:
            self._take(MoneyWithdrawn(producer_id=self._id, amount=amount))
        else:
            raise NotEnoughMoney()

    @_apply.register
    def _(self, event: MoneyWithdrawn) -> None:
        self._balance -= event.amount
