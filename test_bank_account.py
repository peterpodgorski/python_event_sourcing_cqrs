from abc import ABC
from collections import deque, defaultdict
from functools import singledispatchmethod, reduce
from typing import (
    Tuple,
    Generic,
    TypeVar,
    Deque,
    cast,
    overload,
    Type,
    Dict,
    Sequence,
    Generator,
)
from uuid import UUID, uuid4

import attr
import pytest
from money import Money


@attr.s
class Command:
    pass


@attr.s
class CreateAccount(Command):
    with_deposit: Money = attr.ib()


@attr.s
class Withdraw(Command):
    amount: Money = attr.ib()


class AccountApp:
    def handle(self, command):
        pass


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


def test_creating_account_emits_AccountCreated_event():
    account: Account = Account(deposit=Money(100, "PLN"))

    events = account.uncommitted_changes

    assert len(events) == 1
    event: AccountCreated = cast(AccountCreated, events[0])
    assert type(event) == AccountCreated
    assert event.producer_id == account.id
    assert event.deposit == Money(100, "PLN")


def test_withdraw_is_not_possible_with_too_little_money():
    account: Account = Account(deposit=Money(100, "PLN"))

    with pytest.raises(NotEnoughMoney):
        account.withdraw(Money(200, "PLN"))


def test_withdraw_is_successful_with_enough_money():
    account: Account = Account(deposit=Money(100, "PLN"))

    account.withdraw(Money(10, "PLN"))
    events = account.uncommitted_changes

    assert len(events) == 2
    withdraw_event: MoneyWithdrawn = cast(MoneyWithdrawn, events[-1])
    assert withdraw_event.amount == Money(10, "PLN")


class EventStore:
    def __init__(self) -> None:
        self._event_streams: Dict[UUID, Deque[Event]] = defaultdict(deque)

    def store(self, producer_id: UUID, events: Sequence[Event]) -> None:
        self._event_streams[producer_id].extend(events)

    def all_events_for(self, producer_id: UUID) -> Generator[Event, None, None]:
        return (e for e in self._event_streams[producer_id])


class Repository(Generic[TEntity]):
    def __init__(self, entity_class: Type[Entity], event_store: EventStore) -> None:
        self._entity_class = entity_class
        self._event_store = event_store

    def save(self, account: TEntity) -> None:
        self._event_store.store(account.id, account.uncommitted_changes)

    def _apply_event(self, ag: TEntity, e: Event) -> TEntity:
        ag.hydrate(e)
        return ag

    def get(self, entity_id: UUID) -> TEntity:
        changes = self._event_store.all_events_for(entity_id)
        root: TEntity = self._entity_class.construct()

        final_form: TEntity = reduce(self._apply_event, changes, root)

        return final_form


def test_entity_can_be_saved_and_restored():
    repo: Repository[Account] = Repository(Account, EventStore())

    account: Account = Account(deposit=Money(100, "PLN"))
    repo.save(account)
    retrieved: Account = repo.get(account.id)

    assert retrieved.id == account.id


def test_a_stream_of_events_can_be_saved_into_EventStore_and_retrieved_from_it():  # noqa
    account: Account = Account(deposit=Money(100, "PLN"))
    account.withdraw(amount=Money(10, "PLN"))
    produced_events = account.uncommitted_changes

    event_store: EventStore = EventStore()
    event_store.store(account.id, produced_events)
    retrieved_events = event_store.all_events_for(account.id)

    assert tuple(retrieved_events) == produced_events


class Driver:
    def create_account(self, initial_deposit: Money) -> None:
        command = CreateAccount(with_deposit=initial_deposit)
        app = AccountApp()
        app.handle(command)

    def withdraw(self, amount: Money) -> None:
        command = Withdraw(amount=amount)
        app = AccountApp()
        app.handle(command)

    def check_balance(self) -> Money:
        return Money(0, "PLN")


class PrivateBankingDSL:
    def __init__(self, driver: Driver) -> None:
        self._driver = driver

    def have(self, amount: str, currency: str) -> None:
        money = Money(amount, currency)
        self._driver.create_account(initial_deposit=money)

    def withdraw_money(self, amount: str, currency: str) -> None:
        money = Money(amount, currency)
        self._driver.withdraw(amount=money)

    def assert_have(self, amount: str, currency: str) -> None:
        balance: Money = self._driver.check_balance()
        assert balance == Money(amount, currency)


@pytest.fixture
def private_banking() -> PrivateBankingDSL:
    return PrivateBankingDSL(driver=Driver())


def test_withdraw_money(private_banking: PrivateBankingDSL):
    private_banking.have("5000.00", "PLN")
    private_banking.withdraw_money("1500.00", "PLN")
    private_banking.assert_have("3500.00", "PLN")
