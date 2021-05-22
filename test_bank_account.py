import uuid
from abc import ABC, abstractmethod
from collections import defaultdict
from functools import singledispatchmethod
from typing import (
    cast,
    Tuple,
    Iterable,
    Dict,
    Type,
    List,
)
from uuid import UUID

import attr
import pytest
from money import Money

from domain import (
    AccountCreated,
    MoneyWithdrawn,
    NotEnoughMoney,
    Account,
    Event,
)
from persistance import EventStore, Repository


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


@pytest.fixture
def account() -> Account:
    return Account(deposit=Money(100, "PLN"))


def test_creating_account_emits_AccountCreated_event(account: Account):
    events = account.uncommitted_changes

    assert len(events) == 1
    event: AccountCreated = cast(AccountCreated, events[0])
    assert type(event) == AccountCreated
    assert event.producer_id == account.id
    assert event.deposit == Money(100, "PLN")


def test_withdraw_is_not_possible_with_too_little_money(account: Account):
    with pytest.raises(NotEnoughMoney):
        account.withdraw(Money(200, "PLN"))


def test_withdraw_is_successful_with_enough_money(account: Account):
    account.withdraw(Money(10, "PLN"))
    events = account.uncommitted_changes

    assert len(events) == 2
    withdraw_event: MoneyWithdrawn = cast(MoneyWithdrawn, events[-1])
    assert withdraw_event.amount == Money(10, "PLN")


def test_entity_can_be_saved_and_restored(account: Account):
    repo: Repository[Account] = Repository(Account, EventStore())
    repo.save(account)

    retrieved: Account = repo.get(account.id)

    assert retrieved.id == account.id


def test_a_stream_of_events_can_be_saved_into_EventStore_and_retrieved_from_it(
    account,
):  # noqa
    account.withdraw(amount=Money(10, "PLN"))
    produced_events = account.uncommitted_changes

    event_store: EventStore = EventStore()
    event_store.store(account.id, produced_events)
    retrieved_events = event_store.all_events_for(account.id)

    assert tuple(retrieved_events) == produced_events


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

    def for_account(self, account_id: UUID) -> AccountDTO:
        return AccountDTO(account_id=account_id, balance=self._storage[account_id])


def test_read_model_is_built_from_events(account: Account):
    events: Tuple[Event, ...] = account.uncommitted_changes
    creation_event: AccountCreated = cast(AccountCreated, events[0])

    balance_view: BalanceView = BalanceView()
    balance_view.handle(creation_event)

    account_read_model: AccountDTO = balance_view.for_account(account.id)
    assert account_read_model.balance == Money(100, "PLN")
    assert account_read_model.account_id == account.id


class Reader:
    def __init__(self, event_store: EventStore) -> None:
        self._handlers: Dict[Type[Event], List[ReadModel]] = defaultdict(list)
        self._event_store: EventStore = event_store

    def register(self, read_model: ReadModel, event: Type[Event]) -> None:
        self._handlers[event].append(read_model)

    def update_all(self) -> None:
        for event in self._event_store.all_streams():
            for handler in self._handlers[type(event)]:
                handler.handle(event)


def test_all_streams_reads_from_all_streams_in_order():
    event_1 = AccountCreated(producer_id=uuid.uuid4(), deposit=Money("100", "PLN"))
    event_2 = AccountCreated(producer_id=uuid.uuid4(), deposit=Money("200", "PLN"))
    event_3 = MoneyWithdrawn(producer_id=event_1.producer_id, amount=Money("10", "PLN"))

    event_store: EventStore = EventStore()
    event_store.store(event_1.producer_id, [event_1])
    event_store.store(event_2.producer_id, [event_2])
    event_store.store(event_3.producer_id, [event_3])

    assert tuple(event_store.all_streams()) == (event_1, event_2, event_3)


def test_event_handler_builds_read_models_from_event_store(account: Account):
    event_store: EventStore = EventStore()
    repo: Repository[Account] = Repository(Account, event_store)
    repo.save(account)

    balance_view: BalanceView = BalanceView()

    reader: Reader = Reader(event_store)
    reader.register(balance_view, AccountCreated)
    reader.update_all()

    account_read_model: AccountDTO = balance_view.for_account(account.id)
    assert account_read_model.balance == Money(100, "PLN")
    assert account_read_model.account_id == account.id


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
