from typing import (
    cast,
)

import attr
import pytest
from money import Money

from domain import (
    AccountCreated,
    MoneyWithdrawn,
    NotEnoughMoney,
    Account,
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
