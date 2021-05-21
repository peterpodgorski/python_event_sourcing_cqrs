from abc import ABC
from collections import deque
from functools import singledispatchmethod
from typing import Tuple, Generic, TypeVar, Deque, cast, overload
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


class Account:
    def __init__(self, deposit: Money) -> None:
        self._changes: Deque[Event] = deque()
        self._take(AccountCreated(producer_id=uuid4(), deposit=deposit))

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

    @singledispatchmethod
    def _apply(self, event) -> None:
        raise UnknownEvent(event)

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
