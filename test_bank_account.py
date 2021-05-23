from abc import ABC
from collections import deque
from functools import singledispatchmethod
from typing import (
    cast,
    Tuple,
    Deque,
)
from uuid import UUID, uuid4

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
from read_model import AccountDTO, BalanceView, Reader, ReadModel


@attr.s
class Command:
    pass


@attr.s
class CreateAccount(Command):
    with_deposit: Money = attr.ib()


@attr.s
class Withdraw(Command):
    account_id: UUID = attr.ib()
    amount: Money = attr.ib()


class UnknownCommand(Exception):
    pass


@attr.s(frozen=True)
class Response(ABC):
    pass


@attr.s(frozen=True)
class CreateAccountResponse(Response):
    new_account_id: UUID = attr.ib()


@attr.s(frozen=True)
class WithdrawResponse(Response):
    pass


class AccountApp:
    def __init__(self, account_repo: Repository) -> None:
        self._account_repo: Repository[Account] = account_repo

    @singledispatchmethod
    def handle(self, command) -> Response:
        raise UnknownCommand(command)

    @handle.register
    def _(self, command: CreateAccount) -> CreateAccountResponse:
        account = Account(deposit=command.with_deposit)
        self._account_repo.save(account)
        return CreateAccountResponse(new_account_id=account.id)

    @handle.register
    def _(self, command: Withdraw) -> WithdrawResponse:
        account: Account = self._account_repo.get(command.account_id)
        account.withdraw(command.amount)
        self._account_repo.save(account)
        return WithdrawResponse()


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


def test_read_model_is_built_from_events(account: Account):
    events: Tuple[Event, ...] = account.uncommitted_changes
    creation_event: AccountCreated = cast(AccountCreated, events[0])

    balance_view: BalanceView = BalanceView()
    balance_view.handle(creation_event)

    account_read_model: AccountDTO = balance_view.for_account(account.id)
    assert account_read_model.balance == Money(100, "PLN")
    assert account_read_model.account_id == account.id


def test_all_streams_reads_from_all_streams_in_order():
    event_1 = AccountCreated(producer_id=uuid4(), deposit=Money("100", "PLN"))
    event_2 = AccountCreated(producer_id=uuid4(), deposit=Money("200", "PLN"))
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


def test_event_reader_remembers_last_update_position():
    event_1 = AccountCreated(producer_id=uuid4(), deposit=Money("100", "PLN"))
    event_2 = MoneyWithdrawn(producer_id=event_1.producer_id, amount=Money("10", "PLN"))

    event_store: EventStore = EventStore()
    event_store.store(event_1.producer_id, [event_1])

    class EventCollector:
        def __init__(self) -> None:
            self.handled_events: Deque[Event] = deque()

        def handle(self, event: Event) -> None:
            self.handled_events.append(event)

    collector = EventCollector()

    reader: Reader = Reader(event_store)
    reader.register(cast(ReadModel, collector), AccountCreated, MoneyWithdrawn)

    reader.update_all()

    event_store.store(event_2.producer_id, [event_2])

    reader.update_all()

    assert collector.handled_events == deque((event_1, event_2))


class Driver:
    def __init__(self) -> None:
        event_store = EventStore()
        self._app = AccountApp(Repository[Account](Account, event_store))

        self._balance_view: BalanceView = BalanceView()

        self._reader = Reader(event_store=event_store)
        self._reader.register(self._balance_view, AccountCreated, MoneyWithdrawn)

    def create_account(self, initial_deposit: Money) -> UUID:
        command = CreateAccount(with_deposit=initial_deposit)
        response: CreateAccountResponse = cast(
            CreateAccountResponse, self._app.handle(command)
        )

        # Simulating event store reader in a different container
        self._reader.update_all()

        return response.new_account_id

    def withdraw(self, from_account: UUID, amount: Money) -> None:
        command = Withdraw(account_id=from_account, amount=amount)
        self._app.handle(command)

        # Simulating event store reader in a different container
        self._reader.update_all()

    def check_balance(self, for_account: UUID) -> Money:
        account_read_model: AccountDTO = self._balance_view.for_account(for_account)
        return account_read_model.balance


class PrivateBankingDSL:
    def __init__(self, driver: Driver) -> None:
        self._account_id: UUID
        self._driver = driver

    def have(self, amount: str, currency: str) -> None:
        money = Money(amount, currency)
        self._account_id = self._driver.create_account(initial_deposit=money)

    def withdraw_money(self, amount: str, currency: str) -> None:
        money = Money(amount, currency)
        self._driver.withdraw(from_account=self._account_id, amount=money)

    def assert_have(self, amount: str, currency: str) -> None:
        balance: Money = self._driver.check_balance(self._account_id)
        assert balance == Money(amount, currency)


@pytest.fixture
def private_banking() -> PrivateBankingDSL:
    return PrivateBankingDSL(driver=Driver())


def test_withdraw_money(private_banking: PrivateBankingDSL):
    private_banking.have("5000.00", "PLN")
    private_banking.withdraw_money("1500.00", "PLN")
    private_banking.assert_have("3500.00", "PLN")
