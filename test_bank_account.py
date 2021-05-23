from collections import deque
from typing import (
    cast,
    Tuple,
    Deque,
)
from uuid import uuid4

import pytest
from money import Money

from domain import (
    AccountCreated,
    MoneyWithdrawn,
    NotEnoughMoney,
    Account,
    Event,
)
from internal_dsl import PrivateBankingDSL
from persistance import EventStore, Repository
from read_model import AccountDTO, BalanceView, Reader, ReadModel


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


def test_event_reader_remembers_last_update_position_with_multiple_handers():
    event_1 = AccountCreated(producer_id=uuid4(), deposit=Money("100", "PLN"))
    event_2 = MoneyWithdrawn(producer_id=event_1.producer_id, amount=Money("10", "PLN"))

    event_store: EventStore = EventStore()
    event_store.store(event_1.producer_id, [event_1])

    class EventCollector:
        def __init__(self) -> None:
            self.handled_events: Deque[Event] = deque()

        def handle(self, event: Event) -> None:
            self.handled_events.append(event)

    collector_1 = EventCollector()
    collector_2 = EventCollector()

    reader: Reader = Reader(event_store)
    reader.register(cast(ReadModel, collector_1), AccountCreated, MoneyWithdrawn)
    reader.register(cast(ReadModel, collector_2), AccountCreated, MoneyWithdrawn)

    reader.update_all()
    event_store.store(event_2.producer_id, [event_2])
    reader.update_all()

    assert collector_1.handled_events == deque((event_1, event_2))
    assert collector_2.handled_events == deque((event_1, event_2))


def test_withdraw_money(private_banking: PrivateBankingDSL):
    private_banking.have("5000.00", "PLN")
    private_banking.withdraw_money("1500.00", "PLN")
    private_banking.assert_have("3500.00", "PLN")
