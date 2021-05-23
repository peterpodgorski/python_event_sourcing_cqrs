from typing import cast
from uuid import UUID

from money import Money

from app import AccountApp, CreateAccount, CreateAccountResponse, Withdraw
from domain import Account, AccountCreated, MoneyWithdrawn
from persistance import EventStore, Repository
from read_model import BalanceView, Reader, AccountDTO


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
