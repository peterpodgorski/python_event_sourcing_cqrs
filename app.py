from abc import ABC, abstractmethod
from functools import singledispatchmethod
from uuid import UUID

import attr
from money import Money

from domain import Account
from persistance import Repository


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


class App(ABC):
    @abstractmethod
    def handle(self, command) -> Response:
        raise UnknownCommand(command)


class AccountApp(App):
    def __init__(self, account_repo: Repository) -> None:
        self._account_repo: Repository[Account] = account_repo

    @singledispatchmethod
    def handle(self, command) -> Response:
        return super().handle(command)

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
