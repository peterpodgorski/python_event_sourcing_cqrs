import pytest
from money import Money


class Driver:
    def create_account(self, initial_deposit: Money) -> None:
        pass

    def withdraw(self, amount: Money) -> None:
        pass

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
