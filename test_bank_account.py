import pytest


class PrivateBankingDSL:
    def __init__(self) -> None:
        pass

    def have(self, amount: str, currency: str) -> None:
        pass

    def withdraw_money(self, amount: str, currency: str) -> None:
        pass

    def assert_have(self, amount: str, currency: str) -> None:
        assert False


@pytest.fixture
def private_banking() -> PrivateBankingDSL:
    return PrivateBankingDSL()


def test_withdraw_money(private_banking: PrivateBankingDSL):
    private_banking.have("5000.00", "PLN")
    private_banking.withdraw_money("1500.00", "PLN")
    private_banking.assert_have("3500.00", "PLN")
