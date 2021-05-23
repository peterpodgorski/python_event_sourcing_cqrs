import pytest
from money import Money

from domain import Account
from internal_dsl import PrivateBankingDSL, Driver


@pytest.fixture
def account() -> Account:
    return Account(deposit=Money(100, "PLN"))


@pytest.fixture
def private_banking() -> PrivateBankingDSL:
    return PrivateBankingDSL(driver=Driver())
