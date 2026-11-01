import pytest

from pulsecity.config import load_city
from pulsecity.testing.spark import local_spark


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    session = local_spark(str(tmp_path_factory.mktemp("warehouse")), app_name="pulsecity-tests")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def london():
    return load_city("london")
