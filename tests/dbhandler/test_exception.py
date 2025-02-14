import pytest
from dbhandler.exception import TableNotFoundError


def test_TableNotFoundError() -> None:
    with pytest.raises(TableNotFoundError) as e:
        raise TableNotFoundError("test")
    assert str(e.value) == "table 'test' does not exists yet."
