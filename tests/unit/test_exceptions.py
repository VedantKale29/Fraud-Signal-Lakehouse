"""Stage-0 gate: the exception hierarchy behaves as designed."""

import pytest

from fraud_lakehouse.common.exceptions import (
    DataQualityError,
    IngestionError,
    LakehouseError,
    SchemaContractError,
)


def test_hierarchy():
    assert issubclass(IngestionError, LakehouseError)
    assert issubclass(SchemaContractError, DataQualityError)


def test_captures_origin_file_and_line():
    try:
        try:
            1 / 0
        except ZeroDivisionError as e:
            raise IngestionError("bronze landing failed", e) from e
    except IngestionError as err:
        msg = str(err)
        assert "bronze landing failed" in msg
        assert "test_exceptions.py" in msg          # file captured
        assert "caused_by=ZeroDivisionError" in msg  # original preserved
        assert err.original is not None
        assert "ZeroDivisionError" in err.full_traceback()


def test_works_without_original_exception():
    err = LakehouseError("standalone failure")
    assert "standalone failure" in str(err)
    assert err.original is None


def test_can_be_caught_as_base():
    with pytest.raises(LakehouseError):
        raise DataQualityError("gate failed")
