import sys
from pathlib import Path
from datetime import datetime
from decimal import Decimal
import numpy as np
import pandas as pd

# Ensure the project root is on the path so utils can be imported
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils.data_transformers import transform_numeric_value, transform_datetime_value


def test_transform_numeric_large_currency():
    result = transform_numeric_value(Decimal('123456789012345678901'), field_name='amount')
    assert isinstance(result, float)
    assert result == 999999999999999.99


def test_transform_numeric_large_rate():
    result = transform_numeric_value(1e10, field_name='split_rate')
    assert isinstance(result, float)
    assert result == 9999.9999


def test_transform_numeric_nan_returns_none():
    assert transform_numeric_value(np.nan, field_name='amount') is None


def test_transform_datetime_salesforce_string():
    sf_date = '2023-05-01T10:30:00Z'
    result = transform_datetime_value(sf_date, source_type='salesforce', field_name='CreatedDate')
    assert isinstance(result, str)
    assert result == '2023-05-01 10:30:00'


def test_transform_datetime_nat():
    assert transform_datetime_value(pd.NaT) is None
