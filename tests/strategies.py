# -*- coding: utf-8 -*-
import pathlib
from datetime import datetime

from hypothesis import assume, given, strategies as st

TEST_ROOT = pathlib.Path(__file__).absolute().parent


def random_sql_file():
    return st.sampled_from(list(TEST_ROOT.joinpath("fixtures/sql").glob("*.sql")))


def random_csv_file():
    return st.sampled_from(list(TEST_ROOT.joinpath("fixtures/csvs").glob("*.csv")))


def date_range():
    return st.datetimes(max_value=datetime(2019, 5, 24), min_value=datetime(2019, 4, 9))


@st.composite
def allowed_date_range(draw, elements=date_range()):
    max_date = draw(elements)
    min_date = draw(st.datetimes(max_value=max_date, min_value=datetime(2019, 4, 9)))
    return (min_date, max_date)
