"""The participation contract of a person, as the people dataframe holds it
(wagon migration 20261006200006)."""

import datetime

import pandas
from wsjrdp2027 import _payment, _people
from wsjrdp2027._models.person import Person


# The columns are timestamps without a time zone, as Rails writes them.
CONFIRMED_AT = datetime.datetime.fromisoformat("2025-12-16T20:00:00")
ENDED_AT = datetime.datetime.fromisoformat("2026-07-07T21:54:04.583311")


def _loaded(rows: list[dict]) -> pandas.DataFrame:
    """The rows as load_people_dataframe makes them a data frame: psycopg
    gives a datetime or None, pandas makes the column datetime64 with NaT
    (object with None where all are empty), the loader plain values again."""
    df = pandas.DataFrame(rows)
    for col in _people.NULLABLE_TIMESTAMP_COLUMNS:
        _people._to_datetime_or_none_column(df, col)
    return df


def _row(contract_status, confirmed_at=None, ended_at=None, id=1) -> dict:
    return {
        "id": id,
        "status": "confirmed",
        "contract_status": contract_status,
        "contract_confirmed_at": confirmed_at,
        "contract_ended_at": ended_at,
    }


class Test_Contract_Columns:
    def test_are_listed_for_people_and_payments(self):
        columns = {"contract_status", "contract_confirmed_at", "contract_ended_at"}
        assert columns <= set(_people.PEOPLE_DATAFRAME_COLUMNS)
        assert columns <= set(_payment.PAYMENT_DATAFRAME_COLUMNS)
        assert set(_people.NULLABLE_TIMESTAMP_COLUMNS) <= columns


class Test_Loaded_Timestamp_Columns:
    def test_hold_a_plain_datetime_or_none_not_timestamp_or_nat(self):
        df = _loaded(
            [
                _row("confirmed", CONFIRMED_AT),
                _row("none", id=2),
                _row("ended", CONFIRMED_AT, ENDED_AT, id=3),
            ]
        )

        for col in _people.NULLABLE_TIMESTAMP_COLUMNS:
            assert df[col].dtype == object
        assert df["contract_confirmed_at"].tolist() == [
            CONFIRMED_AT,
            None,
            CONFIRMED_AT,
        ]
        assert df["contract_ended_at"].tolist() == [None, None, ENDED_AT]
        filled = [v for col in _people.NULLABLE_TIMESTAMP_COLUMNS for v in df[col] if v]
        assert {type(v) for v in filled} == {datetime.datetime}

    def test_a_column_empty_everywhere(self):
        df = _loaded(
            [_row("confirmed", CONFIRMED_AT), _row("confirmed", CONFIRMED_AT, id=2)]
        )

        assert df["contract_ended_at"].tolist() == [None, None]

    def test_reach_the_person_as_they_are(self):
        df = _loaded([_row("confirmed", CONFIRMED_AT), _row("none", id=2)])
        confirmed, none = (Person.from_pandas_row(row) for _, row in df.iterrows())

        assert confirmed.contract_status == "confirmed"
        assert type(confirmed.contract_confirmed_at) is datetime.datetime
        assert confirmed.contract_confirmed_at == CONFIRMED_AT
        assert confirmed.contract_ended_at is None
        assert none.contract_confirmed_at is None


class Test_Person_Without_The_Loader:
    def test_nat_of_a_raw_data_frame_reads_as_none(self):
        df = pandas.DataFrame([_row("confirmed", CONFIRMED_AT), _row("none", id=2)])
        assert type(df["contract_confirmed_at"][1]).__name__ == "NaTType"

        none = Person.from_pandas_row(df.iloc[1])

        assert none.contract_confirmed_at is None
        assert none.contract_ended_at is None
