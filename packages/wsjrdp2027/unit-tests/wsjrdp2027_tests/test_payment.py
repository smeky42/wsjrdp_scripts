"""Payment dataframes (wsjrdp2027._payment)."""

import pandas as pd
from wsjrdp2027 import split_off_credit_transfer_payers


class Test_split_off_credit_transfer_payers:
    def _df(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "id": [1, 2, 3],
                "installments_payment_method": [
                    "direct_debit",
                    "credit_transfer",
                    "direct_debit",
                ],
                "payment_status": ["ok", "skipped", "skipped"],
                "payment_status_reason": [
                    "",
                    "payment_method=credit_transfer, No IBAN",
                    "No IBAN",
                ],
                "open_amount_cents": [10_000, 20_000, 30_000],
            }
        )

    def test_splits_off_credit_transfer_payers_only(self):
        rest, credit_transfer = split_off_credit_transfer_payers(self._df())

        assert list(rest["id"]) == [1, 3]
        assert list(credit_transfer["id"]) == [2]

    def test_marks_them_credit_transfer_and_keeps_the_reason(self):
        _, credit_transfer = split_off_credit_transfer_payers(self._df())

        row = credit_transfer.iloc[0]
        assert row["payment_status"] == "credit_transfer"
        assert row["payment_status_reason"] == "payment_method=credit_transfer, No IBAN"

    def test_leaves_a_direct_debit_payer_without_iban_skipped(self):
        rest, _ = split_off_credit_transfer_payers(self._df())

        assert rest.set_index("id").loc[3, "payment_status"] == "skipped"

    def test_leaves_the_input_unchanged(self):
        df = self._df()
        split_off_credit_transfer_payers(df)

        assert list(df["payment_status"]) == ["ok", "skipped", "skipped"]


class Test_find_direct_debit_mandate_differences:
    def _df(self, **overrides) -> pd.DataFrame:
        import datetime

        row = {
            "id": 115,
            "payment_status": "ok",
            "pn_mandate_id": "wsjrdp2027115",
            "calculated_sepa_mandate_id": "wsjrdp2027115",
            "pn_dbtr_iban": "DE02120300000000202051",
            "calculated_sepa_iban": "DE02120300000000202051",
            "pn_mandate_date": datetime.date(2025, 8, 28),
            "calculated_sepa_mandate_date": datetime.date(2025, 8, 28),
        }
        row.update(overrides)
        return pd.DataFrame([row])

    def _ids(self, df) -> list:
        from wsjrdp2027._payment import find_direct_debit_mandate_differences

        return list(find_direct_debit_mandate_differences(df)["id"])

    def test_same_mandate(self):
        assert self._ids(self._df()) == []

    def test_iban_compared_without_spaces_and_case(self):
        assert self._ids(self._df(pn_dbtr_iban="de02 1203 0000 0000 2020 51")) == []

    def test_new_mandate_reference_of_the_person(self):
        assert self._ids(self._df(calculated_sepa_mandate_id="wsjrdp2027115b")) == [115]

    def test_new_iban_of_the_person(self):
        assert self._ids(self._df(calculated_sepa_iban="DE89370400440532013000")) == [
            115
        ]

    def test_skipped_debit_is_no_hint(self):
        df = self._df(
            payment_status="skipped", calculated_sepa_mandate_id="wsjrdp2027115b"
        )
        assert self._ids(df) == []

    def test_without_pre_notification_data(self):
        df = pd.DataFrame([{"id": 1, "payment_status": "ok"}])
        assert self._ids(df) == []
