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
