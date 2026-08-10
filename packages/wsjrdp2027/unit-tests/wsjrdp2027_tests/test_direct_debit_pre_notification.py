import decimal as _decimal

from wsjrdp2027._models.direct_debit_pre_notification import DirectDebitPreNotification


class Test_DirectDebitPreNotification:
    def test_accessors(self):
        pn = DirectDebitPreNotification(
            subject_id=42,
            payment_initiation_id=7,
            payment_status="pre_notified",
            amount_cents=12300,
        )
        assert pn.subject_id == 42
        assert pn.get("payment_initiation_id") == 7
        assert pn.is_pre_notified
        assert not pn.is_skipped
        assert pn.amount == _decimal.Decimal("123.00")

    def test_is_skipped(self):
        pn = DirectDebitPreNotification(payment_status="skipped")
        assert pn.is_skipped
        assert not pn.is_pre_notified


class Test_load_for_subject_ids:
    def test_empty_ids_returns_empty_without_db(self):
        # No connection is touched when there are no ids.
        assert DirectDebitPreNotification.load_for_subject_ids(None, []) == []

    def test_builds_expected_sql(self, monkeypatch):
        from wsjrdp2027 import _pg

        captured: dict = {}

        def fake_pg_select_dict_rows(conn, query, *, show_result=None):
            captured["query"] = query
            return []

        monkeypatch.setattr(_pg, "pg_select_dict_rows", fake_pg_select_dict_rows)

        result = DirectDebitPreNotification.load_for_subject_ids(
            None, [5, 3, 1, 3], status=["pre_notified"]
        )
        assert result == []

        sql = captured["query"].as_string()
        assert "FROM wsjrdp_direct_debit_pre_notifications" in sql
        assert "subject_type = 'Person'" in sql
        assert "subject_id = ANY(" in sql
        # ids are sorted and de-duplicated
        assert "'{1,3,5}'" in sql
        assert "payment_status = ANY(" in sql
        assert "'{pre_notified}'" in sql
        assert sql.rstrip().endswith("ORDER BY subject_id, id")

    def test_status_none_omits_payment_status_filter(self, monkeypatch):
        from wsjrdp2027 import _pg

        captured: dict = {}

        def fake_pg_select_dict_rows(conn, query, *, show_result=None):
            captured["query"] = query
            return []

        monkeypatch.setattr(_pg, "pg_select_dict_rows", fake_pg_select_dict_rows)

        DirectDebitPreNotification.load_for_subject_ids(None, [1], status=None)

        sql = captured["query"].as_string()
        # payment_status appears as a selected column, but not as a WHERE filter.
        assert "payment_status = ANY(" not in sql
        assert "WHERE subject_type = 'Person' AND subject_id = ANY(" in sql
