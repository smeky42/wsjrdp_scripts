import datetime as _datetime

import pytest
from wsjrdp2027._models.direct_debit_pre_notification import DirectDebitPreNotification
from wsjrdp2027._models.person import Person, load_pre_notifications_for_people


class Test_Person:
    def test_additional_info(self):
        p = Person()

        p.moss_email = "foo@foo"

        assert p.additional_info_changed
        assert p.additional_info_was == {}
        assert p.additional_info == {"moss_email": "foo@foo"}
        assert p.additional_info_updates_dict() == {"moss_email": [None, "foo@foo"]}

        p.moss_email = None
        assert not p.additional_info_changed
        assert p.additional_info == {}
        assert p.additional_info_updates_dict() == {}

        p.moss_email = "foo@bar"
        assert p.additional_info_changed
        assert p.additional_info == {"moss_email": "foo@bar"}
        assert p.additional_info_updates_dict() == {"moss_email": [None, "foo@bar"]}

        p.moss_email = "foo@baz"
        assert p.additional_info_changed
        assert p.additional_info == {"moss_email": "foo@baz"}
        assert p.additional_info_updates_dict() == {"moss_email": [None, "foo@baz"]}

    def test_moss_user_attributes(self):
        p = Person()
        assert p.moss_status is None
        assert p.moss_phone is None
        assert p.moss_team is None
        assert p.moss_roles == []

        p.moss_status = "ACTIVE"
        p.moss_phone = "+491234"
        p.moss_team = "UL D4"
        p.moss_roles = ["TEAMLEAD", "USER"]
        assert p.moss_status == "ACTIVE"
        assert p.moss_phone == "+491234"
        assert p.moss_team == "UL D4"
        assert p.moss_roles == ["TEAMLEAD", "USER"]
        assert p.additional_info == {
            "moss_status": "ACTIVE",
            "moss_phone": "+491234",
            "moss_team": "UL D4",
            "moss_roles": ["TEAMLEAD", "USER"],
        }

        # moss_roles returns a copy (mutation does not leak back)
        p.moss_roles.append("SUPER_USER")
        assert p.moss_roles == ["TEAMLEAD", "USER"]

        # empty / falsy values clear the field (mirror the CSV)
        p.moss_phone = ""
        p.moss_team = None
        p.moss_roles = []
        assert p.moss_phone is None
        assert p.moss_team is None
        assert p.moss_roles == []
        assert p.additional_info == {"moss_status": "ACTIVE"}


def _make_pns() -> list[DirectDebitPreNotification]:
    return [
        DirectDebitPreNotification(
            subject_id=1,
            payment_initiation_id=10,
            collection_date=_datetime.date(2026, 1, 5),
            payment_status="pre_notified",
        ),
        DirectDebitPreNotification(
            subject_id=1,
            payment_initiation_id=11,
            collection_date=_datetime.date(2026, 2, 5),
            payment_status="pre_notified",
        ),
    ]


class Test_Person_open_pre_notifications:
    def __raise_runtime_error(self):
        raise RuntimeError

    @pytest.fixture(autouse=True)
    def monkeypatch_load_open_pre_notifications(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(
            Person, "_load_open_pre_notifications", self.__raise_runtime_error
        )

    def test_set_and_get_all(self):
        p = Person(id=1)
        pns = _make_pns()
        p.set_open_pre_notifications(pns)
        assert p.get_open_pre_notifications() == pns

    def test_get_returns_a_copy(self):
        p = Person(id=1)
        p.set_open_pre_notifications(_make_pns())
        result = p.get_open_pre_notifications()
        result.clear()
        assert len(p.get_open_pre_notifications()) == 2

    def test_filter_by_payment_initiation_id(self):
        p = Person(id=1)
        p.set_open_pre_notifications(_make_pns())
        result = p.get_open_pre_notifications(payment_initiation_id=11)
        assert [pn.payment_initiation_id for pn in result] == [11]

    def test_filter_by_collection_date(self):
        p = Person(id=1)
        p.set_open_pre_notifications(_make_pns())
        result = p.get_open_pre_notifications(
            collection_date=_datetime.date(2026, 1, 5)
        )
        assert [pn.payment_initiation_id for pn in result] == [10]

    def test_filter_by_collection_date_str(self):
        p = Person(id=1)
        p.set_open_pre_notifications(_make_pns())
        result = p.get_open_pre_notifications(collection_date="2026-02-05")
        assert [pn.payment_initiation_id for pn in result] == [11]

    def test_filter_combined(self):
        p = Person(id=1)
        p.set_open_pre_notifications(_make_pns())
        result = p.get_open_pre_notifications(
            collection_date=_datetime.date(2026, 1, 5), payment_initiation_id=11
        )
        assert result == []

    def test_empty_set_does_not_fetch(self):
        p = Person(id=1)
        p.set_open_pre_notifications([])
        assert p.get_open_pre_notifications() == []


class Test_load_pre_notifications_for_people:
    def test_groups_and_attaches(self, monkeypatch):
        pns = [
            DirectDebitPreNotification(
                subject_id=1, payment_initiation_id=10, payment_status="pre_notified"
            ),
            DirectDebitPreNotification(
                subject_id=2, payment_initiation_id=10, payment_status="pre_notified"
            ),
            DirectDebitPreNotification(
                subject_id=1, payment_initiation_id=11, payment_status="pre_notified"
            ),
        ]
        monkeypatch.setattr(
            DirectDebitPreNotification,
            "load_for_subject_ids",
            classmethod(lambda cls, conn, ids, *, status=("pre_notified",): pns),
        )
        p1, p2, p3 = Person(id=1), Person(id=2), Person(id=3)
        load_pre_notifications_for_people(None, people=[p1, p2, p3])
        assert [pn.payment_initiation_id for pn in p1.get_open_pre_notifications()] == [
            10,
            11,
        ]
        assert [pn.payment_initiation_id for pn in p2.get_open_pre_notifications()] == [
            10
        ]
        assert p3.get_open_pre_notifications() == []
