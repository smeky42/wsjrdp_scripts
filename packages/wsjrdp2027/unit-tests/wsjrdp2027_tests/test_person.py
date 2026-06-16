from wsjrdp2027._person import Person


class Test_Person:
    def test_additional_info(self):
        p = Person()

        p.moss_email = "foo@foo"

        assert p.additional_info_changed
        assert p.additional_info_was == {}
        assert p.additional_info == {"moss_email": "foo@foo"}
        assert p.additional_info_updates_dict == {"moss_email": [None, "foo@foo"]}

        p.moss_email = None
        assert not p.additional_info_changed
        assert p.additional_info == {}
        assert p.additional_info_updates_dict == {}

        p.moss_email = "foo@bar"
        assert p.additional_info_changed
        assert p.additional_info == {"moss_email": "foo@bar"}
        assert p.additional_info_updates_dict == {"moss_email": [None, "foo@bar"]}

        p.moss_email = "foo@baz"
        assert p.additional_info_changed
        assert p.additional_info == {"moss_email": "foo@baz"}
        assert p.additional_info_updates_dict == {"moss_email": [None, "foo@baz"]}
