from __future__ import annotations

import pytest
import requests
from wsjrdp2027._context import WsjRdpContext, WsjRdpContextConfig
from wsjrdp2027._internal.hitobito_client import HitobitoClient, HitobitoConfig


class _FakeResponse:
    def __init__(self, payload=None, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}", response=self)

    def json(self):
        return self._payload


class _FakeSession:
    """Stands in for requests.Session: records the requests, sends nothing.
    The responses come from ``replies`` in order, an empty 200 after them."""

    instances: list[_FakeSession] = []
    replies: list[_FakeResponse] = []

    def __init__(self):
        self.headers = {}
        self.calls = []
        self.closed = False
        _FakeSession.instances.append(self)

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        if _FakeSession.replies:
            return _FakeSession.replies.pop(0)
        return _FakeResponse()

    def close(self):
        self.closed = True


@pytest.fixture(autouse=True)
def fake_session(monkeypatch):
    _FakeSession.instances = []
    _FakeSession.replies = []
    monkeypatch.setattr(requests, "Session", _FakeSession)


def _client(*, dry_run=False, audithook=None):
    config = HitobitoConfig(
        hitobito_url="https://hitobito.example/",
        purpose="finance_import",
        token="test-token",
    )
    return HitobitoClient(config, dry_run=dry_run, logger=False, audithook=audithook)


class Test_HitobitoClient:
    def test_sends_the_token_in_the_header_to_the_configured_host(self):
        client = _client()
        client.post("/fin/acc/internal_transfers/link_unique.json")

        (session,) = _FakeSession.instances
        assert session.headers["X-Token"] == "test-token"
        assert session.headers["Accept"] == "application/json"
        ((method, url, kwargs),) = session.calls
        assert method == "POST"
        assert (
            url
            == "https://hitobito.example/fin/acc/internal_transfers/link_unique.json"
        )
        assert kwargs["timeout"] == 60.0

    @pytest.mark.parametrize("name", ["get", "post", "put", "patch"])
    def test_shims(self, name):
        client = _client()
        getattr(client, name)("people.json", params={"q": "x"})

        ((method, _url, kwargs),) = _FakeSession.instances[0].calls
        assert method == name.upper()
        assert kwargs["params"] == {"q": "x"}

    def test_request_takes_the_method(self):
        client = _client()
        client.request("delete", "groups/1.json")

        assert _FakeSession.instances[0].calls[0][0] == "DELETE"

    def test_dry_run_sends_only_get(self):
        client = _client(dry_run=True)

        assert client.get("people.json") is not None
        for name in ("post", "put", "patch"):
            assert getattr(client, name)("people.json") is None
        assert client.request("DELETE", "people/1.json") is None
        assert [call[0] for call in _FakeSession.instances[0].calls] == ["GET"]

    def test_audithook_sees_every_sent_request(self):
        seen = []
        client = _client(audithook=lambda action, **kw: seen.append((action, kw)))
        client.get("people.json")
        client.post("people.json")

        assert [(action, kw["method"], kw["read_only"]) for action, kw in seen] == [
            ("people.json", "GET", True),
            ("people.json", "POST", False),
        ]

    def test_rejects_an_absolute_url(self):
        with pytest.raises(ValueError, match="relative to the Hitobito URL"):
            _client().get("https://elsewhere.example/people.json")
        assert _FakeSession.instances == []

    def test_close(self):
        client = _client()
        client.get("people.json")
        client.close()

        assert client.closed
        assert _FakeSession.instances[0].closed
        with pytest.raises(RuntimeError, match="already closed"):
            client.get("people.json")

    def test_properties_and_repr_without_the_token(self):
        client = _client(dry_run=True)

        assert client.dry_run is True
        assert client.purpose == "finance_import"
        assert client.audithook is None
        assert "test-token" not in repr(client)
        assert "test-token" not in repr(client._config)


class Test_HitobitoClient_link_internal_transfers:
    def test_posts_and_returns_the_counts(self):
        counts = {"linked": 2, "ambiguous": 1, "waiting": 0, "without_counterpart": 0}
        _FakeSession.replies = [_FakeResponse(counts)]

        assert _client().link_internal_transfers() == counts
        ((method, url, _kwargs),) = _FakeSession.instances[0].calls
        assert method == "POST"
        assert (
            url
            == "https://hitobito.example/fin/acc/internal_transfers/link_unique.json"
        )

    def test_dry_run_sends_nothing(self):
        assert _client(dry_run=True).link_internal_transfers() is None
        assert _FakeSession.instances == []

    def test_raises_on_an_http_error(self):
        _FakeSession.replies = [_FakeResponse(status_code=403)]

        with pytest.raises(requests.HTTPError, match="403"):
            _client().link_internal_transfers()


def _people(*ids):
    return {"data": [{"id": str(i), "type": "people", "attributes": {}} for i in ids]}


class Test_HitobitoClient_people:
    def test_get_person_resource_by_id(self):
        _FakeSession.replies = [_FakeResponse({"data": {"id": "7", "type": "people"}})]
        person = _client().get_person_resource_by_id(
            7, fields=["first_name"], include=["roles"]
        )

        assert person == {"id": "7", "type": "people"}
        ((method, url, kwargs),) = _FakeSession.instances[0].calls
        assert (method, url) == ("GET", "https://hitobito.example/api/people/7")
        assert kwargs["params"] == {"fields[people]": "first_name", "include": "roles"}
        assert kwargs["headers"] == {"Accept": "application/vnd.api+json"}

    def test_get_person_resource_by_id_raises_on_an_error(self):
        _FakeSession.replies = [_FakeResponse({"errors": []}, status_code=403)]
        with pytest.raises(requests.HTTPError, match="403"):
            _client().get_person_resource_by_id(1)

    def test_get_person_resource_list_reads_page_by_page(self):
        _FakeSession.replies = [
            _FakeResponse(_people(1, 2)),
            _FakeResponse(_people(3, 4)),
            _FakeResponse(_people(5)),
        ]
        people = _client().get_person_resource_list(
            filter={"updated_at": {"gte": "2026-01-01"}}, page_size=2
        )

        assert [p["id"] for p in people] == ["1", "2", "3", "4", "5"]
        calls = _FakeSession.instances[0].calls
        assert [call[2]["params"]["page[number]"] for call in calls] == [1, 2, 3]
        assert calls[0][1] == "https://hitobito.example/api/people"
        assert calls[0][2]["params"]["page[size]"] == 2
        assert calls[0][2]["params"]["filter[updated_at][gte]"] == "2026-01-01"

    def test_get_person_resource_list_stops_after_an_empty_page(self):
        _FakeSession.replies = [_FakeResponse(_people(1, 2)), _FakeResponse(_people())]

        assert len(_client().get_person_resource_list(page_size=2)) == 2
        assert len(_FakeSession.instances[0].calls) == 2

    def test_or_none_on_404_only(self):
        _FakeSession.replies = [_FakeResponse({"errors": []}, status_code=404)]
        assert _client().get_person_resource_or_none_by_id(99) is None

        _FakeSession.replies = [_FakeResponse({"errors": []}, status_code=404)]
        assert _client().get_person_or_none_by_id(99) is None

        _FakeSession.replies = [_FakeResponse({"errors": []}, status_code=403)]
        with pytest.raises(requests.HTTPError, match="403"):
            _client().get_person_or_none_by_id(1)

    def test_person_objects(self):
        resource = {
            "id": "7",
            "type": "people",
            "attributes": {"first_name": "Test", "primary_group_id": 3},
        }
        _FakeSession.replies = [
            _FakeResponse({"data": resource}),
            _FakeResponse({"data": [resource]}),
        ]
        client = _client()

        person = client.get_person_by_id(7, fields=["first_name"])
        assert (person.id, person.first_name, person.primary_group_id) == (7, "Test", 3)
        (listed,) = client.get_person_list(filter={"id": "7"})
        assert listed.id == 7
        calls = _FakeSession.instances[0].calls
        assert calls[0][2]["params"] == {"fields[people]": "first_name"}
        assert calls[1][2]["params"]["filter[id]"] == "7"

    def test_reading_works_in_a_dry_run(self):
        _FakeSession.replies = [_FakeResponse(_people(1))]

        assert len(_client(dry_run=True).get_person_resource_list()) == 1


class Test_Context_get_hitobito_client:
    @staticmethod
    def _ctx(**config_kwargs) -> WsjRdpContext:
        config = WsjRdpContextConfig(
            is_production=False,
            keycloak_url="",
            helpdesk_url="",
            hitobito_url="https://hitobito.example",
            **config_kwargs,
        )
        return WsjRdpContext(config, parse_arguments=False, setup_logging=False)

    def test_one_client_per_purpose_closed_with_the_context(self):
        ctx = self._ctx(hitobito_api_tokens={"finance_import": "test-token"})
        with ctx:
            client = ctx.get_hitobito_client("finance_import")
            assert ctx.get_hitobito_client("finance_import") is client
            assert client.purpose == "finance_import"
            assert client.audithook is not None
            client.get("people.json")
            assert _FakeSession.instances[0].headers["X-Token"] == "test-token"
        assert client.closed

    def test_falls_back_to_the_general_token(self):
        ctx = self._ctx(hitobito_api_token="test-fallback")
        with ctx:
            ctx.get_hitobito_client("mailing").get("people.json")
        assert _FakeSession.instances[0].headers["X-Token"] == "test-fallback"

    def test_without_a_token(self):
        ctx = self._ctx()
        with ctx, pytest.raises(ValueError, match="hitobito_api_token_finance_import"):
            ctx.get_hitobito_client("finance_import")

    def test_dry_run(self):
        ctx = self._ctx(hitobito_api_tokens={"finance_import": "test-token"})
        with ctx:
            assert ctx.get_hitobito_client("finance_import", dry_run=True).dry_run
