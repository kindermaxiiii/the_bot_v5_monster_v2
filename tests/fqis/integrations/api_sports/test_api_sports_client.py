import pytest
import requests

from app.fqis.integrations.api_sports.client import ApiSportsClient, ApiSportsHttpError
from app.fqis.integrations.api_sports.config import ApiSportsConfig


class FakeResponse:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self.headers = {
            "x-ratelimit-requests-limit": "75000",
            "x-ratelimit-requests-remaining": "74999",
            "x-ratelimit-limit": "300",
            "x-ratelimit-remaining": "299",
        }
        self._payload = payload or {
            "get": "countries",
            "parameters": [],
            "errors": [],
            "results": 1,
            "paging": {"current": 1, "total": 1},
            "response": [{"name": "France"}],
        }
        self.text = text

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, response=None, exc=None):
        self.calls = []
        self.response = response or FakeResponse()
        self.exc = exc

    def get(self, url, params, headers, timeout):
        self.calls.append(
            {
                "url": url,
                "params": params,
                "headers": headers,
                "timeout": timeout,
            }
        )
        if self.exc:
            raise self.exc
        return self.response


def test_client_get_uses_header_and_parses_response(tmp_path):
    session = FakeSession()
    config = ApiSportsConfig(api_key="SECRET", cache_dir=tmp_path)

    client = ApiSportsClient(config=config, session=session)
    response = client.countries()

    assert response.endpoint == "countries"
    assert response.results == 1
    assert session.calls[0]["headers"]["x-apisports-key"] == "SECRET"
    assert "SECRET" not in str(config.redacted())


def test_client_cleans_empty_params(tmp_path):
    session = FakeSession()
    config = ApiSportsConfig(api_key="SECRET", cache_dir=tmp_path)

    client = ApiSportsClient(config=config, session=session)
    client.get("fixtures", {"date": "2026-04-27", "league": None, "team": ""})

    assert session.calls[0]["params"] == {"date": "2026-04-27"}


def test_client_raises_http_error(tmp_path):
    session = FakeSession(response=FakeResponse(status_code=500, text="server down"))
    config = ApiSportsConfig(api_key="SECRET", cache_dir=tmp_path)

    client = ApiSportsClient(config=config, session=session)

    with pytest.raises(ApiSportsHttpError):
        client.get("status")


def test_client_raises_network_error(tmp_path):
    session = FakeSession(exc=requests.Timeout("boom"))
    config = ApiSportsConfig(api_key="SECRET", cache_dir=tmp_path)

    client = ApiSportsClient(config=config, session=session)

    with pytest.raises(ApiSportsHttpError):
        client.get("status")


def test_client_uses_cache_hit_without_network_call(tmp_path):
    session = FakeSession()
    config = ApiSportsConfig(api_key="SECRET", cache_dir=tmp_path)
    client = ApiSportsClient(config=config, session=session)

    client.cache.set(
        "countries",
        {},
        {
            "get": "countries",
            "parameters": {},
            "errors": [],
            "results": 1,
            "paging": {"current": 1, "total": 1},
            "response": [{"name": "France"}],
        },
    )

    response = client.countries()

    assert response.endpoint == "countries"
    assert session.calls == []
