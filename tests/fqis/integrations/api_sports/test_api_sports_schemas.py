import pytest

from app.fqis.integrations.api_sports.schemas import ApiSportsResponse, ApiSportsResponseError


def test_response_parses_standard_payload():
    parsed = ApiSportsResponse.from_payload(
        {
            "get": "countries",
            "parameters": [],
            "errors": [],
            "results": 1,
            "paging": {"current": 1, "total": 1},
            "response": [{"name": "France"}],
        }
    )

    assert parsed.endpoint == "countries"
    assert parsed.results == 1
    assert parsed.paging.current == 1
    assert parsed.response[0]["name"] == "France"


def test_response_raises_on_errors():
    with pytest.raises(ApiSportsResponseError):
        ApiSportsResponse.from_payload(
            {
                "get": "fixtures",
                "parameters": {},
                "errors": {"token": "invalid"},
                "results": 0,
                "paging": {"current": 1, "total": 1},
                "response": [],
            }
        )


def test_response_handles_missing_paging():
    parsed = ApiSportsResponse.from_payload(
        {
            "get": "status",
            "parameters": {},
            "errors": [],
            "results": "1",
            "response": {},
        }
    )

    assert parsed.results == 1
    assert parsed.paging.current is None
