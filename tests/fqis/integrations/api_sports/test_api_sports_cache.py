from app.fqis.integrations.api_sports.cache import ApiSportsJsonCache


def test_cache_roundtrip(tmp_path):
    cache = ApiSportsJsonCache(tmp_path)
    payload = {
        "get": "countries",
        "errors": [],
        "results": 1,
        "paging": {"current": 1, "total": 1},
        "response": [],
    }

    cache.set("countries", {}, payload)
    loaded = cache.get("countries", {}, ttl_seconds=60)

    assert loaded == payload


def test_cache_miss(tmp_path):
    cache = ApiSportsJsonCache(tmp_path)

    assert cache.get("fixtures", {"date": "2026-04-27"}, ttl_seconds=60) is None


def test_cache_expired(tmp_path):
    cache = ApiSportsJsonCache(tmp_path)
    payload = {
        "get": "fixtures",
        "errors": [],
        "results": 0,
        "paging": {"current": 1, "total": 1},
        "response": [],
    }

    cache.set("fixtures", {"date": "2026-04-27"}, payload)

    assert cache.get("fixtures", {"date": "2026-04-27"}, ttl_seconds=-1) is None
