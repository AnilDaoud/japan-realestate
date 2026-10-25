"""
Tests for the Area Snapshot feature (GET /area-snapshot).

Covers the shared helpers in ``snapshot_utils`` and the FastAPI endpoint in
``api.py``. The endpoint tests monkeypatch ``api.run_query`` so no database is
required, and ``app.py`` (Streamlit) is deliberately not imported because
streamlit is not installed in this environment.

Run: python -m pytest test_area_snapshot.py -v
"""

import pytest
from fastapi.testclient import TestClient

import api
import snapshot_utils
from snapshot_utils import (
    MAX_SNAPSHOT_MUNICIPALITIES,
    SNAPSHOT_MUNICIPALITY_KEYS,
    build_snapshot_district_query,
    build_snapshot_overall_query,
    build_snapshot_segment_query,
    build_snapshot_size_band_query,
    empty_snapshot_municipality,
    parse_municipality_codes,
)


# =============================================================================
# parse_municipality_codes
# =============================================================================

class TestParseMunicipalityCodes:
    def test_accepts_single_code(self):
        assert parse_municipality_codes("47201") == ["47201"]

    def test_accepts_and_dedupes_valid_codes(self):
        assert parse_municipality_codes("47201, 47202,47201") == ["47201", "47202"]

    def test_strips_whitespace_and_blank_items(self):
        assert parse_municipality_codes(" 13101 ,, 13102 ") == ["13101", "13102"]

    @pytest.mark.parametrize("value", [None, "", "   ", ",", " , "])
    def test_rejects_empty(self, value):
        with pytest.raises(ValueError, match="required"):
            parse_municipality_codes(value)

    @pytest.mark.parametrize("value", ["abc", "13a01", "123456", "4720!", "1.5"])
    def test_rejects_invalid_codes(self, value):
        with pytest.raises(ValueError, match="Invalid municipality code"):
            parse_municipality_codes(value)

    def test_rejects_too_many_codes(self):
        too_many = ",".join(str(1000 + i) for i in range(MAX_SNAPSHOT_MUNICIPALITIES + 1))
        with pytest.raises(ValueError, match="Too many municipality codes"):
            parse_municipality_codes(too_many)

    def test_accepts_max_number_of_codes(self):
        codes = ",".join(str(1000 + i) for i in range(MAX_SNAPSHOT_MUNICIPALITIES))
        assert parse_municipality_codes(codes) == ["1000", "1001", "1002"]


# =============================================================================
# empty_snapshot_municipality
# =============================================================================

class TestEmptySnapshotMunicipality:
    def test_returns_stable_schema_keys(self):
        entry = empty_snapshot_municipality("47202")
        expected = set(SNAPSHOT_MUNICIPALITY_KEYS) | {"segments", "size_bands"}
        assert set(entry.keys()) == expected

    def test_empty_state_values(self):
        entry = empty_snapshot_municipality("47202")
        assert entry["municipality_code"] == "47202"
        assert entry["transaction_count"] == 0
        assert entry["segments"] == []
        assert entry["size_bands"] == []
        for key in SNAPSHOT_MUNICIPALITY_KEYS:
            if key not in ("municipality_code", "transaction_count"):
                assert entry[key] is None


# =============================================================================
# GET /area-snapshot (run_query monkeypatched, no DB needed)
# =============================================================================

POPULATED_CODE = "47201"
EMPTY_CODE = "47202"


def _overall_row(code):
    return {
        "municipality_code": code,
        "municipality_name": "Naha",
        "transaction_count": 120,
        "total_price_p25": 20_000_000,
        "total_price_p50": 25_000_000,
        "total_price_p75": 30_000_000,
        "total_price_p90": 34_000_000,
        "unit_price_p25": 400_000,
        "unit_price_p50": 450_000,
        "unit_price_p75": 520_000,
        "unit_price_p90": 610_000,
        "earliest_year": 2003,
        "latest_year": 2025,
        "avg_area_m2": 60.5,
    }


def _segment_row():
    row = _overall_row(POPULATED_CODE)
    row["segment"] = "Pre-owned Condominiums, etc."
    return row


def _band_row():
    row = _overall_row(POPULATED_CODE)
    row["size_band"] = "Medium (>50-70m2)"
    return row


def _district_row():
    row = _overall_row(POPULATED_CODE)
    row["district_name"] = "Naha-city Asahibashi"
    return row


def _fake_run_query(query, params=()):
    """Dispatch canned results based on distinguishing query fragments."""
    if "MIN(t.transaction_year)" in query:  # overall
        return [_overall_row(POPULATED_CODE)]  # EMPTY_CODE has no transactions
    if "as segment" in query:
        return [_segment_row()]
    if "as size_band" in query:
        return [_band_row()]
    if "as district_name" in query:
        return [_district_row()]
    raise AssertionError(f"Unexpected query passed to run_query: {query[:80]}")


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setattr(api, "run_query", _fake_run_query)
    return TestClient(api.app)


class TestAreaSnapshotEndpoint:
    def test_returns_200_for_valid_codes(self, client):
        resp = client.get("/area-snapshot", params={"municipality_codes": POPULATED_CODE})
        assert resp.status_code == 200
        body = resp.json()
        assert body["parameters"]["municipality_codes"] == [POPULATED_CODE]
        assert body["parameters"]["low_sample_threshold"] == snapshot_utils.SNAPSHOT_LOW_SAMPLE_COUNT
        assert body["condo_property_type"] == snapshot_utils.CONDO_PROPERTY_TYPE
        assert [m["municipality_code"] for m in body["municipalities"]] == [POPULATED_CODE]
        m = body["municipalities"][0]
        assert m["unit_price_p50"] == 450_000
        assert m["segments"][0]["segment"] == "Pre-owned Condominiums, etc."
        assert m["size_bands"][0]["size_band"] == "Medium (>50-70m2)"
        assert body["top_districts"][0]["district_name"] == "Naha-city Asahibashi"

    def test_preserves_schema_for_empty_municipality(self, client):
        resp = client.get(
            "/area-snapshot", params={"municipality_codes": f"{POPULATED_CODE},{EMPTY_CODE}"}
        )
        assert resp.status_code == 200
        municipalities = resp.json()["municipalities"]
        assert [m["municipality_code"] for m in municipalities] == [POPULATED_CODE, EMPTY_CODE]
        populated, empty = municipalities
        # Same key set whether or not the municipality has transactions.
        assert set(empty.keys()) == set(populated.keys())
        assert empty["transaction_count"] == 0
        assert empty["segments"] == []
        assert empty["size_bands"] == []
        assert empty["unit_price_p50"] is None

    def test_rejects_missing_codes_with_400(self, client):
        assert client.get("/area-snapshot").status_code == 400

    def test_rejects_invalid_codes_with_400(self, client):
        resp = client.get("/area-snapshot", params={"municipality_codes": "abc"})
        assert resp.status_code == 400
        assert "Invalid municipality code" in resp.json()["detail"]

    def test_rejects_too_many_codes_with_400(self, client):
        resp = client.get(
            "/area-snapshot", params={"municipality_codes": "1000,1001,1002,1003"}
        )
        assert resp.status_code == 400
        assert "Too many" in resp.json()["detail"]


# =============================================================================
# API / dashboard parity: no station filtering on the snapshot path
# =============================================================================

class TestSnapshotNoStationParity:
    """The Area Snapshot must not reference station data anywhere in the
    shared API path. ``stations`` is empty and ``transactions.nearest_station_code``
    is never populated by ingestion, and the /area-snapshot filter contract
    has no station parameter — so the Streamlit tab may not apply one either
    (reviewer parity bug). These tests pin that contract."""

    def test_snapshot_builders_never_reference_station_column(self):
        queries = []
        queries.append(build_snapshot_overall_query(["13101"])[0])
        queries.append(build_snapshot_segment_query(["13101"])[0])
        queries.append(build_snapshot_size_band_query(["13101"])[0])
        queries.append(build_snapshot_district_query(["13101"])[0])
        for query in queries:
            assert "station" not in query.lower()

    def test_endpoint_signature_has_no_station_param(self):
        params = api.get_area_snapshot.__annotations__.keys()
        assert not any("station" in name.lower() for name in params)


# =============================================================================
# Endpoint discovery (/ and /mcp/tools)
# =============================================================================

class TestEndpointListing:
    def test_mcp_tools_include_area_snapshot(self, client):
        resp = client.get("/mcp/tools")
        assert resp.status_code == 200
        names = [t["name"] for t in resp.json()["tools"]]
        assert "get_area_snapshot" in names

    def test_root_lists_area_snapshot(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        endpoints = resp.json()["endpoints"]
        assert "/area-snapshot" in endpoints["transactions"]
