"""
Area Snapshot Shared SQL Helpers
================================
Shared query builders for the Area Snapshot feature. Used by both the FastAPI
backend (``api.py``) and the Streamlit dashboard (``app.py``) so the REST
endpoint and the dashboard tab compute identical distributions:
p25/p50/p75/p90 for total price and unit price per municipality, broken down
by property segment and condominium size band, plus top districts ranked by
median unit price.

All builders return ``(query, params)`` tuples suitable for psycopg2. The
``params`` list is ordered exactly like the ``%s`` placeholders appear in the
query text (municipality codes first, then feature-specific values, then any
``extra_params`` from the caller's shared filter conditions).
"""

import re

# =============================================================================
# SNAPSHOT CONSTANTS
# =============================================================================

# Validation rule for ``municipality_codes``: 1-3 comma-separated codes.
MAX_SNAPSHOT_MUNICIPALITIES = 3

# Top-districts list defaults (per snapshot request, across the selected
# municipalities). Mirrors ``limit`` handling on /price-by-district.
SNAPSHOT_DISTRICT_LIMIT_DEFAULT = 10
SNAPSHOT_DISTRICT_LIMIT_MAX = 50

# Groups with fewer transactions than this are flagged as low-sample in the
# dashboard (warning captions) and exposed via ``low_sample_threshold`` in the
# API payload so clients can apply the same rule.
SNAPSHOT_LOW_SAMPLE_COUNT = 10

# Condominium segment used for the size-band breakdown. Matches the exact MLIT
# label surfaced by /property-types.
CONDO_PROPERTY_TYPE = "Pre-owned Condominiums, etc."

# Canonical key set of one ``municipalities[]`` entry, mirroring the columns of
# ``build_snapshot_overall_query`` in query order. Clients (dashboard tab, MCP
# agents) can rely on every requested municipality appearing with these keys
# even when it has no matching transactions — see ``empty_snapshot_municipality``.
SNAPSHOT_MUNICIPALITY_KEYS = (
    "municipality_code",
    "municipality_name",
    "transaction_count",
    "total_price_p25",
    "total_price_p50",
    "total_price_p75",
    "total_price_p90",
    "unit_price_p25",
    "unit_price_p50",
    "unit_price_p75",
    "unit_price_p90",
    "earliest_year",
    "latest_year",
    "avg_area_m2",
)

_MUNICIPALITY_CODE_RE = re.compile(r"^\d{1,5}$")

# Validity guards shared by every snapshot query. Same unit-price bounds the
# dashboard uses for its aggregations elsewhere in app.py.
_BASE_CONDITIONS = [
    "t.trade_price IS NOT NULL",
    "t.trade_price > 0",
    "t.unit_price IS NOT NULL",
    "t.unit_price > 0",
    "t.unit_price < 50000000",
]


# =============================================================================
# VALIDATION
# =============================================================================

def parse_municipality_codes(value):
    """Validate and normalize the ``municipality_codes`` snapshot parameter.

    Rules: required; comma-separated; each code 1-5 digits; duplicates are
    dropped; at most ``MAX_SNAPSHOT_MUNICIPALITIES`` codes (the Area Snapshot
    is a side-by-side comparison, not a national table). Raises ``ValueError``
    with a client-safe message when a rule is violated; API endpoints convert
    this into HTTP 400.
    """
    if value is None or not str(value).strip():
        raise ValueError(
            "municipality_codes is required (comma-separated codes, "
            f"1-{MAX_SNAPSHOT_MUNICIPALITIES} codes, e.g. '47201')"
        )

    codes = []
    for item in str(value).split(","):
        item = item.strip()
        if not item:
            continue
        if not _MUNICIPALITY_CODE_RE.match(item):
            raise ValueError(
                f"Invalid municipality code: '{item}' (expected 1-5 digits, e.g. '47201')"
            )
        if item not in codes:
            codes.append(item)

    if not codes:
        raise ValueError(
            "municipality_codes is required (comma-separated codes, "
            f"1-{MAX_SNAPSHOT_MUNICIPALITIES} codes, e.g. '47201')"
        )
    if len(codes) > MAX_SNAPSHOT_MUNICIPALITIES:
        raise ValueError(
            f"Too many municipality codes: got {len(codes)}, "
            f"maximum is {MAX_SNAPSHOT_MUNICIPALITIES}"
        )
    return codes


def empty_snapshot_municipality(code):
    """Build the stable empty-state entry for one requested municipality.

    Returned when a municipality has no matching transactions so the
    ``municipalities[]`` schema is identical whether or not data exists: the
    same keys as a populated row (``SNAPSHOT_MUNICIPALITY_KEYS`` +
    ``segments``/``size_bands``), with a ``0`` transaction count, ``None``
    percentiles/span facts, and empty breakdown lists.
    """
    entry = {key: None for key in SNAPSHOT_MUNICIPALITY_KEYS}
    entry["municipality_code"] = code
    entry["transaction_count"] = 0
    entry["segments"] = []
    entry["size_bands"] = []
    return entry


# =============================================================================
# SQL FRAGMENTS
# =============================================================================

def _normalize_expr(column):
    """Normalize a text column the same way ingestion does: full-width spaces
    become normal spaces, result is trimmed, empty becomes NULL. Mirrors
    ``api.py``'s ``normalized_text_expr`` so shared queries behave identically."""
    return f"NULLIF(BTRIM(REPLACE({column}, '　', ' ')), '')"


def _percentile_columns():
    """p25/median/p75/p90 for total price and unit price + group count."""
    return """
        COUNT(*) as transaction_count,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY t.trade_price)::BIGINT as total_price_p25,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.trade_price)::BIGINT as total_price_p50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY t.trade_price)::BIGINT as total_price_p75,
        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY t.trade_price)::BIGINT as total_price_p90,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY t.unit_price)::BIGINT as unit_price_p25,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.unit_price)::BIGINT as unit_price_p50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY t.unit_price)::BIGINT as unit_price_p75,
        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY t.unit_price)::BIGINT as unit_price_p90
    """


def _size_band_expr():
    """Condo size bands matching the dashboard's Property Size cohorts."""
    return """
        CASE
            WHEN t.area_m2 <= 30 THEN 'Compact (<=30m2)'
            WHEN t.area_m2 <= 50 THEN 'Small (>30-50m2)'
            WHEN t.area_m2 <= 70 THEN 'Medium (>50-70m2)'
            WHEN t.area_m2 <= 100 THEN 'Large (>70-100m2)'
            ELSE 'XL (>100m2)'
        END
    """


def _size_band_order_expr():
    """Sort key that keeps size bands in ascending size order."""
    return """
        CASE
            WHEN t.area_m2 <= 30 THEN 1
            WHEN t.area_m2 <= 50 THEN 2
            WHEN t.area_m2 <= 70 THEN 3
            WHEN t.area_m2 <= 100 THEN 4
            ELSE 5
        END
    """


def _snapshot_where(codes, extra_conditions=None, extra_params=None,
                    extra_first_conditions=None, extra_first_params=None):
    """Build the shared WHERE clause: municipality IN-list + base guards, then
    any feature-specific conditions (inserted before the caller's extra shared
    filters so placeholder order matches parameter order)."""
    placeholders = ",".join(["%s"] * len(codes))
    conditions = [f"t.municipality_code IN ({placeholders})"] + _BASE_CONDITIONS
    params = list(codes)
    if extra_first_conditions:
        conditions.extend(extra_first_conditions)
        params.extend(extra_first_params or [])
    if extra_conditions:
        conditions.extend(extra_conditions)
        params.extend(extra_params or [])
    return " WHERE " + " AND ".join(conditions), params


# =============================================================================
# QUERY BUILDERS
# =============================================================================

def build_snapshot_overall_query(codes, extra_conditions=None, extra_params=None):
    """One row per municipality: overall distribution + span facts for the
    summary panel."""
    where, params = _snapshot_where(codes, extra_conditions, extra_params)
    query = f"""
    SELECT
        t.municipality_code,
        COALESCE(m.name_en, m.name_ja) as municipality_name,
        {_percentile_columns()},
        MIN(t.transaction_year) as earliest_year,
        MAX(t.transaction_year) as latest_year,
        ROUND(AVG(t.area_m2), 1) as avg_area_m2
    FROM transactions t
    JOIN municipalities m ON m.code = t.municipality_code
    {where}
    GROUP BY t.municipality_code, COALESCE(m.name_en, m.name_ja)
    ORDER BY t.municipality_code
    """
    return query, params


def build_snapshot_segment_query(codes, extra_conditions=None, extra_params=None):
    """One row per municipality x property segment (property type label)."""
    where, params = _snapshot_where(codes, extra_conditions, extra_params)
    segment_expr = f"COALESCE({_normalize_expr('t.property_type_raw')}, 'Unspecified')"
    query = f"""
    SELECT
        t.municipality_code,
        COALESCE(m.name_en, m.name_ja) as municipality_name,
        {segment_expr} as segment,
        {_percentile_columns()}
    FROM transactions t
    JOIN municipalities m ON m.code = t.municipality_code
    {where}
    GROUP BY t.municipality_code, COALESCE(m.name_en, m.name_ja), {segment_expr}
    ORDER BY t.municipality_code, transaction_count DESC
    """
    return query, params


def build_snapshot_size_band_query(codes, extra_conditions=None, extra_params=None):
    """One row per municipality x condo size band. Condominium transactions
    only; area must be valid (excludes the sentinel 9999/8888 values)."""
    where, params = _snapshot_where(
        codes,
        extra_conditions,
        extra_params,
        extra_first_conditions=[
            "t.area_m2 IS NOT NULL",
            "t.area_m2 > 0",
            "t.area_m2 NOT IN (9999, 8888)",
            f"{_normalize_expr('t.property_type_raw')} = %s",
        ],
        extra_first_params=[CONDO_PROPERTY_TYPE],
    )
    band_expr = _size_band_expr()
    order_expr = _size_band_order_expr()
    query = f"""
    SELECT
        t.municipality_code,
        COALESCE(m.name_en, m.name_ja) as municipality_name,
        {band_expr} as size_band,
        {_percentile_columns()}
    FROM transactions t
    JOIN municipalities m ON m.code = t.municipality_code
    {where}
    GROUP BY t.municipality_code, COALESCE(m.name_en, m.name_ja), {band_expr}, {order_expr}
    ORDER BY t.municipality_code, {order_expr}
    """
    return query, params


def build_snapshot_district_query(codes, district_limit=SNAPSHOT_DISTRICT_LIMIT_DEFAULT,
                                  extra_conditions=None, extra_params=None):
    """Top districts across the selected municipalities, ranked by median
    unit price (same ranking convention as /price-by-district)."""
    where, params = _snapshot_where(
        codes,
        extra_conditions,
        extra_params,
        extra_first_conditions=[f"{_normalize_expr('t.district_name')} IS NOT NULL"],
    )
    district_expr = _normalize_expr("t.district_name")
    query = f"""
    SELECT
        t.municipality_code,
        COALESCE(m.name_en, m.name_ja) as municipality_name,
        {district_expr} as district_name,
        {_percentile_columns()}
    FROM transactions t
    JOIN municipalities m ON m.code = t.municipality_code
    {where}
    GROUP BY t.municipality_code, COALESCE(m.name_en, m.name_ja), {district_expr}
    ORDER BY unit_price_p50 DESC NULLS LAST
    LIMIT %s
    """
    params.append(district_limit)
    return query, params
