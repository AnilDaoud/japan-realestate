"""
Japan Real Estate Analytics - FastAPI Backend
==============================================
REST API for Japanese real estate transaction data.

Run: uvicorn api:app --reload --port 8000
"""

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from typing import List, Optional, Dict, Any, Annotated
from datetime import datetime
import time

# Import rate limiting middleware
from api_security import add_rate_limit_middleware, logger

# =============================================================================
# CONFIG
# =============================================================================

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://localhost/mlit_realestate"
)

app = FastAPI(
    title="Japan Real Estate Analytics API",
    description="REST API for exploring Japanese real estate transaction data (public, no authentication required)",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# Enable CORS
allowed_origins = os.getenv("CORS_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add rate limiting middleware
add_rate_limit_middleware(app)
logger.info("Rate limiting middleware enabled")

# =============================================================================
# CONSTANTS
# =============================================================================

TSUBO_TO_M2 = 3.30579
M2_TO_TSUBO = 1 / TSUBO_TO_M2

# =============================================================================
# FILTER HELPERS
# =============================================================================

def parse_csv_values(
    value: Optional[str], valid_values: Optional[List[str]] = None
) -> Optional[List[str]]:
    """Parse a comma-separated parameter into a list of trimmed, non-empty values.

    Returns None when the input is missing or contains no usable entries.

    When ``valid_values`` is provided, the input is parsed by greedily matching
    the longest valid label span at each position. This lets categorical labels
    that themselves contain commas (e.g. ``"Pre-owned Condominiums, etc."``)
    pass through intact while ordinary comma-separated multi-value filters keep
    splitting as before. For example, ``"Pre-owned Condominiums, etc.,Pre-owned
    House"`` correctly parses to ``["Pre-owned Condominiums, etc.",
    "Pre-owned House"]``.
    """
    if value is None:
        return None
    stripped = value.strip()
    if not valid_values:
        values = [item.strip() for item in value.split(",")]
        values = [item for item in values if item]
        return values or None
    if stripped in valid_values:
        return [stripped]
    # Greedy longest-match over the comma-separated tokens, but a valid label
    # may itself contain commas. Sort candidate labels by length (descending)
    # so the longest valid span wins at each position.
    valid_set = set(valid_values)
    valid_sorted = sorted(valid_values, key=len, reverse=True)
    values = []
    remaining = stripped
    while remaining:
        remaining = remaining.strip()
        if not remaining:
            break
        matched = False
        for label in valid_sorted:
            if remaining.startswith(label):
                # Only accept a match when the remaining string is exactly the
                # label, or the next character after the label is a comma. This
                # avoids accepting arbitrary prefix matches like "<label>XYZ".
                after = remaining[len(label):]
                if after and not after.startswith(","):
                    continue
                values.append(label)
                remaining = after
                # Consume a following comma separator if present.
                if remaining.startswith(","):
                    remaining = remaining[1:]
                matched = True
                break
        if not matched:
            # No valid label matched; fall back to a single plain token.
            token, _, rest = remaining.partition(",")
            token = token.strip()
            if token:
                values.append(token)
            remaining = rest
    return values or None


def normalized_text_expr(column: str) -> str:
    """Return a SQL expression that normalizes a text column the same way the
    ingestion path does: replace full-width spaces with normal spaces, trim,
    and treat empty/whitespace-only results as NULL."""
    return f"NULLIF(BTRIM(REPLACE({column}, '　', ' ')), '')"


def build_filter_conditions(
    prefecture_code: Optional[str] = None,
    municipality_codes: Optional[str] = None,
    districts: Optional[str] = None,
    property_types: Optional[str] = None,
    structures: Optional[str] = None,
    floor_plans: Optional[str] = None,
    year_min: Optional[int] = None,
    year_max: Optional[int] = None,
    price_min: Optional[float] = None,
    price_max: Optional[float] = None,
    area_min: Optional[float] = None,
    area_max: Optional[float] = None,
    building_year_min: Optional[int] = None,
    building_year_max: Optional[int] = None,
    unit_price_min: Optional[float] = None,
    unit_price_max: Optional[float] = None,
    table_alias: str = "",
) -> tuple:
    """Build shared WHERE conditions and params for the analysis endpoints.

    Returns a (conditions, params) tuple. ``table_alias`` (e.g. ``"t."``) is
    prepended to column names when the query joins other tables.
    """
    conditions = []
    params = []

    def col(name: str) -> str:
        return f"{table_alias}{name}"

    if prefecture_code:
        conditions.append(f"{col('prefecture_code')} = %s")
        params.append(prefecture_code)

    for value, column in (
        (municipality_codes, "municipality_code"),
        (districts, "district_name"),
        (property_types, "property_type_raw"),
        (structures, "structure"),
        (floor_plans, "floor_plan"),
    ):
        valid_values = None
        if column == "property_type_raw" and value:
            valid_values = get_property_types()
        parsed = parse_csv_values(value, valid_values)
        if parsed:
            if column == "municipality_code":
                expr = col(column)
            else:
                expr = normalized_text_expr(col(column))
            placeholders = ",".join(["%s"] * len(parsed))
            conditions.append(f"{expr} IN ({placeholders})")
            params.extend(parsed)

    for value, column, operator in (
        (year_min, "transaction_year", ">="),
        (year_max, "transaction_year", "<="),
        (price_min, "trade_price", ">="),
        (price_max, "trade_price", "<="),
        (area_min, "area_m2", ">="),
        (area_max, "area_m2", "<="),
        (building_year_min, "building_year", ">="),
        (building_year_max, "building_year", "<="),
        (unit_price_min, "unit_price", ">="),
        (unit_price_max, "unit_price", "<="),
    ):
        if value is not None:
            conditions.append(f"{col(column)} {operator} %s")
            params.append(value)

    return conditions, params


def build_where_clause(conditions: List[str]) -> str:
    """Render a list of conditions into a SQL WHERE clause (or empty string)."""
    return " WHERE " + " AND ".join(conditions) if conditions else ""


# =============================================================================
# DATABASE HELPERS
# =============================================================================

def get_db_connection():
    """Create a new database connection."""
    return psycopg2.connect(DATABASE_URL)

def run_query(query: str, params: tuple = ()) -> List[Dict[str, Any]]:
    """Execute a query and return results as list of dicts."""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            results = [dict(row) for row in cur.fetchall()]
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        if conn is not None:
            conn.close()

# =============================================================================
# REFERENCE DATA ENDPOINTS
# =============================================================================

@app.get("/prefectures")
def get_prefectures():
    """Get all prefectures."""
    query = """
    SELECT code as prefecture_code, name_en as prefecture_name
    FROM prefectures
    ORDER BY name_en
    """
    results = run_query(query)
    return results

@app.get("/municipalities")
def get_municipalities(prefecture_code: str):
    """Get municipalities for a prefecture."""
    query = """
    SELECT code as municipality_code, name_en as municipality_name, prefecture_code
    FROM municipalities
    WHERE prefecture_code = %s
    ORDER BY name_en
    """
    results = run_query(query, (prefecture_code,))
    return results

@app.get("/districts")
def get_districts(municipality_codes: str):
    """Get districts for municipalities (comma-separated codes)."""
    codes = tuple(municipality_codes.split(","))
    placeholders = ",".join(["%s"] * len(codes))
    query = f"""
    SELECT DISTINCT {normalized_text_expr("t.district_name")} as district_name, t.municipality_code, m.name_en as municipality_name
    FROM transactions t
    JOIN municipalities m ON m.code = t.municipality_code
    WHERE t.municipality_code IN ({placeholders})
      AND {normalized_text_expr("t.district_name")} IS NOT NULL
    ORDER BY district_name
    """
    results = run_query(query, codes)
    return results

# TTL for the cached property-type reference values (seconds). Keeps per-request
# filtering free of a DB roundtrip while letting newly ingested labels eventually
# be picked up without a process restart.
PROPERTY_TYPES_CACHE_TTL = 3600

_property_types_cache: Dict[str, Any] = {"timestamp": 0.0, "values": None}


@app.get("/property-types")
def get_property_types():
    """Get all property types.

    Cached at module level so per-request filtering of ``property_types`` does
    not add a DB roundtrip. The cache refreshes periodically (``PROPERTY_TYPES_
    CACHE_TTL``) so newly ingested labels are eventually picked up without a
    process restart.
    """
    now = time.time()
    if (
        _property_types_cache["values"] is not None
        and now - _property_types_cache["timestamp"] < PROPERTY_TYPES_CACHE_TTL
    ):
        return _property_types_cache["values"]
    query = """
    SELECT DISTINCT NULLIF(BTRIM(REPLACE(property_type_raw, '　', ' ')), '') as property_type_raw
    FROM transactions
    WHERE property_type_raw IS NOT NULL
      AND NULLIF(BTRIM(REPLACE(property_type_raw, '　', ' ')), '') IS NOT NULL
    ORDER BY property_type_raw
    """
    results = run_query(query)
    values = [r["property_type_raw"] for r in results if r["property_type_raw"]]
    _property_types_cache["timestamp"] = now
    _property_types_cache["values"] = values
    return values

@app.get("/structures")
def get_structures():
    """Get all building structure types."""
    query = """
    SELECT DISTINCT NULLIF(BTRIM(REPLACE(structure, '　', ' ')), '') as structure
    FROM transactions
    WHERE structure IS NOT NULL
      AND NULLIF(BTRIM(REPLACE(structure, '　', ' ')), '') IS NOT NULL
    ORDER BY structure
    """
    results = run_query(query)
    return [r["structure"] for r in results if r["structure"]]

@app.get("/floor-plans")
def get_floor_plans():
    """Get all floor plan types."""
    query = """
    SELECT DISTINCT NULLIF(BTRIM(REPLACE(floor_plan, '　', ' ')), '') as floor_plan
    FROM transactions
    WHERE floor_plan IS NOT NULL
      AND NULLIF(BTRIM(REPLACE(floor_plan, '　', ' ')), '') IS NOT NULL
    ORDER BY floor_plan
    """
    results = run_query(query)
    return [r["floor_plan"] for r in results if r["floor_plan"]]

@app.get("/year-range")
def get_year_range():
    """Get min/max transaction years."""
    query = """
    SELECT MIN(transaction_year) as min_year, MAX(transaction_year) as max_year
    FROM transactions
    """
    results = run_query(query)
    if results:
        return results[0]
    return {"min_year": None, "max_year": None}

@app.get("/building-year-range")
def get_building_year_range():
    """Get min/max building years."""
    query = """
    SELECT MIN(building_year) as min_year, MAX(building_year) as max_year
    FROM transactions
    WHERE building_year IS NOT NULL
    """
    results = run_query(query)
    if results:
        return results[0]
    return {"min_year": None, "max_year": None}

# =============================================================================
# STATISTICS ENDPOINTS
# =============================================================================

@app.get("/stats")
def get_db_stats():
    """Get database statistics by year and quarter."""
    query = """
    SELECT
        transaction_year,
        transaction_quarter,
        COUNT(*) as count
    FROM transactions
    GROUP BY transaction_year, transaction_quarter
    ORDER BY transaction_year DESC, transaction_quarter DESC
    """
    results = run_query(query)
    return results

@app.get("/stats/summary")
def get_stats_summary():
    """Get overall database statistics."""
    query = """
    SELECT
        COUNT(*) as total_records,
        COUNT(DISTINCT prefecture_code) as prefectures,
        COUNT(DISTINCT municipality_code) as municipalities,
        COUNT(DISTINCT district_name) as districts,
        MIN(transaction_year) as earliest_year,
        MAX(transaction_year) as latest_year,
        MIN(transaction_quarter) as earliest_quarter,
        MAX(transaction_quarter) as latest_quarter
    FROM transactions
    """
    results = run_query(query)
    return results[0] if results else {}

# =============================================================================
# TRANSACTION QUERY ENDPOINTS
# =============================================================================

@app.get("/transactions")
def get_transactions(
    prefecture_code: Optional[str] = None,
    municipality_codes: Optional[str] = None,
    districts: Optional[str] = None,
    property_types: Optional[str] = None,
    structures: Optional[str] = None,
    floor_plans: Optional[str] = None,
    year_min: Optional[int] = None,
    year_max: Optional[int] = None,
    price_min: Optional[float] = None,
    price_max: Optional[float] = None,
    area_min: Optional[float] = None,
    area_max: Optional[float] = None,
    building_year_min: Optional[int] = None,
    building_year_max: Optional[int] = None,
    unit_price_min: Optional[float] = None,
    unit_price_max: Optional[float] = None,
    limit: Annotated[int, Query(le=10000)] = 1000,
    offset: int = 0,
):
    """Query transactions with flexible filters."""
    limit = max(1, min(limit, 10000))
    offset = max(0, offset)

    conditions, params = build_filter_conditions(
        prefecture_code=prefecture_code,
        municipality_codes=municipality_codes,
        districts=districts,
        property_types=property_types,
        structures=structures,
        floor_plans=floor_plans,
        year_min=year_min,
        year_max=year_max,
        price_min=price_min,
        price_max=price_max,
        area_min=area_min,
        area_max=area_max,
        building_year_min=building_year_min,
        building_year_max=building_year_max,
        unit_price_min=unit_price_min,
        unit_price_max=unit_price_max,
    )

    where_clause = build_where_clause(conditions)

    query = f"""
    SELECT *
    FROM transactions
    {where_clause}
    ORDER BY transaction_year DESC, transaction_quarter DESC, id DESC
    LIMIT %s OFFSET %s
    """
    params.extend([limit, offset])

    results = run_query(query, tuple(params))
    return {"count": len(results), "data": results, "limit": limit, "offset": offset}

# =============================================================================
# PRICE ANALYSIS ENDPOINTS
# =============================================================================

@app.get("/price-trends")
def get_price_trends(
    prefecture_code: Optional[str] = None,
    municipality_codes: Optional[str] = None,
    districts: Optional[str] = None,
    property_types: Optional[str] = None,
    structures: Optional[str] = None,
    floor_plans: Optional[str] = None,
    year_min: Optional[int] = None,
    year_max: Optional[int] = None,
    price_min: Optional[float] = None,
    price_max: Optional[float] = None,
    area_min: Optional[float] = None,
    area_max: Optional[float] = None,
    building_year_min: Optional[int] = None,
    building_year_max: Optional[int] = None,
    unit_price_min: Optional[float] = None,
    unit_price_max: Optional[float] = None,
    frequency: Annotated[str, Query(pattern="^(Quarterly|Yearly)$")] = "Quarterly",
):
    """Get historical price trends."""
    conditions, params = build_filter_conditions(
        prefecture_code=prefecture_code,
        municipality_codes=municipality_codes,
        districts=districts,
        property_types=property_types,
        structures=structures,
        floor_plans=floor_plans,
        year_min=year_min,
        year_max=year_max,
        price_min=price_min,
        price_max=price_max,
        area_min=area_min,
        area_max=area_max,
        building_year_min=building_year_min,
        building_year_max=building_year_max,
        unit_price_min=unit_price_min,
        unit_price_max=unit_price_max,
    )

    where_clause = build_where_clause(conditions)

    if frequency == "Quarterly":
        group_by = "transaction_year, transaction_quarter"
        order_by = "transaction_year DESC, transaction_quarter DESC"
        quarter_select = "transaction_quarter"
    else:
        group_by = "transaction_year"
        order_by = "transaction_year DESC"
        quarter_select = "NULL as transaction_quarter"

    query = f"""
    SELECT
        transaction_year,
        {quarter_select},
        COUNT(*) as volume,
        AVG(trade_price) as avg_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY trade_price) as median_price,
        AVG(CASE WHEN area_m2 > 0 THEN trade_price / area_m2 ELSE NULL END) as avg_unit_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN area_m2 > 0 THEN trade_price / area_m2 ELSE NULL END) as median_unit_price
    FROM transactions
    {where_clause}
    GROUP BY {group_by}
    ORDER BY {order_by}
    """

    results = run_query(query, tuple(params))
    return results

@app.get("/median-price")
def get_median_price(
    prefecture_code: Optional[str] = None,
    municipality_codes: Optional[str] = None,
    districts: Optional[str] = None,
    property_types: Optional[str] = None,
    structures: Optional[str] = None,
    floor_plans: Optional[str] = None,
    year_min: Optional[int] = None,
    year_max: Optional[int] = None,
    price_min: Optional[float] = None,
    price_max: Optional[float] = None,
    area_min: Optional[float] = None,
    area_max: Optional[float] = None,
    building_year_min: Optional[int] = None,
    building_year_max: Optional[int] = None,
    unit_price_min: Optional[float] = None,
    unit_price_max: Optional[float] = None,
):
    """Get latest median price for an area."""
    conditions, params = build_filter_conditions(
        prefecture_code=prefecture_code,
        municipality_codes=municipality_codes,
        districts=districts,
        property_types=property_types,
        structures=structures,
        floor_plans=floor_plans,
        year_min=year_min,
        year_max=year_max,
        price_min=price_min,
        price_max=price_max,
        area_min=area_min,
        area_max=area_max,
        building_year_min=building_year_min,
        building_year_max=building_year_max,
        unit_price_min=unit_price_min,
        unit_price_max=unit_price_max,
    )

    where_clause = build_where_clause(conditions)

    query = f"""
    SELECT
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY trade_price) as median_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN area_m2 > 0 THEN trade_price / area_m2 ELSE NULL END) as median_unit_price,
        AVG(trade_price) as avg_price,
        COUNT(*) as transaction_count
    FROM transactions
    {where_clause}
    """

    results = run_query(query, tuple(params))
    return results[0] if results else {}

@app.get("/price-by-district")
def get_price_by_district(
    prefecture_code: Optional[str] = None,
    municipality_codes: Optional[str] = None,
    districts: Optional[str] = None,
    property_types: Optional[str] = None,
    structures: Optional[str] = None,
    floor_plans: Optional[str] = None,
    year_min: Optional[int] = None,
    year_max: Optional[int] = None,
    price_min: Optional[float] = None,
    price_max: Optional[float] = None,
    area_min: Optional[float] = None,
    area_max: Optional[float] = None,
    building_year_min: Optional[int] = None,
    building_year_max: Optional[int] = None,
    unit_price_min: Optional[float] = None,
    unit_price_max: Optional[float] = None,
    limit: Annotated[int, Query(le=500)] = 50,
):
    """Get median prices grouped by district."""
    limit = max(1, min(limit, 500))

    conditions, params = build_filter_conditions(
        prefecture_code=prefecture_code,
        municipality_codes=municipality_codes,
        districts=districts,
        property_types=property_types,
        structures=structures,
        floor_plans=floor_plans,
        year_min=year_min,
        year_max=year_max,
        price_min=price_min,
        price_max=price_max,
        area_min=area_min,
        area_max=area_max,
        building_year_min=building_year_min,
        building_year_max=building_year_max,
        unit_price_min=unit_price_min,
        unit_price_max=unit_price_max,
        table_alias="t.",
    )

    where_clause = build_where_clause(conditions)

    query = f"""
    SELECT
        t.district_name,
        m.name_en as municipality_name,
        COUNT(*) as transaction_count,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.trade_price) as median_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN t.area_m2 > 0 THEN t.trade_price / t.area_m2 ELSE NULL END) as median_unit_price,
        AVG(t.trade_price) as avg_price
    FROM transactions t
    JOIN municipalities m ON m.code = t.municipality_code
    {where_clause}
    GROUP BY t.district_name, m.name_en
    ORDER BY median_price DESC NULLS LAST
    LIMIT %s
    """
    params.append(limit)

    results = run_query(query, tuple(params))
    return results

# =============================================================================
# HEALTH CHECK
# =============================================================================

@app.get("/health")
def health_check():
    """Health check endpoint."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        conn.close()
        return {"status": "healthy", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        raise HTTPException(status_code=503, detail="Database connection failed")

# =============================================================================
# MCP (MODEL CONTEXT PROTOCOL) ENDPOINTS
# =============================================================================

@app.get("/mcp/tools")
def get_mcp_tools():
    """Get list of available MCP tools for AI agents."""
    from mcp_server import TOOLS
    return {
        "tools": TOOLS,
        "description": "MCP tools for querying Japanese real estate data"
    }

@app.post("/mcp/call/{tool_name}")
def call_mcp_tool(tool_name: str, params: dict = None):
    """
    Execute an MCP tool with given parameters.

    Example:
    POST /mcp/call/search_transactions
    {
        "prefecture_code": "13",
        "property_types": "Pre-owned Condominiums, etc.",
        "price_max": 50000000,
        "limit": 10
    }
    """
    if params is None:
        params = {}

    # Route to appropriate endpoint based on tool name
    try:
        if tool_name == "search_transactions":
            return get_transactions(**params)
        elif tool_name == "get_price_trends":
            return get_price_trends(**params)
        elif tool_name == "get_district_prices":
            return get_price_by_district(**params)
        elif tool_name == "get_median_price":
            return get_median_price(**params)
        elif tool_name == "list_prefectures":
            return get_prefectures()
        elif tool_name == "list_municipalities":
            return get_municipalities(prefecture_code=params.get("prefecture_code"))
        elif tool_name == "list_property_types":
            return get_property_types()
        elif tool_name == "get_statistics":
            return get_stats_summary()
        else:
            raise HTTPException(status_code=404, detail=f"Unknown tool: {tool_name}")
    except TypeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid parameters: {str(e)}")

@app.get("/")
def root():
    """API root - documentation redirect."""
    return {
        "name": "Japan Real Estate Analytics API",
        "version": "1.0.0",
        "docs": "/docs",
        "mcp_tools": "/mcp/tools",
        "endpoints": {
            "reference_data": ["/prefectures", "/municipalities", "/districts", "/property-types", "/structures", "/floor-plans", "/year-range", "/building-year-range"],
            "statistics": ["/stats", "/stats/summary"],
            "transactions": ["/transactions", "/price-trends", "/median-price", "/price-by-district"],
            "mcp": ["/mcp/tools", "/mcp/call/{tool_name}"],
            "health": ["/health"]
        }
    }
