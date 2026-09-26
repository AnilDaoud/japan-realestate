"""
Japan Real Estate Analytics - FastAPI Backend
==============================================
REST API for Japanese real estate transaction data.

Run: uvicorn api:app --reload --port 8000
"""

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
import os
from typing import List, Optional, Dict, Any
from datetime import datetime
import requests
from functools import lru_cache
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
    allow_credentials=True,
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
# DATABASE HELPERS
# =============================================================================

def get_db_connection():
    """Create a new database connection."""
    return psycopg2.connect(DATABASE_URL)

def run_query(query: str, params: tuple = ()) -> List[Dict[str, Any]]:
    """Execute a query and return results as list of dicts."""
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description] if cur.description else []
            results = []
            for row in cur.fetchall():
                results.append(dict(row))
            conn.close()
            return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

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
def get_districts(municipality_codes: str = Query(...)):
    """Get districts for municipalities (comma-separated codes)."""
    codes = tuple(municipality_codes.split(","))
    placeholders = ",".join(["%s"] * len(codes))
    query = f"""
    SELECT DISTINCT district_name, municipality_code, municipality_name
    FROM transactions
    WHERE municipality_code IN ({placeholders})
    ORDER BY district
    """
    results = run_query(query, codes)
    return results

@app.get("/property-types")
def get_property_types():
    """Get all property types."""
    query = """
    SELECT DISTINCT property_type_raw
    FROM transactions
    WHERE property_type_raw IS NOT NULL
    ORDER BY property_type_raw
    """
    results = run_query(query)
    return [r["property_type_raw"] for r in results if r["property_type_raw"]]

@app.get("/structures")
def get_structures():
    """Get all building structure types."""
    query = """
    SELECT DISTINCT structure_type
    FROM transactions
    WHERE structure_type IS NOT NULL
    ORDER BY structure_type
    """
    results = run_query(query)
    return [r["structure_type"] for r in results]

@app.get("/floor-plans")
def get_floor_plans():
    """Get all floor plan types."""
    query = """
    SELECT DISTINCT floor_plan
    FROM transactions
    WHERE floor_plan IS NOT NULL
    ORDER BY floor_plan
    """
    results = run_query(query)
    return [r["floor_plan"] for r in results]

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
    SELECT MIN(year_built) as min_year, MAX(year_built) as max_year
    FROM transactions
    WHERE year_built IS NOT NULL
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
    year_min: Optional[int] = None,
    year_max: Optional[int] = None,
    price_min: Optional[float] = None,
    price_max: Optional[float] = None,
    area_min: Optional[float] = None,
    area_max: Optional[float] = None,
    limit: int = Query(1000, le=10000),
    offset: int = 0,
):
    """Query transactions with flexible filters."""
    conditions = []
    params = []

    if prefecture_code:
        conditions.append("prefecture_code = %s")
        params.append(prefecture_code)

    if municipality_codes:
        codes = tuple(municipality_codes.split(","))
        placeholders = ",".join(["%s"] * len(codes))
        conditions.append(f"municipality_code IN ({placeholders})")
        params.extend(codes)

    if districts:
        district_list = tuple(districts.split(","))
        placeholders = ",".join(["%s"] * len(district_list))
        conditions.append(f"district_name IN ({placeholders})")
        params.extend(district_list)

    if property_types:
        types = tuple(property_types.split(","))
        placeholders = ",".join(["%s"] * len(types))
        conditions.append(f"property_type IN ({placeholders})")
        params.extend(types)

    if year_min:
        conditions.append("transaction_year >= %s")
        params.append(year_min)

    if year_max:
        conditions.append("transaction_year <= %s")
        params.append(year_max)

    if price_min:
        conditions.append("transaction_price >= %s")
        params.append(price_min)

    if price_max:
        conditions.append("transaction_price <= %s")
        params.append(price_max)

    if area_min:
        conditions.append("area >= %s")
        params.append(area_min)

    if area_max:
        conditions.append("area <= %s")
        params.append(area_max)

    where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""

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
    frequency: str = Query("Quarterly", regex="^(Quarterly|Yearly)$"),
):
    """Get historical price trends."""
    conditions = []
    params = []

    if prefecture_code:
        conditions.append("prefecture_code = %s")
        params.append(prefecture_code)

    if municipality_codes:
        codes = tuple(municipality_codes.split(","))
        placeholders = ",".join(["%s"] * len(codes))
        conditions.append(f"municipality_code IN ({placeholders})")
        params.extend(codes)

    if districts:
        district_list = tuple(districts.split(","))
        placeholders = ",".join(["%s"] * len(district_list))
        conditions.append(f"district_name IN ({placeholders})")
        params.extend(district_list)

    if property_types:
        types = tuple(property_types.split(","))
        placeholders = ",".join(["%s"] * len(types))
        conditions.append(f"property_type IN ({placeholders})")
        params.extend(types)

    where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""

    if frequency == "Quarterly":
        group_by = "transaction_year, transaction_quarter"
        order_by = "transaction_year DESC, transaction_quarter DESC"
    else:
        group_by = "transaction_year"
        order_by = "transaction_year DESC"

    query = f"""
    SELECT
        transaction_year,
        transaction_quarter,
        COUNT(*) as volume,
        AVG(transaction_price) as avg_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY transaction_price) as median_price,
        AVG(CASE WHEN area > 0 THEN transaction_price / area ELSE NULL END) as avg_unit_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN area > 0 THEN transaction_price / area ELSE NULL END) as median_unit_price
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
    property_types: Optional[str] = None,
):
    """Get latest median price for an area."""
    conditions = []
    params = []

    if prefecture_code:
        conditions.append("prefecture_code = %s")
        params.append(prefecture_code)

    if municipality_codes:
        codes = tuple(municipality_codes.split(","))
        placeholders = ",".join(["%s"] * len(codes))
        conditions.append(f"municipality_code IN ({placeholders})")
        params.extend(codes)

    if property_types:
        types = tuple(property_types.split(","))
        placeholders = ",".join(["%s"] * len(types))
        conditions.append(f"property_type IN ({placeholders})")
        params.extend(types)

    where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""

    query = f"""
    SELECT
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY transaction_price) as median_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN area > 0 THEN transaction_price / area ELSE NULL END) as median_unit_price,
        AVG(transaction_price) as avg_price,
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
    property_types: Optional[str] = None,
    limit: int = Query(50, le=500),
):
    """Get median prices grouped by district."""
    conditions = []
    params = []

    if prefecture_code:
        conditions.append("prefecture_code = %s")
        params.append(prefecture_code)

    if municipality_codes:
        codes = tuple(municipality_codes.split(","))
        placeholders = ",".join(["%s"] * len(codes))
        conditions.append(f"municipality_code IN ({placeholders})")
        params.extend(codes)

    if property_types:
        types = tuple(property_types.split(","))
        placeholders = ",".join(["%s"] * len(types))
        conditions.append(f"property_type IN ({placeholders})")
        params.extend(types)

    where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""

    query = f"""
    SELECT
        district_name,
        municipality_name,
        COUNT(*) as transaction_count,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY transaction_price) as median_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN area > 0 THEN transaction_price / area ELSE NULL END) as median_unit_price,
        AVG(transaction_price) as avg_price
    FROM transactions
    {where_clause}
    GROUP BY district_name, municipality_name
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
        raise HTTPException(status_code=503, detail=f"Database connection failed: {str(e)}")

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
        "property_types": "Apartment",
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
            "reference_data": ["/prefectures", "/municipalities", "/districts", "/property-types", "/structures", "/floor-plans"],
            "statistics": ["/stats", "/stats/summary"],
            "transactions": ["/transactions", "/price-trends", "/median-price", "/price-by-district"],
            "mcp": ["/mcp/tools", "/mcp/call/{tool_name}"],
            "health": ["/health"]
        }
    }
