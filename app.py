"""
MLIT Real Estate Dashboard
==========================
Streamlit app for exploring Japanese real estate transaction data.

Run: streamlit run app.py
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import requests
from datetime import datetime
from urllib.parse import urlencode
import io

# =============================================================================
# GITHUB REPO FOR ISSUES/CONTACT
# =============================================================================

GITHUB_REPO_URL = "https://github.com/AnilDaoud/japan-realestate"

# =============================================================================
# JAPANESE REAL ESTATE TERM TOOLTIPS
# =============================================================================

TOOLTIPS = {
    "tsubo": "Tsubo (坪) is a traditional Japanese unit of area. 1 tsubo ≈ 3.31 m² ≈ 35.58 sq ft. Common in real estate listings.",
    "ldk": "Japanese floor plan notation: L=Living room, D=Dining room, K=Kitchen. Example: 2LDK = 2 bedrooms + Living/Dining/Kitchen area.",
    "mansion": "In Japan, 'mansion' (マンション) refers to a concrete apartment/condo building, not a large house.",
    "chome": "Chome (丁目) is a subdivision of a district, like a block number. Example: Roppongi 1-chome.",
    "rc": "RC = Reinforced Concrete. SRC = Steel Reinforced Concrete. These are common building structure types in Japan.",
    "unit_price": "Price per square meter (¥/m²) or per tsubo. This normalizes prices across different unit sizes for comparison.",
    "building_age": "Years since construction. Older buildings typically depreciate, but location and maintenance matter.",
    "coverage_ratio": "Building Coverage Ratio (建蔽率): Max % of land that can be covered by buildings. Set by zoning.",
    "floor_area_ratio": "Floor Area Ratio (容積率): Max total floor area as % of land area. Higher = taller buildings allowed.",
}

# =============================================================================
# CONSTANTS
# =============================================================================

# Conversion: 1 tsubo = 3.30579 m²
TSUBO_TO_M2 = 3.30579
M2_TO_TSUBO = 1 / TSUBO_TO_M2


def generate_share_url(filters, tab_name, base_url=None):
    """Generate a shareable URL with all current filter values."""
    params = {"tab": tab_name}

    # Add non-None filter values
    if filters.get('prefecture_code'):
        params['pref'] = filters['prefecture_code']
    if filters.get('municipality_codes'):
        params['muni'] = ','.join(filters['municipality_codes'])
    if filters.get('districts'):
        params['dist'] = ','.join(filters['districts'])
    if filters.get('property_types'):
        params['prop'] = ','.join(filters['property_types'])
    if filters.get('year_range'):
        params['yr'] = f"{filters['year_range'][0]}-{filters['year_range'][1]}"
    if filters.get('building_year_range'):
        params['byr'] = f"{filters['building_year_range'][0]}-{filters['building_year_range'][1]}"
    if filters.get('area_range'):
        params['area'] = f"{filters['area_range'][0]}-{filters['area_range'][1]}"
    if filters.get('price_range'):
        params['price'] = f"{filters['price_range'][0]}-{filters['price_range'][1]}"

    query_string = urlencode(params)
    if base_url:
        return f"{base_url}?{query_string}"
    return f"?{query_string}"


def generate_valuation_pdf(valuation_data):
    """Generate a PDF report for property valuation."""
    # Create HTML content for the report
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Property Valuation Report</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 40px; line-height: 1.6; }}
            .header {{ text-align: center; border-bottom: 2px solid #333; padding-bottom: 20px; margin-bottom: 30px; }}
            .header h1 {{ color: #2E86AB; margin-bottom: 5px; }}
            .section {{ margin-bottom: 25px; }}
            .section h2 {{ color: #333; border-bottom: 1px solid #ddd; padding-bottom: 5px; }}
            table {{ width: 100%; border-collapse: collapse; margin: 15px 0; }}
            th, td {{ padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }}
            th {{ background-color: #f5f5f5; }}
            .highlight {{ background-color: #e8f4f8; font-weight: bold; }}
            .verdict {{ font-size: 1.3em; padding: 15px; border-radius: 8px; text-align: center; margin: 20px 0; }}
            .verdict.good {{ background-color: #d4edda; color: #155724; }}
            .verdict.fair {{ background-color: #fff3cd; color: #856404; }}
            .verdict.high {{ background-color: #f8d7da; color: #721c24; }}
            .footer {{ margin-top: 40px; padding-top: 20px; border-top: 1px solid #ddd; font-size: 0.9em; color: #666; }}
            .disclaimer {{ font-size: 0.8em; color: #999; margin-top: 30px; }}
            @media print {{ body {{ margin: 20px; }} }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>🏠 Property Valuation Report</h1>
            <p>Generated on {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
        </div>

        <div class="section">
            <h2>Property Details</h2>
            <table>
                <tr><th>Location</th><td>{valuation_data.get('location', 'N/A')}</td></tr>
                <tr><th>District</th><td>{valuation_data.get('district', 'N/A')}</td></tr>
                <tr><th>Property Type</th><td>{valuation_data.get('property_type', 'N/A')}</td></tr>
                <tr><th>Area</th><td>{valuation_data.get('area', 0):.1f} m² ({valuation_data.get('area', 0) * M2_TO_TSUBO:.1f} tsubo)</td></tr>
                <tr><th>Year Built</th><td>{valuation_data.get('building_year', 'N/A')}</td></tr>
                <tr><th>Building Age</th><td>{valuation_data.get('building_age', 'N/A')} years</td></tr>
                <tr><th>Layout</th><td>{valuation_data.get('floor_plan', 'N/A')}</td></tr>
            </table>
        </div>

        <div class="section">
            <h2>Valuation Summary</h2>
            <div class="verdict {valuation_data.get('verdict_class', 'fair')}">
                {valuation_data.get('verdict', 'Fair Price')}
            </div>
            <table>
                <tr class="highlight"><th>Estimated Market Value</th><td>¥{valuation_data.get('estimated_value', 0):,.0f}</td></tr>
                <tr><th>Low Estimate</th><td>¥{valuation_data.get('low_estimate', 0):,.0f}</td></tr>
                <tr><th>High Estimate</th><td>¥{valuation_data.get('high_estimate', 0):,.0f}</td></tr>
                <tr><th>Median Price per m²</th><td>¥{valuation_data.get('median_price_m2', 0):,.0f}</td></tr>
                <tr><th>Comparable Transactions</th><td>{valuation_data.get('comparable_count', 0)}</td></tr>
            </table>
        </div>

        {f'''
        <div class="section">
            <h2>Listing Analysis</h2>
            <table>
                <tr><th>Listing Price</th><td>¥{valuation_data.get('listing_price', 0):,.0f}</td></tr>
                <tr><th>Listing Price per m²</th><td>¥{valuation_data.get('listing_price_m2', 0):,.0f}</td></tr>
                <tr><th>Difference from Market</th><td>¥{valuation_data.get('price_diff', 0):+,.0f} ({valuation_data.get('price_diff_pct', 0):+.1f}%)</td></tr>
                <tr><th>Price Percentile</th><td>{valuation_data.get('percentile', 0):.0f}%</td></tr>
            </table>
        </div>
        ''' if valuation_data.get('listing_price') else ''}

        <div class="section">
            <h2>Market Context</h2>
            <p>This valuation is based on <strong>{valuation_data.get('comparable_count', 0)}</strong> comparable
            transactions in <strong>{valuation_data.get('location', 'the selected area')}</strong> from the
            past 3 years, filtered by similar property characteristics.</p>
        </div>

        <div class="disclaimer">
            <p><strong>Disclaimer:</strong> This valuation is for informational purposes only and should not be
            considered as professional appraisal advice. Actual property values may vary based on specific
            conditions, market timing, and other factors not captured in this analysis.</p>
        </div>

        <div class="footer">
            <p>Data source: MLIT Real Estate Information Library (国土交通省不動産情報ライブラリ)</p>
            <p>Report generated by Japan Real Estate Analytics | <a href="{GITHUB_REPO_URL}">{GITHUB_REPO_URL}</a></p>
        </div>
    </body>
    </html>
    """
    return html_content

# =============================================================================
# CONFIG
# =============================================================================

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://localhost/mlit_realestate"
)

st.set_page_config(
    page_title="Japan Real Estate Analytics",
    page_icon="🏠",
    layout="wide"
)

# Tab state management via query params
TAB_NAMES = ["charts", "map", "cohorts", "micro", "valuation", "data"]
TAB_LABELS = ["📈 Charts", "🗺️ Map", "📅 Age Cohorts", "📍 District", "💰 Valuation", "📋 Raw Data"]

def get_current_tab():
    """Get current tab from query params, default to first tab."""
    params = st.query_params
    tab = params.get("tab", TAB_NAMES[0])
    if tab in TAB_NAMES:
        return TAB_NAMES.index(tab)
    return 0

def on_tab_change():
    """Callback when tab selection changes - update query params."""
    selected = st.session_state.main_nav
    tab_idx = TAB_LABELS.index(selected)
    st.query_params["tab"] = TAB_NAMES[tab_idx]

# =============================================================================
# DATABASE
# =============================================================================

@st.cache_resource
def get_connection():
    return psycopg2.connect(DATABASE_URL)

@st.cache_data(ttl=3600)
def run_query(query, params=None):
    conn = get_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, params or ())
        return pd.DataFrame(cur.fetchall())

@st.cache_data(ttl=3600)
def get_prefectures():
    return run_query("""
        SELECT DISTINCT p.code, p.name_en
        FROM prefectures p
        JOIN transactions t ON t.prefecture_code = p.code
        ORDER BY p.code
    """)

@st.cache_data(ttl=3600)
def get_municipalities(prefecture_code):
    return run_query("""
        SELECT DISTINCT m.code, COALESCE(m.name_en, m.name_ja) as name
        FROM municipalities m
        JOIN transactions t ON t.municipality_code = m.code
        WHERE m.prefecture_code = %s
        ORDER BY name
    """, (prefecture_code,))

@st.cache_data(ttl=3600)
def get_districts(municipality_codes):
    if not municipality_codes:
        return pd.DataFrame(columns=['district_name'])
    return run_query("""
        SELECT DISTINCT district_name
        FROM transactions
        WHERE municipality_code = ANY(%s)
          AND district_name IS NOT NULL
          AND district_name != ''
        ORDER BY district_name
    """, (municipality_codes,))

@st.cache_data(ttl=3600)
def get_districts_by_prefecture(prefecture_code):
    """Get districts for prefectures where municipality_code is NULL."""
    return run_query("""
        SELECT DISTINCT district_name
        FROM transactions
        WHERE prefecture_code = %s
          AND municipality_code IS NULL
          AND district_name IS NOT NULL
          AND district_name != ''
        ORDER BY district_name
        LIMIT 500
    """, (prefecture_code,))

@st.cache_data(ttl=3600)
def search_districts(prefecture_code, search_term):
    """Search for districts by name pattern."""
    if not search_term or len(search_term) < 2:
        return pd.DataFrame(columns=['district_name', 'tx_count'])
    return run_query("""
        SELECT DISTINCT district_name, COUNT(*) as tx_count
        FROM transactions
        WHERE prefecture_code = %s
          AND district_name IS NOT NULL
          AND district_name != ''
          AND district_name ILIKE %s
        GROUP BY district_name
        ORDER BY tx_count DESC
        LIMIT 50
    """, (prefecture_code, f'%{search_term}%'))

@st.cache_data(ttl=3600)
def get_property_types():
    return run_query("""
        SELECT DISTINCT property_type_raw
        FROM transactions
        WHERE property_type_raw IS NOT NULL AND property_type_raw != ''
        ORDER BY property_type_raw
    """)

@st.cache_data(ttl=3600)
def get_structures():
    return run_query("""
        SELECT DISTINCT structure
        FROM transactions
        WHERE structure IS NOT NULL AND structure != ''
        ORDER BY structure
    """)

@st.cache_data(ttl=3600)
def get_floor_plans():
    return run_query("""
        SELECT DISTINCT floor_plan
        FROM transactions
        WHERE floor_plan IS NOT NULL AND floor_plan != ''
        ORDER BY floor_plan
    """)

@st.cache_data(ttl=3600)
def get_year_range():
    result = run_query("""
        SELECT MIN(transaction_year) as min_year, MAX(transaction_year) as max_year
        FROM transactions
    """)
    return int(result['min_year'].iloc[0]), int(result['max_year'].iloc[0])

@st.cache_data(ttl=3600)
def get_building_year_range():
    result = run_query("""
        SELECT MIN(building_year) as min_year, MAX(building_year) as max_year
        FROM transactions
        WHERE building_year IS NOT NULL AND building_year > 1900
    """)
    return int(result['min_year'].iloc[0]), int(result['max_year'].iloc[0])

@st.cache_data(ttl=3600)
def get_stations(prefecture_code):
    """Get stations with transaction data for a prefecture."""
    return run_query("""
        SELECT DISTINCT s.code, COALESCE(s.name_en, s.name_ja) as name
        FROM stations s
        JOIN transactions t ON t.nearest_station_code = s.code
        WHERE s.municipality_code IN (
            SELECT code FROM municipalities WHERE prefecture_code = %s
        )
        ORDER BY name
        LIMIT 500
    """, (prefecture_code,))

@st.cache_data(ttl=3600)
def get_map_data(filters, latest_only=False):
    """Get aggregated price data by municipality for map visualization.

    Args:
        filters: Standard filters dict
        latest_only: If True, only use data from the most recent year in the range
    """
    # Determine year filter
    year_start = filters.get('year_range', [2005, 2025])[0]
    year_end = filters.get('year_range', [2005, 2025])[1]

    if latest_only:
        # Use only the most recent year
        year_start = year_end

    query = """
        SELECT
            m.code as municipality_code,
            COALESCE(m.name_en, m.name_ja) as name,
            COUNT(*) as transactions,
            ROUND(AVG(t.unit_price)) as avg_price_m2,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.unit_price)::INTEGER as median_price_m2
        FROM transactions t
        JOIN municipalities m ON t.municipality_code = m.code
        WHERE t.prefecture_code = %s
          AND t.unit_price IS NOT NULL
          AND t.unit_price > 0
          AND t.unit_price < 50000000
          AND t.transaction_year BETWEEN %s AND %s
    """
    params = [filters['prefecture_code'], year_start, year_end]

    if filters.get('property_types'):
        query += " AND t.property_type_raw = ANY(%s)"
        params.append(filters['property_types'])

    query += " GROUP BY m.code, m.name_en, m.name_ja HAVING COUNT(*) >= 5"
    query += " ORDER BY median_price_m2 DESC"

    return run_query(query, params)


@st.cache_data(ttl=3600)
def get_latest_median_price(filters):
    """Get median price for the latest quarter (last data point) in the selected range."""
    year_start = filters.get('year_range', [2005, 2025])[0]
    year_end = filters.get('year_range', [2005, 2025])[1]

    # First, find the latest quarter with data in the range
    latest_query = """
        SELECT t.transaction_year, t.transaction_quarter
        FROM transactions t
        WHERE t.unit_price IS NOT NULL
          AND t.unit_price > 0
          AND t.transaction_year BETWEEN %s AND %s
    """
    latest_params = [year_start, year_end]

    if filters.get('prefecture_code'):
        latest_query = latest_query.replace("WHERE", "WHERE t.prefecture_code = %s AND")
        latest_params.insert(0, filters['prefecture_code'])

    if filters.get('municipality_codes'):
        latest_query += " AND t.municipality_code = ANY(%s)"
        latest_params.append(filters['municipality_codes'])

    if filters.get('property_types'):
        latest_query += " AND t.property_type_raw = ANY(%s)"
        latest_params.append(filters['property_types'])

    if filters.get('districts'):
        latest_query += " AND t.district_name = ANY(%s)"
        latest_params.append(filters['districts'])

    if filters.get('station_codes'):
        latest_query += " AND t.nearest_station_code = ANY(%s)"
        latest_params.append(filters['station_codes'])

    latest_query += " ORDER BY t.transaction_year DESC, t.transaction_quarter DESC LIMIT 1"

    latest_result = run_query(latest_query, latest_params)
    if latest_result.empty:
        return None, None

    latest_year = int(latest_result['transaction_year'].iloc[0])
    latest_quarter = int(latest_result['transaction_quarter'].iloc[0])

    # Now get the median for that specific quarter
    query = """
        SELECT
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.unit_price) as median
        FROM transactions t
        WHERE t.unit_price IS NOT NULL
          AND t.unit_price > 0
          AND t.unit_price < 50000000
          AND t.transaction_year = %s
          AND t.transaction_quarter = %s
    """
    params = [latest_year, latest_quarter]

    if filters.get('prefecture_code'):
        query = query.replace("WHERE", "WHERE t.prefecture_code = %s AND")
        params.insert(0, filters['prefecture_code'])

    if filters.get('municipality_codes'):
        query += " AND t.municipality_code = ANY(%s)"
        params.append(filters['municipality_codes'])

    if filters.get('property_types'):
        query += " AND t.property_type_raw = ANY(%s)"
        params.append(filters['property_types'])

    if filters.get('districts'):
        query += " AND t.district_name = ANY(%s)"
        params.append(filters['districts'])

    if filters.get('station_codes'):
        query += " AND t.nearest_station_code = ANY(%s)"
        params.append(filters['station_codes'])

    result = run_query(query, params)
    if not result.empty and result['median'].iloc[0]:
        return float(result['median'].iloc[0]), f"{latest_year} Q{latest_quarter}", latest_year, latest_quarter
    return None, None, None, None

# =============================================================================
# CURRENCY CONVERSION (FX rates from database)
# =============================================================================

@st.cache_data(ttl=3600)
def get_historical_fx_rates(start_year, end_year, target_currency="USD"):
    """Fetch historical quarterly FX rates from database."""
    result = run_query("""
        SELECT year, quarter, rate
        FROM fx_rates
        WHERE currency = %s
          AND year BETWEEN %s AND %s
        ORDER BY year, quarter
    """, (target_currency, start_year, end_year))

    rates = {}
    if not result.empty:
        for _, row in result.iterrows():
            rates[(int(row['year']), int(row['quarter']))] = float(row['rate'])
    return rates

@st.cache_data(ttl=3600)
def get_current_fx_rate(target_currency="USD"):
    """Get the most recent FX rate for display purposes."""
    result = run_query("""
        SELECT rate
        FROM fx_rates
        WHERE currency = %s
        ORDER BY year DESC, quarter DESC
        LIMIT 1
    """, (target_currency,))

    if not result.empty:
        return float(result['rate'].iloc[0])
    return None

# =============================================================================
# QUERY BUILDER
# =============================================================================

def build_query(select_clause, filters, group_by=None, order_by=None, limit=None):
    """Build SQL query with dynamic filters."""
    query = f"SELECT {select_clause} FROM transactions t"

    # Join municipalities if needed
    if 'ward' in select_clause.lower() or 'm.name_en' in select_clause:
        query += " LEFT JOIN municipalities m ON t.municipality_code = m.code"

    conditions = ["t.unit_price IS NOT NULL", "t.unit_price > 0", "t.unit_price < 50000000"]
    params = []

    if filters.get('prefecture_code'):
        conditions.append("t.prefecture_code = %s")
        params.append(filters['prefecture_code'])

    if filters.get('municipality_codes'):
        conditions.append("t.municipality_code = ANY(%s)")
        params.append(filters['municipality_codes'])
    elif filters.get('no_municipality_data'):
        # For prefectures like Hokkaido where municipality_code is NULL
        conditions.append("t.municipality_code IS NULL")

    if filters.get('districts'):
        conditions.append("t.district_name = ANY(%s)")
        params.append(filters['districts'])

    if filters.get('station_codes'):
        conditions.append("t.nearest_station_code = ANY(%s)")
        params.append(filters['station_codes'])

    if filters.get('property_types'):
        conditions.append("t.property_type_raw = ANY(%s)")
        params.append(filters['property_types'])

    if filters.get('structures'):
        conditions.append("t.structure = ANY(%s)")
        params.append(filters['structures'])

    if filters.get('floor_plans'):
        conditions.append("t.floor_plan = ANY(%s)")
        params.append(filters['floor_plans'])

    if filters.get('year_range'):
        conditions.append("t.transaction_year BETWEEN %s AND %s")
        params.extend(filters['year_range'])

    if filters.get('building_year_range'):
        conditions.append("t.building_year BETWEEN %s AND %s")
        params.extend(filters['building_year_range'])

    if filters.get('price_range'):
        conditions.append("t.trade_price BETWEEN %s AND %s")
        params.extend([filters['price_range'][0] * 1000000, filters['price_range'][1] * 1000000])

    if filters.get('price_m2_range'):
        conditions.append("t.unit_price BETWEEN %s AND %s")
        params.extend([filters['price_m2_range'][0] * 10000, filters['price_m2_range'][1] * 10000])

    if filters.get('area_range'):
        conditions.append("t.area_m2 BETWEEN %s AND %s")
        params.extend(filters['area_range'])

    query += " WHERE " + " AND ".join(conditions)

    if group_by:
        query += f" GROUP BY {group_by}"

    if order_by:
        query += f" ORDER BY {order_by}"

    if limit:
        query += f" LIMIT {limit}"

    return query, params

# =============================================================================
# UI - SIDEBAR FILTERS
# =============================================================================

st.sidebar.header("📍 Location Filters")

# Prefecture
prefectures = get_prefectures()
prefecture_options = dict(zip(prefectures['name_en'], prefectures['code']))
selected_prefecture_name = st.sidebar.selectbox(
    "Prefecture",
    options=list(prefecture_options.keys()),
    index=list(prefecture_options.keys()).index("Tokyo") if "Tokyo" in prefecture_options else 0
)
selected_prefecture = prefecture_options[selected_prefecture_name]

# Municipality (Ward/City)
municipalities = get_municipalities(selected_prefecture)
if not municipalities.empty and 'name' in municipalities.columns:
    municipality_options = dict(zip(municipalities['name'], municipalities['code']))
else:
    municipality_options = {}

# Check if this prefecture has municipality data
has_municipality_data = len(municipality_options) > 0

if has_municipality_data:
    # Default to Minato Ward for Tokyo
    default_municipality = []
    if selected_prefecture_name == "Tokyo" and "Minato Ward" in municipality_options:
        default_municipality = ["Minato Ward"]

    selected_municipalities = st.sidebar.multiselect(
        "Ward / City",
        options=list(municipality_options.keys()),
        default=default_municipality
    )
    selected_municipality_codes = [municipality_options[m] for m in selected_municipalities] if selected_municipalities else None

    # District (Chome) - only show when municipalities are selected
    if selected_municipality_codes:
        districts = get_districts(selected_municipality_codes)
        if not districts.empty:
            selected_districts = st.sidebar.multiselect(
                "District / Chome",
                options=districts['district_name'].tolist(),
                help=TOOLTIPS["chome"]
            )
            selected_districts = selected_districts if selected_districts else None
        else:
            selected_districts = None
    else:
        selected_districts = None
else:
    # No municipality data for this prefecture - show districts directly
    selected_municipality_codes = None
    st.sidebar.caption("ℹ️ No ward/city data - use search to find districts")

    # Text search for districts
    district_search = st.sidebar.text_input(
        "🔍 Search District",
        placeholder="e.g. Niseko, Kutchan, Sapporo",
        help="Type at least 2 characters to search"
    )

    if district_search and len(district_search) >= 2:
        # Search for matching districts
        search_results = search_districts(selected_prefecture, district_search)
        if not search_results.empty:
            # Show results with transaction counts
            district_options = [f"{row['district_name']} ({row['tx_count']:,})" for _, row in search_results.iterrows()]
            district_map = {f"{row['district_name']} ({row['tx_count']:,})": row['district_name'] for _, row in search_results.iterrows()}

            selected_district_labels = st.sidebar.multiselect(
                "Matching Districts",
                options=district_options
            )
            selected_districts = [district_map[label] for label in selected_district_labels] if selected_district_labels else None
        else:
            st.sidebar.caption("No districts found matching your search")
            selected_districts = None
    else:
        # Fallback to top districts by transaction count
        districts = get_districts_by_prefecture(selected_prefecture)
        if not districts.empty:
            selected_districts = st.sidebar.multiselect(
                "District / Area (top 500)",
                options=districts['district_name'].tolist()
            )
            selected_districts = selected_districts if selected_districts else None
        else:
            selected_districts = None

# Station filter
stations = get_stations(selected_prefecture)
if not stations.empty and 'name' in stations.columns:
    station_options = dict(zip(stations['name'], stations['code']))
    selected_stations = st.sidebar.multiselect(
        "🚉 Near Station",
        options=list(station_options.keys())
    )
    selected_station_codes = [station_options[s] for s in selected_stations] if selected_stations else None
else:
    selected_station_codes = None

st.sidebar.header("🏢 Property Filters")

# Property type - default to Pre-owned Condominiums
property_types = get_property_types()
property_type_list = property_types['property_type_raw'].tolist()
default_property_type = ["Pre-owned Condominiums, etc."] if "Pre-owned Condominiums, etc." in property_type_list else []
selected_property_types = st.sidebar.multiselect(
    "Property Type",
    options=property_type_list,
    default=default_property_type
)
selected_property_types = selected_property_types if selected_property_types else None

# Structure
structures = get_structures()
selected_structures = st.sidebar.multiselect(
    "Structure (RC, Wood, etc.)",
    options=structures['structure'].tolist(),
    help=TOOLTIPS["rc"]
)
selected_structures = selected_structures if selected_structures else None

# Floor plan
floor_plans = get_floor_plans()
selected_floor_plans = st.sidebar.multiselect(
    "Layout (1LDK, 2DK, etc.)",
    options=floor_plans['floor_plan'].tolist(),
    help=TOOLTIPS["ldk"]
)
selected_floor_plans = selected_floor_plans if selected_floor_plans else None

st.sidebar.header("📏 Size Filters")

# Area range
area_range = st.sidebar.slider(
    "Area (m²)",
    min_value=0,
    max_value=500,
    value=(0, 500),
    step=10
)
area_range = area_range if area_range != (0, 500) else None

# Note: building_floors and walk_minutes removed - 0% populated in data

st.sidebar.header("💰 Price Filters")

# Price range (millions)
price_range = st.sidebar.slider(
    "Total Price (¥ millions)",
    min_value=0,
    max_value=500,
    value=(0, 500),
    step=5
)
price_range = price_range if price_range != (0, 500) else None

# Price per m2 (万円)
price_m2_range = st.sidebar.slider(
    "Price per m² (¥ 万)",
    min_value=0,
    max_value=500,
    value=(0, 500),
    step=10,
    help=TOOLTIPS["unit_price"]
)
price_m2_range = price_m2_range if price_m2_range != (0, 500) else None

st.sidebar.header("📅 Date Filters")

# Transaction year range
min_year, max_year = get_year_range()
year_range = st.sidebar.slider(
    "Transaction Year",
    min_value=min_year,
    max_value=max_year,
    value=(min_year, max_year)  # Default to full range
)

# Building year range
min_build_year, max_build_year = get_building_year_range()
building_year_range = st.sidebar.slider(
    "Year Built",
    min_value=min_build_year,
    max_value=max_build_year,
    value=(min_build_year, max_build_year),
    help=TOOLTIPS["building_age"]
)
building_year_range = building_year_range if building_year_range != (min_build_year, max_build_year) else None

st.sidebar.header("📊 Chart Options")

# Aggregation frequency
frequency = st.sidebar.selectbox(
    "Aggregation",
    options=["Quarterly", "Yearly"],
    index=0
)

# Chart mode toggle
chart_mode = st.sidebar.radio(
    "Chart Mode",
    options=["Time Series", "Histogram", "Scatter (X vs Y)"],
    index=0,
    horizontal=True
)

# X/Y axis options for scatter mode
if chart_mode == "Scatter (X vs Y)":
    scatter_x = st.sidebar.selectbox(
        "X Axis",
        options=["Building Age", "Area (m²)", "Year Built", "Transaction Year"],
        index=0
    )
    scatter_y = st.sidebar.selectbox(
        "Y Axis",
        options=["Price per m²", "Total Price"],
        index=0
    )
else:
    scatter_x = None
    scatter_y = None

st.sidebar.header("🔄 Display Options")

# Price unit toggle (m² vs tsubo)
price_unit = st.sidebar.radio(
    "Price Unit",
    options=["per m²", "per tsubo"],
    index=0,
    horizontal=True,
    help=TOOLTIPS["tsubo"]
)
use_tsubo = price_unit == "per tsubo"

# Currency toggle
currency = st.sidebar.radio(
    "Currency",
    options=["JPY", "USD", "EUR", "GBP"],
    index=0,
    horizontal=True
)
use_fx = currency != "JPY"

# Fetch FX rates if needed (cached)
fx_rates = {}
current_fx_rate = None
if use_fx:
    fx_rates = get_historical_fx_rates(min_year, max_year, currency)
    current_fx_rate = get_current_fx_rate(currency)
    if current_fx_rate:
        st.sidebar.caption(f"Current rate: ¥1 = {currency} {current_fx_rate:.6f}")

# Build filters dict
filters = {
    'prefecture_code': selected_prefecture,
    'municipality_codes': selected_municipality_codes,
    'no_municipality_data': not has_municipality_data,  # For prefectures like Hokkaido
    'districts': selected_districts,
    'station_codes': selected_station_codes,
    'property_types': selected_property_types,
    'structures': selected_structures,
    'floor_plans': selected_floor_plans,
    'year_range': year_range,
    'building_year_range': building_year_range,
    'price_range': price_range,
    'price_m2_range': price_m2_range,
    'area_range': area_range,
}

# =============================================================================
# HELPER FUNCTIONS FOR DISPLAY
# =============================================================================

def convert_price(price_jpy, year=None, quarter=None):
    """Convert price from JPY to selected currency using historical rates."""
    if not use_fx or price_jpy is None:
        return price_jpy

    # Convert Decimal to float if needed
    price = float(price_jpy) if price_jpy is not None else None

    # Try to get historical rate
    if year and quarter and (year, quarter) in fx_rates:
        rate = fx_rates[(year, quarter)]
    elif current_fx_rate:
        rate = current_fx_rate
    else:
        return price  # No rate available

    return price * rate

def convert_to_tsubo(price_per_m2):
    """Convert price/m² to price/tsubo."""
    if price_per_m2 is None:
        return None
    return price_per_m2 * TSUBO_TO_M2

def format_price(price, year=None, quarter=None, is_unit_price=True):
    """Format price with currency symbol and optional tsubo conversion."""
    if price is None:
        return "N/A"

    # Convert to selected currency
    converted = convert_price(price, year, quarter)

    # Convert to tsubo if needed (only for unit prices)
    if is_unit_price and use_tsubo:
        converted = convert_to_tsubo(converted)

    # Format with appropriate symbol
    if use_fx:
        if currency == "USD":
            return f"${converted:,.0f}"
        elif currency == "EUR":
            return f"€{converted:,.0f}"
        elif currency == "GBP":
            return f"£{converted:,.0f}"
    return f"¥{converted:,.0f}"

def get_unit_label():
    """Get the current unit label for charts."""
    unit = "tsubo" if use_tsubo else "m²"
    if use_fx:
        return f"{currency}/{unit}"
    return f"¥/{unit}"

# =============================================================================
# QUERIES WITH FILTERS
# =============================================================================

def get_price_trends(filters, frequency='Quarterly'):
    if frequency == 'Yearly':
        select = """
            t.transaction_year,
            COUNT(*) as transaction_count,
            ROUND(AVG(t.unit_price)) as avg_price_m2,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.unit_price) as median_price_m2,
            ROUND(AVG(t.trade_price)) as avg_price
        """
        group_by = "t.transaction_year"
        order_by = "t.transaction_year"
    else:
        select = """
            t.transaction_year,
            t.transaction_quarter,
            COUNT(*) as transaction_count,
            ROUND(AVG(t.unit_price)) as avg_price_m2,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.unit_price) as median_price_m2,
            ROUND(AVG(t.trade_price)) as avg_price
        """
        group_by = "t.transaction_year, t.transaction_quarter"
        order_by = "t.transaction_year, t.transaction_quarter"

    query, params = build_query(select, filters, group_by, order_by)
    return run_query(query, params)

def get_scatter_data(filters, limit=5000):
    select = """
        t.transaction_year,
        t.unit_price,
        t.trade_price,
        t.area_m2,
        t.building_year,
        (t.transaction_year - t.building_year) as building_age,
        t.floor_plan,
        t.municipality_code,
        t.structure
    """
    query, params = build_query(select, filters, limit=limit)
    # Add random ordering for sampling
    query = query.replace(f"LIMIT {limit}", f"ORDER BY RANDOM() LIMIT {limit}")
    return run_query(query, params)

def get_histogram_data(filters, limit=50000):
    select = "t.unit_price"
    query, params = build_query(select, filters, limit=limit)
    return run_query(query, params)

def get_ward_comparison(filters):
    select = """
        m.name_en as ward,
        COUNT(*) as transactions,
        ROUND(AVG(t.unit_price)) as avg_price_m2,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.unit_price) as median_price_m2
    """
    # Don't filter by municipality for comparison
    comparison_filters = {k: v for k, v in filters.items() if k != 'municipality_codes' and k != 'districts'}

    query = f"""
        SELECT {select}
        FROM transactions t
        LEFT JOIN municipalities m ON t.municipality_code = m.code
        WHERE t.unit_price IS NOT NULL
          AND t.unit_price > 0
          AND t.unit_price < 50000000
          AND t.prefecture_code = %s
    """
    params = [comparison_filters['prefecture_code']]

    if comparison_filters.get('year_range'):
        query += " AND t.transaction_year BETWEEN %s AND %s"
        params.extend(comparison_filters['year_range'])

    if comparison_filters.get('property_types'):
        query += " AND t.property_type_raw = ANY(%s)"
        params.append(comparison_filters['property_types'])

    query += " GROUP BY m.name_en HAVING COUNT(*) > 10 ORDER BY avg_price_m2 DESC"

    return run_query(query, params)

def get_age_vs_price_by_area(filters):
    """Get average price by building age, grouped by area size."""
    select = """
        (t.transaction_year - t.building_year) as building_age,
        CASE
            WHEN t.area_m2 < 40 THEN 'Small (<40m²)'
            WHEN t.area_m2 < 70 THEN 'Medium (40-70m²)'
            WHEN t.area_m2 < 100 THEN 'Large (70-100m²)'
            ELSE 'XL (100m²+)'
        END as size_category,
        ROUND(AVG(t.unit_price)) as avg_price_m2,
        COUNT(*) as count
    """
    query, params = build_query(select, filters)
    query += " AND t.building_year IS NOT NULL"
    query += " GROUP BY building_age, size_category"
    query += " HAVING COUNT(*) >= 5"
    query += " ORDER BY building_age, size_category"

    return run_query(query, params)


def get_station_price_trends(prefecture_code, station_codes, filters, frequency='Quarterly'):
    """Get price trends grouped by station."""
    if not station_codes:
        return pd.DataFrame()

    year_start = filters.get('year_range', [2005, 2025])[0]
    year_end = filters.get('year_range', [2005, 2025])[1]

    if frequency == 'Yearly':
        select = """
            s.code as station_code,
            COALESCE(s.name_en, s.name_ja) as station_name,
            t.transaction_year,
            COUNT(*) as transaction_count,
            ROUND(AVG(t.unit_price)) as avg_price_m2,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.unit_price) as median_price_m2
        """
        group_by = "s.code, s.name_en, s.name_ja, t.transaction_year"
        order_by = "t.transaction_year, station_name"
    else:
        select = """
            s.code as station_code,
            COALESCE(s.name_en, s.name_ja) as station_name,
            t.transaction_year,
            t.transaction_quarter,
            COUNT(*) as transaction_count,
            ROUND(AVG(t.unit_price)) as avg_price_m2,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.unit_price) as median_price_m2
        """
        group_by = "s.code, s.name_en, s.name_ja, t.transaction_year, t.transaction_quarter"
        order_by = "t.transaction_year, t.transaction_quarter, station_name"

    query = f"""
        SELECT {select}
        FROM transactions t
        JOIN stations s ON t.nearest_station_code = s.code
        WHERE t.prefecture_code = %s
          AND t.nearest_station_code = ANY(%s)
          AND t.unit_price IS NOT NULL
          AND t.unit_price > 0
          AND t.unit_price < 50000000
          AND t.transaction_year BETWEEN %s AND %s
    """
    params = [prefecture_code, station_codes, year_start, year_end]

    if filters.get('property_types'):
        query += " AND t.property_type_raw = ANY(%s)"
        params.append(filters['property_types'])

    query += f" GROUP BY {group_by} HAVING COUNT(*) >= 3"
    query += f" ORDER BY {order_by}"

    return run_query(query, params)


def get_district_price_trends(prefecture_code, municipality_codes, districts, filters, frequency='Quarterly'):
    """Get price trends grouped by district."""
    if not districts:
        return pd.DataFrame()

    year_start = filters.get('year_range', [2005, 2025])[0]
    year_end = filters.get('year_range', [2005, 2025])[1]

    if frequency == 'Yearly':
        select = """
            t.district_name,
            t.transaction_year,
            COUNT(*) as transaction_count,
            ROUND(AVG(t.unit_price)) as avg_price_m2,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.unit_price) as median_price_m2
        """
        group_by = "t.district_name, t.transaction_year"
        order_by = "t.transaction_year, t.district_name"
    else:
        select = """
            t.district_name,
            t.transaction_year,
            t.transaction_quarter,
            COUNT(*) as transaction_count,
            ROUND(AVG(t.unit_price)) as avg_price_m2,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.unit_price) as median_price_m2
        """
        group_by = "t.district_name, t.transaction_year, t.transaction_quarter"
        order_by = "t.transaction_year, t.transaction_quarter, t.district_name"

    query = f"""
        SELECT {select}
        FROM transactions t
        WHERE t.prefecture_code = %s
          AND t.district_name = ANY(%s)
          AND t.unit_price IS NOT NULL
          AND t.unit_price > 0
          AND t.unit_price < 50000000
          AND t.transaction_year BETWEEN %s AND %s
    """
    params = [prefecture_code, districts, year_start, year_end]

    if municipality_codes:
        query += " AND t.municipality_code = ANY(%s)"
        params.append(municipality_codes)

    if filters.get('property_types'):
        query += " AND t.property_type_raw = ANY(%s)"
        params.append(filters['property_types'])

    query += f" GROUP BY {group_by} HAVING COUNT(*) >= 3"
    query += f" ORDER BY {order_by}"

    return run_query(query, params)


def get_station_rankings(prefecture_code, filters, limit=50):
    """Get stations ranked by median price."""
    year_start = filters.get('year_range', [2005, 2025])[0]
    year_end = filters.get('year_range', [2005, 2025])[1]

    query = """
        SELECT
            s.code as station_code,
            COALESCE(s.name_en, s.name_ja) as station_name,
            COUNT(*) as transaction_count,
            ROUND(AVG(t.unit_price)) as avg_price_m2,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.unit_price)::INTEGER as median_price_m2
        FROM transactions t
        JOIN stations s ON t.nearest_station_code = s.code
        WHERE t.prefecture_code = %s
          AND t.unit_price IS NOT NULL
          AND t.unit_price > 0
          AND t.unit_price < 50000000
          AND t.transaction_year BETWEEN %s AND %s
    """
    params = [prefecture_code, year_start, year_end]

    if filters.get('property_types'):
        query += " AND t.property_type_raw = ANY(%s)"
        params.append(filters['property_types'])

    query += f"""
        GROUP BY s.code, s.name_en, s.name_ja
        HAVING COUNT(*) >= 10
        ORDER BY median_price_m2 DESC
        LIMIT {limit}
    """

    return run_query(query, params)


def get_district_rankings(prefecture_code, municipality_codes, filters, limit=50):
    """Get districts ranked by median price."""
    year_start = filters.get('year_range', [2005, 2025])[0]
    year_end = filters.get('year_range', [2005, 2025])[1]

    query = """
        SELECT
            t.district_name,
            COUNT(*) as transaction_count,
            ROUND(AVG(t.unit_price)) as avg_price_m2,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.unit_price)::INTEGER as median_price_m2
        FROM transactions t
        WHERE t.prefecture_code = %s
          AND t.district_name IS NOT NULL
          AND t.district_name != ''
          AND t.unit_price IS NOT NULL
          AND t.unit_price > 0
          AND t.unit_price < 50000000
          AND t.transaction_year BETWEEN %s AND %s
    """
    params = [prefecture_code, year_start, year_end]

    if municipality_codes:
        query += " AND t.municipality_code = ANY(%s)"
        params.append(municipality_codes)

    if filters.get('property_types'):
        query += " AND t.property_type_raw = ANY(%s)"
        params.append(filters['property_types'])

    query += f"""
        GROUP BY t.district_name
        HAVING COUNT(*) >= 5
        ORDER BY median_price_m2 DESC
        LIMIT {limit}
    """

    return run_query(query, params)

# =============================================================================
# MAIN UI
# =============================================================================

st.title("🏠 Japan Real Estate Analytics")
st.caption("Data source: MLIT Real Estate Information Library | 6.1M+ transactions")

# Summary stats
col1, col2, col3, col4 = st.columns(4)

with st.spinner("Loading summary stats..."):
    # Quick count query
    count_query, count_params = build_query("COUNT(*) as count", filters)
    count_result = run_query(count_query, count_params)
    transaction_count = count_result['count'].iloc[0] if not count_result.empty else 0

    # Period median (over entire date range)
    median_query, median_params = build_query(
        "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.unit_price) as median",
        filters
    )
    median_result = run_query(median_query, median_params)
    median_price = median_result['median'].iloc[0] if not median_result.empty and median_result['median'].iloc[0] else 0

    # Latest median (most recent quarter - last data point)
    latest_median_price, latest_period, latest_year, latest_quarter = get_latest_median_price(filters)
    if latest_median_price is None:
        latest_median_price = median_price  # Fallback to period median
        latest_period = f"{year_range[1]}"
        latest_year = year_range[1]
        latest_quarter = 4  # Default to Q4

col1.metric("Matching Transactions", f"{transaction_count:,}")
col2.metric(
    f"Latest Median ({latest_period})",
    format_price(latest_median_price, year=latest_year, quarter=latest_quarter, is_unit_price=True),
    help=f"Median price per {get_unit_label()} for {latest_period} (last data point)"
)
col3.metric(
    f"Period Median ({year_range[0]}-{year_range[1]})",
    format_price(median_price, is_unit_price=True),
    help=f"Median price per {get_unit_label()} over the entire selected period"
)
col4.metric("Prefecture", selected_prefecture_name)

# Share button
share_url = generate_share_url(filters, TAB_NAMES[get_current_tab()])
share_col1, share_col2 = st.columns([6, 1])
with share_col2:
    if st.button("🔗 Share View", help="Copy a link to this view with your current filters"):
        st.session_state['show_share_url'] = True

if st.session_state.get('show_share_url'):
    st.info(f"**Share this view:** Copy the URL below\n\n`{share_url}`\n\n*(Append this to your app's base URL)*")
    if st.button("✕ Close"):
        st.session_state['show_share_url'] = False
        st.rerun()

# Tab navigation with state preservation
current_tab_idx = get_current_tab()

# Initialize session state for tab if needed
if "main_nav" not in st.session_state:
    st.session_state.main_nav = TAB_LABELS[current_tab_idx]

# Use radio buttons styled as tabs for state preservation
selected_tab = st.radio(
    "Navigation",
    options=TAB_LABELS,
    index=current_tab_idx,
    horizontal=True,
    label_visibility="collapsed",
    key="main_nav",
    on_change=on_tab_change
)

st.divider()

# ============= CHARTS TAB =============
if selected_tab == "📈 Charts":
    # Dynamic chart based on chart_mode
    if chart_mode == "Time Series":
        st.subheader("Historical Price Trends")

        with st.spinner("Loading price trends..."):
            trends = get_price_trends(filters, frequency)

        if not trends.empty:
            if frequency == 'Yearly':
                trends['period'] = trends['transaction_year'].astype(str)
                trends['quarter'] = 2  # Default for yearly
            else:
                trends['period'] = trends['transaction_year'].astype(str) + ' Q' + trends['transaction_quarter'].astype(str)
                trends['quarter'] = trends['transaction_quarter']

            # Apply conversions for display
            def apply_conversions(row):
                price = float(row['median_price_m2'])  # Convert Decimal to float
                year = int(row['transaction_year'])
                quarter = int(row['quarter']) if 'quarter' in row else 2

                # Apply FX conversion with historical rate
                if use_fx and (year, quarter) in fx_rates:
                    price = price * fx_rates[(year, quarter)]
                elif use_fx and current_fx_rate:
                    price = price * current_fx_rate

                # Apply tsubo conversion
                if use_tsubo:
                    price = convert_to_tsubo(price)

                return price

            trends['display_median'] = trends.apply(apply_conversions, axis=1)
            trends['display_avg'] = trends.apply(
                lambda row: apply_conversions(pd.Series({
                    'median_price_m2': float(row['avg_price_m2']),  # Convert Decimal to float
                    'transaction_year': row['transaction_year'],
                    'quarter': row.get('quarter', 2)
                })), axis=1
            )

            fig = go.Figure()

            unit_label = get_unit_label()

            fig.add_trace(go.Scatter(
                x=trends['period'],
                y=trends['display_median'],
                mode='lines+markers',
                name=f'Median {unit_label}',
                line=dict(color='#2E86AB', width=2)
            ))

            fig.add_trace(go.Scatter(
                x=trends['period'],
                y=trends['display_avg'],
                mode='lines',
                name=f'Average {unit_label}',
                line=dict(color='#A23B72', width=1, dash='dash')
            ))

            fig.update_layout(
                xaxis_title="Period",
                yaxis_title=f"Price ({unit_label})",
                hovermode='x unified',
                yaxis_tickformat=',',
                height=500
            )

            st.plotly_chart(fig, width="stretch")

            # Transaction volume
            fig2 = px.bar(
                trends,
                x='period',
                y='transaction_count',
                title='Transaction Volume',
                labels={'transaction_count': 'Transactions', 'period': 'Period'}
            )
            fig2.update_layout(height=300)
            st.plotly_chart(fig2, width="stretch")
        else:
            st.warning("No data available for selected filters")

    elif chart_mode == "Histogram":
        st.subheader("Price Distribution")

        with st.spinner("Loading price distribution..."):
            hist_data = get_histogram_data(filters)

        if not hist_data.empty:
            fig = px.histogram(
                hist_data,
                x='unit_price',
                nbins=50,
                title='Distribution of Price per m²',
                labels={'unit_price': 'Price per m² (¥)', 'count': 'Count'}
            )
            fig.update_layout(
                xaxis_tickformat=',',
                height=500
            )
            st.plotly_chart(fig, width="stretch")

            # Stats
            stat_col1, stat_col2, stat_col3, stat_col4 = st.columns(4)
            stat_col1.metric("Count", f"{len(hist_data):,}")
            stat_col2.metric("Median", f"¥{hist_data['unit_price'].median():,.0f}/m²")
            stat_col3.metric("Mean", f"¥{hist_data['unit_price'].mean():,.0f}/m²")
            stat_col4.metric("Std Dev", f"¥{hist_data['unit_price'].std():,.0f}")
        else:
            st.warning("No data available for selected filters")

    elif chart_mode == "Scatter (X vs Y)":
        st.subheader(f"{scatter_y} vs {scatter_x}")

        with st.spinner("Loading scatter data..."):
            scatter_data = get_scatter_data(filters)

        if not scatter_data.empty:
            # Map axis selections to column names
            x_col_map = {
                "Building Age": "building_age",
                "Area (m²)": "area_m2",
                "Year Built": "building_year",
                "Transaction Year": "transaction_year"
            }
            y_col_map = {
                "Price per m²": "unit_price",
                "Total Price": "trade_price"
            }

            x_col = x_col_map.get(scatter_x, "building_age")
            y_col = y_col_map.get(scatter_y, "unit_price")

            # Filter out invalid data
            plot_data = scatter_data.dropna(subset=[x_col, y_col])

            if x_col == "building_age":
                plot_data = plot_data[(plot_data['building_age'] >= 0) & (plot_data['building_age'] <= 60)]

            if not plot_data.empty:
                fig = px.scatter(
                    plot_data,
                    x=x_col,
                    y=y_col,
                    color='structure' if 'structure' in plot_data.columns else None,
                    opacity=0.3,
                    trendline='ols',
                    title=f'{scatter_y} vs {scatter_x}',
                    labels={x_col: scatter_x, y_col: scatter_y}
                )
                fig.update_layout(
                    yaxis_tickformat=',',
                    height=600
                )
                st.plotly_chart(fig, width="stretch")

                # Regression stats
                if len(plot_data) > 10:
                    from scipy import stats
                    valid = plot_data.dropna(subset=[x_col, y_col])
                    if len(valid) > 10:
                        slope, intercept, r_value, p_value, std_err = stats.linregress(
                            valid[x_col],
                            valid[y_col]
                        )
                        if x_col == "building_age":
                            st.info(f"**Regression:** {scatter_y} changes by ¥{slope:,.0f} per year of age (R² = {r_value**2:.3f})")
                        else:
                            st.info(f"**Regression:** R² = {r_value**2:.3f}")
            else:
                st.warning("No valid data for the selected axes")
        else:
            st.warning("No data available for selected filters")

# ============= MAP TAB =============
elif selected_tab == "🗺️ Map":
    st.subheader(f"Price Map: {selected_prefecture_name}")

    # Toggle for data mode
    map_mode = st.radio(
        "Price Data",
        options=[f"Latest Year ({year_range[1]})", f"Full Period ({year_range[0]}-{year_range[1]})"],
        index=0,  # Default to latest year
        horizontal=True,
        key="map_mode"
    )
    use_latest_only = map_mode.startswith("Latest")

    with st.spinner("Loading map data..."):
        map_data = get_map_data(filters, latest_only=use_latest_only)

    if not map_data.empty:
        # Apply conversions
        unit_label = get_unit_label()

        def convert_map_price(price):
            if price is None:
                return None
            result = float(price)  # Convert Decimal to float
            if use_fx and current_fx_rate:
                result = result * current_fx_rate
            if use_tsubo:
                result = convert_to_tsubo(result)
            return result

        map_data['display_median'] = map_data['median_price_m2'].apply(convert_map_price)
        map_data['display_avg'] = map_data['avg_price_m2'].apply(convert_map_price)

        # Create bar chart (map choropleth would need GeoJSON which is complex)
        fig = px.bar(
            map_data.head(30),
            x='name',
            y='display_median',
            color='display_median',
            color_continuous_scale='RdYlGn_r',
            title=f'Median Price by Ward/City ({unit_label}) - {map_mode}',
            labels={'display_median': unit_label, 'name': 'Ward/City'},
            text='transactions'
        )
        fig.update_layout(
            xaxis_tickangle=-45,
            yaxis_tickformat=',',
            height=500,
            showlegend=False
        )
        fig.update_traces(texttemplate='%{text:,}', textposition='outside')
        st.plotly_chart(fig, width="stretch")

        # Also show as a treemap for visual comparison
        st.subheader("Price Treemap")
        fig2 = px.treemap(
            map_data,
            path=['name'],
            values='transactions',
            color='display_median',
            color_continuous_scale='RdYlGn_r',
            title=f'Transactions by Area (color = {unit_label})'
        )
        fig2.update_layout(height=500)
        st.plotly_chart(fig2, width="stretch")

        # Data table with converted values
        st.subheader("Data by Ward/City")

        # Create display dataframe with converted values for export
        export_df = map_data[['name', 'transactions', 'display_median', 'display_avg']].copy()
        export_df.columns = ['Ward/City', 'Transactions', f'Median ({unit_label})', f'Average ({unit_label})']

        st.dataframe(
            export_df.style.format({
                'Transactions': '{:,}',
                f'Median ({unit_label})': '{:,.0f}',
                f'Average ({unit_label})': '{:,.0f}'
            }),
            width="stretch",
            height=400
        )

        # Download button for CSV with proper converted values
        csv_data = export_df.to_csv(index=False)
        st.download_button(
            label="📥 Download CSV",
            data=csv_data,
            file_name=f"price_map_{selected_prefecture_name}_{currency}_{year_range[1] if use_latest_only else f'{year_range[0]}-{year_range[1]}'}.csv",
            mime="text/csv"
        )
    else:
        st.warning("No data available for selected filters")

# ============= AGE COHORTS TAB =============
elif selected_tab == "📅 Age Cohorts":
    st.subheader("Price Trends by Building Age Cohort")
    st.caption("Track how prices evolve for apartments of different ages over time (sliding age buckets)")

    # Age cohort options
    cohort_col1, cohort_col2 = st.columns([1, 3])

    with cohort_col1:
        age_buckets = st.multiselect(
            "Age Cohorts (years)",
            options=[5, 10, 15, 20, 25, 30, 35, 40],
            default=[10, 20, 30],
            help="Select building ages to compare. Each cohort shows apartments that were X years old at the time of transaction."
        )

    if age_buckets:
        # Query for age cohort data with sliding buckets
        cohort_query = """
            SELECT
                t.transaction_year,
                t.transaction_quarter,
                (t.transaction_year - t.building_year) as building_age,
                t.unit_price
            FROM transactions t
            WHERE t.prefecture_code = %s
              AND t.unit_price IS NOT NULL
              AND t.unit_price > 0
              AND t.unit_price < 50000000
              AND t.building_year IS NOT NULL
              AND t.transaction_year BETWEEN %s AND %s
              AND t.property_type_raw = 'Pre-owned Condominiums, etc.'
        """
        cohort_params = [selected_prefecture, year_range[0], year_range[1]]

        if selected_municipality_codes:
            cohort_query += " AND t.municipality_code = ANY(%s)"
            cohort_params.append(selected_municipality_codes)

        with st.spinner("Loading cohort data..."):
            cohort_data = run_query(cohort_query, cohort_params)

        if not cohort_data.empty:
            # Create period column
            cohort_data['period'] = cohort_data['transaction_year'].astype(str) + ' Q' + cohort_data['transaction_quarter'].astype(str)

            # Assign each transaction to an age bucket (±2 years tolerance)
            def assign_bucket(age, buckets):
                for bucket in sorted(buckets):
                    if abs(age - bucket) <= 2:
                        return f"{bucket}yr"
                return None

            cohort_data['age_bucket'] = cohort_data['building_age'].apply(lambda x: assign_bucket(x, age_buckets))
            cohort_data = cohort_data[cohort_data['age_bucket'].notna()]

            if not cohort_data.empty:
                # Aggregate by period and age bucket
                agg_data = cohort_data.groupby(['transaction_year', 'transaction_quarter', 'age_bucket']).agg(
                    median_price=('unit_price', 'median'),
                    count=('unit_price', 'count')
                ).reset_index()
                agg_data['period'] = agg_data['transaction_year'].astype(str) + ' Q' + agg_data['transaction_quarter'].astype(str)

                # Filter to buckets with enough data
                agg_data = agg_data[agg_data['count'] >= 5]

                if not agg_data.empty:
                    # Apply currency/tsubo conversions
                    unit_label = get_unit_label()

                    def convert_cohort_price(row):
                        price = float(row['median_price'])
                        year = int(row['transaction_year'])
                        quarter = int(row['transaction_quarter'])

                        # Apply FX conversion with historical rate
                        if use_fx and (year, quarter) in fx_rates:
                            price = price * fx_rates[(year, quarter)]
                        elif use_fx and current_fx_rate:
                            price = price * current_fx_rate

                        # Apply tsubo conversion
                        if use_tsubo:
                            price = convert_to_tsubo(price)

                        return price

                    agg_data['display_price'] = agg_data.apply(convert_cohort_price, axis=1)

                    fig = px.line(
                        agg_data,
                        x='period',
                        y='display_price',
                        color='age_bucket',
                        title=f'Median Price ({unit_label}) by Building Age Cohort',
                        labels={'display_price': unit_label, 'period': 'Period', 'age_bucket': 'Building Age'},
                        markers=True
                    )
                    fig.update_layout(
                        yaxis_tickformat=',',
                        height=500,
                        hovermode='x unified'
                    )
                    st.plotly_chart(fig, width="stretch")

                    # Show explanation
                    st.info("""
                    **How to read this chart:**
                    Each line shows the median price for apartments that were X years old *at the time of sale*.

                    For example, the "10yr" line shows:
                    - In 2020: apartments built in 2010 (10 years old in 2020)
                    - In 2023: apartments built in 2013 (10 years old in 2023)

                    This "sliding age" approach lets you compare how the market values apartments of the same age across different time periods.
                    """)

                    # Summary table with converted prices
                    summary = agg_data.groupby('age_bucket').agg(
                        avg_median=('display_price', 'mean'),
                        total_transactions=('count', 'sum')
                    ).reset_index()
                    summary.columns = ['Age Cohort', f'Avg Median ({unit_label})', 'Total Transactions']
                    st.dataframe(
                        summary.style.format({
                            f'Avg Median ({unit_label})': '{:,.0f}',
                            'Total Transactions': '{:,}'
                        }),
                        width="stretch"
                    )
                else:
                    st.warning("Not enough data points for the selected age cohorts. Try selecting different ages or expanding the year range.")
            else:
                st.warning("No transactions found matching the selected age cohorts.")
        else:
            st.warning("No condominium data available for the selected filters.")
    else:
        st.info("👈 Select age cohorts to compare (e.g., 10yr, 20yr, 30yr)")

# ============= DISTRICT TAB =============
elif selected_tab == "📍 District":
    st.subheader("District Analysis")
    st.caption("Analyze price trends at the micro-market level by district/chome")

    micro_col1, micro_col2 = st.columns([1, 2])

    with micro_col1:
        st.markdown("##### Select Districts to Compare")

        # Get available districts
        if selected_municipality_codes:
            available_districts = get_districts(selected_municipality_codes)
        elif has_municipality_data:
            st.info("Select a Ward/City in the sidebar to see districts")
            available_districts = pd.DataFrame()
        else:
            available_districts = get_districts_by_prefecture(selected_prefecture)

        if not available_districts.empty:
            # District multiselect
            selected_micro_districts = st.multiselect(
                "Districts",
                options=available_districts['district_name'].tolist(),
                max_selections=8,
                help=TOOLTIPS["chome"] + " Select up to 8 to compare."
            )
        else:
            selected_micro_districts = None

        # Show district rankings
        st.markdown("##### Top Districts by Price")
        with st.spinner("Loading district rankings..."):
            district_rankings = get_district_rankings(
                selected_prefecture,
                selected_municipality_codes,
                filters
            )

        if not district_rankings.empty:
            def convert_ranking_price(price):
                if price is None:
                    return None
                result = float(price)
                if use_fx and current_fx_rate:
                    result = result * current_fx_rate
                if use_tsubo:
                    result = convert_to_tsubo(result)
                return result

            district_rankings['display_median'] = district_rankings['median_price_m2'].apply(convert_ranking_price)

            display_rankings = district_rankings[['district_name', 'display_median', 'transaction_count']].head(15)
            display_rankings.columns = ['District', get_unit_label(), 'Transactions']

            st.dataframe(
                display_rankings.style.format({
                    get_unit_label(): '{:,.0f}',
                    'Transactions': '{:,}'
                }),
                height=400,
                width="stretch"
            )

    with micro_col2:
        unit_label = get_unit_label()

        if selected_micro_districts and len(selected_micro_districts) > 0:
            st.markdown(f"##### Price Trends by District ({unit_label})")

            with st.spinner("Loading district price trends..."):
                district_trends = get_district_price_trends(
                    selected_prefecture,
                    selected_municipality_codes,
                    selected_micro_districts,
                    filters,
                    frequency
                )

            if not district_trends.empty:
                # Create period column
                if frequency == 'Yearly':
                    district_trends['period'] = district_trends['transaction_year'].astype(str)
                    district_trends['quarter'] = 2
                else:
                    district_trends['period'] = district_trends['transaction_year'].astype(str) + ' Q' + district_trends['transaction_quarter'].astype(str)
                    district_trends['quarter'] = district_trends['transaction_quarter']

                # Apply conversions
                def apply_district_conversions(row):
                    price = float(row['median_price_m2'])
                    year = int(row['transaction_year'])
                    quarter = int(row['quarter'])

                    if use_fx and (year, quarter) in fx_rates:
                        price = price * fx_rates[(year, quarter)]
                    elif use_fx and current_fx_rate:
                        price = price * current_fx_rate

                    if use_tsubo:
                        price = convert_to_tsubo(price)

                    return price

                district_trends['display_median'] = district_trends.apply(apply_district_conversions, axis=1)

                # Line chart
                fig = px.line(
                    district_trends,
                    x='period',
                    y='display_median',
                    color='district_name',
                    markers=True,
                    title=f'Median Price Trends by District',
                    labels={'display_median': unit_label, 'period': 'Period', 'district_name': 'District'}
                )
                fig.update_layout(
                    yaxis_tickformat=',',
                    height=450,
                    hovermode='x unified'
                )
                st.plotly_chart(fig, width="stretch")

                # Transaction volume chart
                st.markdown("##### Transaction Volume by District")
                fig2 = px.bar(
                    district_trends,
                    x='period',
                    y='transaction_count',
                    color='district_name',
                    title='Transaction Volume',
                    labels={'transaction_count': 'Transactions', 'period': 'Period', 'district_name': 'District'}
                )
                fig2.update_layout(height=300, barmode='group')
                st.plotly_chart(fig2, width="stretch")

                # Summary stats
                st.markdown("##### Summary Statistics")
                summary = district_trends.groupby('district_name').agg(
                    avg_median=('display_median', 'mean'),
                    total_transactions=('transaction_count', 'sum')
                ).reset_index()
                summary.columns = ['District', f'Avg Median ({unit_label})', 'Total Transactions']
                st.dataframe(
                    summary.style.format({
                        f'Avg Median ({unit_label})': '{:,.0f}',
                        'Total Transactions': '{:,}'
                    }),
                    width="stretch"
                )
            else:
                st.warning("No data available for the selected districts. Try different districts or adjust filters.")
        else:
            st.info("👈 Select districts from the list to see price trends")

            # Show overall district comparison as a bar chart
            if 'district_rankings' in dir() and not district_rankings.empty:
                st.markdown(f"##### District Price Comparison ({unit_label})")
                fig = px.bar(
                    district_rankings.head(20),
                    x='district_name',
                    y='display_median',
                    color='display_median',
                    color_continuous_scale='RdYlGn_r',
                    title='Top 20 Districts by Median Price',
                    labels={'display_median': unit_label, 'district_name': 'District'}
                )
                fig.update_layout(
                    xaxis_tickangle=-45,
                    yaxis_tickformat=',',
                    height=450,
                    showlegend=False
                )
                st.plotly_chart(fig, width="stretch")

# ============= VALUATION TAB =============
elif selected_tab == "💰 Valuation":
    st.subheader("Property Valuation")

    # Toggle between valuation modes
    valuation_mode = st.radio(
        "Mode",
        options=["Estimate Value", "Check Listing Price", "Track Depreciation"],
        horizontal=True,
        key="valuation_mode"
    )

    val_col1, val_col2 = st.columns(2)

    with val_col1:
        st.markdown("##### Property Details")

        val_municipality = st.selectbox(
            "Ward / City",
            options=[""] + list(municipality_options.keys()),
            key="val_municipality"
        )

        # Show district only for Estimate Value and Check Listing modes
        if valuation_mode != "Track Depreciation":
            val_district = st.text_input(
                "District / Address (optional)",
                placeholder="e.g. Roppongi 1-chome",
                key="val_district"
            )

            val_property_type = st.selectbox(
                "Property Type",
                options=property_types['property_type_raw'].tolist(),
                key="val_property_type"
            )
        else:
            val_district = None
            val_property_type = "Pre-owned Condominiums, etc."

        # Show listing price input only in "Check Listing Price" mode
        if valuation_mode == "Check Listing Price":
            val_listing_price = st.number_input(
                "Listing Price (¥)",
                min_value=1000000,
                max_value=2000000000,
                value=50000000,
                step=1000000,
                format="%d",
                key="val_listing_price"
            )
        else:
            val_listing_price = None

        # Show purchase info for depreciation mode
        if valuation_mode == "Track Depreciation":
            dep_purchase_year = st.number_input(
                "Year Purchased",
                min_value=2005,
                max_value=max_year,
                value=2015,
                step=1,
                key="dep_purchase_year"
            )

            dep_purchase_price = st.number_input(
                "Purchase Price (¥)",
                min_value=1000000,
                max_value=2000000000,
                value=50000000,
                step=1000000,
                format="%d",
                key="dep_purchase_price"
            )

        val_area = st.number_input(
            "Area (m²)",
            min_value=10.0,
            max_value=500.0,
            value=60.0,
            step=5.0,
            key="val_area"
        )

        val_building_year = st.number_input(
            "Year Built",
            min_value=1960,
            max_value=2025,
            value=2010,
            step=1,
            key="val_building_year"
        )

        if valuation_mode != "Track Depreciation":
            val_floor_plan = st.selectbox(
                "Layout (optional)",
                options=["", "1K", "1DK", "1LDK", "2K", "2DK", "2LDK", "3K", "3DK", "3LDK", "4LDK+"],
                key="val_floor_plan"
            )
        else:
            val_floor_plan = None

        if valuation_mode == "Estimate Value":
            action_button = st.button("💰 Estimate Value", type="primary", width="stretch")
        elif valuation_mode == "Check Listing Price":
            action_button = st.button("🔍 Check Listing", type="primary", width="stretch")
        else:
            action_button = st.button("📉 Calculate Depreciation", type="primary", width="stretch")

    with val_col2:
        if valuation_mode == "Estimate Value":
            st.markdown("##### Estimated Value")
        elif valuation_mode == "Check Listing Price":
            st.markdown("##### Market Comparison")
        else:
            st.markdown("##### Estimated Current Value")

        if action_button and val_municipality:
            current_year = year_range[1]
            building_age = current_year - val_building_year
            val_muni_code = municipality_options[val_municipality]

            # Track Depreciation mode has different logic
            if valuation_mode == "Track Depreciation":
                # Get historical prices for similar properties
                dep_query = """
                    SELECT
                        t.transaction_year,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.unit_price)::INTEGER as median_price_m2,
                        COUNT(*) as count
                    FROM transactions t
                    WHERE t.municipality_code = %s
                      AND t.property_type_raw = 'Pre-owned Condominiums, etc.'
                      AND t.unit_price IS NOT NULL
                      AND t.unit_price > 0
                      AND t.unit_price < 50000000
                      AND t.area_m2 BETWEEN %s AND %s
                      AND t.building_year IS NOT NULL
                    GROUP BY t.transaction_year
                    HAVING COUNT(*) >= 5
                    ORDER BY t.transaction_year
                """
                dep_params = [val_muni_code, val_area * 0.7, val_area * 1.3]

                with st.spinner("Calculating depreciation..."):
                    dep_data = run_query(dep_query, dep_params)

                if not dep_data.empty and len(dep_data) >= 2:
                    # Get price at purchase year and current year
                    purchase_year_data = dep_data[dep_data['transaction_year'] == dep_purchase_year]
                    current_year_data = dep_data[dep_data['transaction_year'] == max_year]

                    if not purchase_year_data.empty and not current_year_data.empty:
                        purchase_price_m2 = float(purchase_year_data['median_price_m2'].iloc[0])
                        current_price_m2 = float(current_year_data['median_price_m2'].iloc[0])

                        # Calculate market appreciation/depreciation
                        market_change = (current_price_m2 - purchase_price_m2) / purchase_price_m2

                        # Estimate current value based on market change
                        estimated_current = dep_purchase_price * (1 + market_change)
                        value_change = estimated_current - dep_purchase_price
                        value_change_pct = market_change * 100

                        # Building age effect
                        years_held = max_year - dep_purchase_year
                        age_at_purchase = dep_purchase_year - val_building_year
                        age_now = max_year - val_building_year

                        if value_change >= 0:
                            st.success(f"### ¥{estimated_current:,.0f}")
                            st.caption(f"Estimated appreciation: ¥{value_change:+,.0f} ({value_change_pct:+.1f}%)")
                        else:
                            st.error(f"### ¥{estimated_current:,.0f}")
                            st.caption(f"Estimated depreciation: ¥{value_change:+,.0f} ({value_change_pct:+.1f}%)")

                        st.markdown(f"""
                        | Metric | Value |
                        |--------|-------|
                        | **Purchase Price** | ¥{dep_purchase_price:,.0f} |
                        | **Estimated Current Value** | ¥{estimated_current:,.0f} |
                        | **Change** | ¥{value_change:+,.0f} ({value_change_pct:+.1f}%) |
                        | **Years Held** | {years_held} years |
                        | **Age at Purchase** | {age_at_purchase} years old |
                        | **Age Now** | {age_now} years old |
                        | **Market Price/m² (at purchase)** | ¥{purchase_price_m2:,.0f} |
                        | **Market Price/m² (now)** | ¥{current_price_m2:,.0f} |
                        """)

                        # Show historical trend
                        st.divider()
                        st.markdown("##### Market Trend for Similar Properties")

                        fig = px.line(
                            dep_data,
                            x='transaction_year',
                            y='median_price_m2',
                            markers=True,
                            title='Median Price/m² Over Time',
                            labels={'median_price_m2': '¥/m²', 'transaction_year': 'Year'}
                        )

                        # Add markers for purchase and current
                        fig.add_vline(x=dep_purchase_year, line_dash="dash", line_color="green",
                                     annotation_text="Purchased")
                        fig.add_vline(x=max_year, line_dash="dash", line_color="blue",
                                     annotation_text="Now")

                        fig.update_layout(
                            yaxis_tickformat=',',
                            height=350
                        )
                        st.plotly_chart(fig, width="stretch")

                    else:
                        st.warning("Not enough data for the purchase year or current year. Try adjusting the parameters.")
                else:
                    st.warning("Not enough comparable transactions found. Try selecting a different ward or adjusting the area.")

            else:
                # Estimate Value and Check Listing modes
                comp_query = """
                    SELECT
                        t.trade_price,
                        t.unit_price,
                        t.area_m2,
                        t.building_year,
                        t.transaction_year - t.building_year as building_age,
                        t.floor_plan,
                        t.district_name,
                        t.transaction_year,
                        t.transaction_quarter
                    FROM transactions t
                    WHERE t.municipality_code = %s
                      AND t.property_type_raw = %s
                      AND t.unit_price IS NOT NULL
                      AND t.unit_price > 0
                      AND t.unit_price < 50000000
                      AND t.transaction_year >= %s
                      AND t.area_m2 BETWEEN %s AND %s
                """
                comp_params = [
                    val_muni_code,
                    val_property_type,
                    current_year - 3,
                    val_area * 0.7,
                    val_area * 1.3
                ]

                if val_building_year > 0:
                    comp_query += " AND t.building_year IS NOT NULL"
                    comp_query += " AND (t.transaction_year - t.building_year) BETWEEN %s AND %s"
                    comp_params.extend([max(0, building_age - 10), building_age + 10])

                if val_floor_plan:
                    comp_query += " AND t.floor_plan = %s"
                    comp_params.append(val_floor_plan)

                if val_district:
                    comp_query += " AND t.district_name ILIKE %s"
                    comp_params.append(f"%{val_district}%")

                comp_query += " ORDER BY t.transaction_year DESC, t.transaction_quarter DESC LIMIT 100"

                with st.spinner("Finding comparable properties..."):
                    comparables = run_query(comp_query, comp_params)

                if not comparables.empty and len(comparables) >= 3:
                    median_unit_price = comparables['unit_price'].median()
                    std_unit_price = comparables['unit_price'].std()

                    if valuation_mode == "Estimate Value":
                        # Valuation mode
                        estimated_price_median = median_unit_price * val_area
                        low_estimate = (median_unit_price - std_unit_price) * val_area
                        high_estimate = (median_unit_price + std_unit_price) * val_area

                        st.metric(
                            "Estimated Market Value",
                            f"¥{estimated_price_median:,.0f}",
                            help="Based on median price per m² of comparable transactions"
                        )

                        st.markdown(f"""
                        | Metric | Value |
                        |--------|-------|
                        | **Low Estimate** | ¥{max(0, low_estimate):,.0f} |
                        | **Median Estimate** | ¥{estimated_price_median:,.0f} |
                        | **High Estimate** | ¥{high_estimate:,.0f} |
                        | **Price per m² (Median)** | ¥{median_unit_price:,.0f} |
                        | **Comparable Transactions** | {len(comparables)} |
                        """)

                    else:
                        # Check Listing mode
                        listing_price_per_m2 = val_listing_price / val_area
                        fair_value = median_unit_price * val_area
                        price_diff = val_listing_price - fair_value
                        price_diff_pct = (price_diff / fair_value) * 100
                        percentile = (comparables['unit_price'] < listing_price_per_m2).mean() * 100

                        if price_diff_pct < -15:
                            verdict = "🟢 **UNDERPRICED**"
                            verdict_detail = "This listing appears significantly below market value"
                        elif price_diff_pct < -5:
                            verdict = "🟢 **GOOD VALUE**"
                            verdict_detail = "This listing appears below market average"
                        elif price_diff_pct < 5:
                            verdict = "🟡 **FAIR PRICE**"
                            verdict_detail = "This listing is priced around market value"
                        elif price_diff_pct < 15:
                            verdict = "🟠 **ABOVE MARKET**"
                            verdict_detail = "This listing is priced above market average"
                        else:
                            verdict = "🔴 **OVERPRICED**"
                            verdict_detail = "This listing appears significantly above market value"

                        st.markdown(f"### {verdict}")
                        st.caption(verdict_detail)

                        st.markdown(f"""
                        | Metric | Value |
                        |--------|-------|
                        | **Listing Price** | ¥{val_listing_price:,.0f} |
                        | **Listing ¥/m²** | ¥{listing_price_per_m2:,.0f} |
                        | **Market Median ¥/m²** | ¥{median_unit_price:,.0f} |
                        | **Fair Value Estimate** | ¥{fair_value:,.0f} |
                        | **Difference** | ¥{price_diff:+,.0f} ({price_diff_pct:+.1f}%) |
                        | **Price Percentile** | {percentile:.0f}% |
                        | **Comparables Found** | {len(comparables)} |
                        """)

                    st.divider()
                    st.markdown("##### Recent Comparable Sales")

                    display_cols = ['transaction_year', 'transaction_quarter', 'district_name',
                                   'area_m2', 'building_age', 'unit_price', 'trade_price']
                    display_data = comparables[[c for c in display_cols if c in comparables.columns]].head(10)

                    st.dataframe(
                        display_data.style.format({
                            'trade_price': '¥{:,.0f}',
                            'unit_price': '¥{:,.0f}/m²',
                            'area_m2': '{:.1f}m²',
                            'building_age': '{:.0f}yr'
                        }),
                        width="stretch",
                        height=350
                    )

                    # Price distribution
                    fig = px.histogram(
                        comparables,
                        x='unit_price',
                        nbins=20,
                        title='Price Distribution of Comparable Properties',
                        labels={'unit_price': 'Price per m² (¥)'}
                    )
                    fig.add_vline(x=median_unit_price, line_dash="dash", line_color="green",
                                 annotation_text=f"Median: ¥{median_unit_price:,.0f}")
                    if valuation_mode == "Check Listing Price":
                        fig.add_vline(x=listing_price_per_m2, line_dash="solid", line_color="red", line_width=3,
                                     annotation_text=f"Listing: ¥{listing_price_per_m2:,.0f}")
                    fig.update_layout(height=300, xaxis_tickformat=',')
                    st.plotly_chart(fig, width="stretch")

                    # PDF Export button
                    st.divider()

                    # Prepare valuation data for PDF
                    valuation_pdf_data = {
                        'location': val_municipality,
                        'district': val_district or 'Not specified',
                        'property_type': val_property_type,
                        'area': val_area,
                        'building_year': val_building_year,
                        'building_age': building_age,
                        'floor_plan': val_floor_plan or 'Not specified',
                        'median_price_m2': median_unit_price,
                        'comparable_count': len(comparables),
                    }

                    if valuation_mode == "Estimate Value":
                        valuation_pdf_data.update({
                            'estimated_value': estimated_price_median,
                            'low_estimate': max(0, low_estimate),
                            'high_estimate': high_estimate,
                            'verdict': 'Estimated Market Value',
                            'verdict_class': 'fair',
                        })
                    else:  # Check Listing Price
                        valuation_pdf_data.update({
                            'estimated_value': fair_value,
                            'low_estimate': (median_unit_price - std_unit_price) * val_area,
                            'high_estimate': (median_unit_price + std_unit_price) * val_area,
                            'listing_price': val_listing_price,
                            'listing_price_m2': listing_price_per_m2,
                            'price_diff': price_diff,
                            'price_diff_pct': price_diff_pct,
                            'percentile': percentile,
                            'verdict': verdict.replace('🟢 ', '').replace('🟡 ', '').replace('🟠 ', '').replace('🔴 ', ''),
                            'verdict_class': 'good' if price_diff_pct < -5 else ('high' if price_diff_pct > 15 else 'fair'),
                        })

                    pdf_html = generate_valuation_pdf(valuation_pdf_data)

                    st.download_button(
                        label="📄 Download Valuation Report (HTML)",
                        data=pdf_html,
                        file_name=f"valuation_report_{val_municipality}_{datetime.now().strftime('%Y%m%d')}.html",
                        mime="text/html",
                        help="Download a printable HTML report. Open in browser and use Print → Save as PDF for a PDF version."
                    )

                else:
                    st.warning(f"Only {len(comparables)} comparable transactions found. Try adjusting criteria.")
                    st.info("Tips: Remove floor plan filter, widen the district search, or increase the area range.")

        elif action_button:
            st.warning("Please select a Ward/City")
        else:
            if valuation_mode == "Estimate Value":
                st.info("👈 Enter property details and click **Estimate Value**")
            elif valuation_mode == "Check Listing Price":
                st.info("👈 Enter listing details and click **Check Listing**")
            else:
                st.info("👈 Enter your property details and click **Calculate Depreciation**")

# ============= RAW DATA TAB =============
elif selected_tab == "📋 Raw Data":
    st.subheader("Sample Transactions")

    sample_query, sample_params = build_query(
        """t.transaction_year, t.transaction_quarter, t.municipality_code,
           t.district_name, t.property_type_raw, t.trade_price, t.unit_price,
           t.area_m2, t.building_year, t.floor_plan, t.structure""",
        filters,
        order_by="t.transaction_year DESC, t.transaction_quarter DESC",
        limit=100
    )
    with st.spinner("Loading transactions..."):
        sample_data = run_query(sample_query, sample_params)

    if not sample_data.empty:
        unit_label = get_unit_label()

        # Apply currency/tsubo conversions
        def convert_raw_price(row, price_col):
            price = row[price_col]
            if price is None:
                return None
            price = float(price)
            year = int(row['transaction_year'])
            quarter = int(row['transaction_quarter'])

            # Apply FX conversion with historical rate
            if use_fx and (year, quarter) in fx_rates:
                price = price * fx_rates[(year, quarter)]
            elif use_fx and current_fx_rate:
                price = price * current_fx_rate

            return price

        def convert_unit_price(row):
            price = convert_raw_price(row, 'unit_price')
            if price is None:
                return None
            # Apply tsubo conversion
            if use_tsubo:
                price = convert_to_tsubo(price)
            return price

        sample_data['display_trade_price'] = sample_data.apply(lambda r: convert_raw_price(r, 'trade_price'), axis=1)
        sample_data['display_unit_price'] = sample_data.apply(convert_unit_price, axis=1)

        # Create display dataframe
        display_cols = ['transaction_year', 'transaction_quarter', 'district_name',
                       'property_type_raw', 'display_trade_price', 'display_unit_price',
                       'area_m2', 'building_year', 'floor_plan', 'structure']
        display_df = sample_data[[c for c in display_cols if c in sample_data.columns]].copy()

        # Rename columns for display
        currency_symbol = {'JPY': '¥', 'USD': '$', 'EUR': '€', 'GBP': '£'}.get(currency, '¥')
        display_df.columns = ['Year', 'Quarter', 'District', 'Property Type',
                             f'Total Price ({currency_symbol})', f'Price ({unit_label})',
                             'Area (m²)', 'Year Built', 'Layout', 'Structure']

        st.dataframe(
            display_df.style.format({
                f'Total Price ({currency_symbol})': '{:,.0f}',
                f'Price ({unit_label})': '{:,.0f}',
                'Area (m²)': '{:.1f}'
            }),
            width="stretch",
            height=500
        )

        # Download button
        csv_data = display_df.to_csv(index=False)
        st.download_button(
            label="📥 Download CSV",
            data=csv_data,
            file_name=f"transactions_{selected_prefecture_name}_{currency}.csv",
            mime="text/csv"
        )
    else:
        st.warning("No data available for selected filters")

# Footer
st.divider()

footer_col1, footer_col2 = st.columns([3, 1])
with footer_col1:
    st.caption("このサービスは、国土交通省不動産情報ライブラリのAPI機能を使用していますが、提供情報の最新性、正確性、完全性等が保証されたものではありません。")
with footer_col2:
    st.markdown(f"[📝 Report Issues]({GITHUB_REPO_URL}/issues) | [⭐ GitHub]({GITHUB_REPO_URL})")
