"""
MLIT Data Ingestion Pipeline
=============================
Fetches transaction data from MLIT API and loads into PostgreSQL.

Usage:
    python ingest_data.py --year 2023 --prefecture 13
    python ingest_data.py --full --prefecture 13
    python ingest_data.py --incremental
    python ingest_data.py --refresh-fx-only
"""

import argparse
import hashlib
import os
import sys
from datetime import datetime, date
from typing import List, Dict, Any, Optional
import time

import psycopg2
from psycopg2.extras import execute_values
import requests


# =============================================================================
# MLIT API CLIENT
# =============================================================================

MLIT_API_BASE = "https://www.reinfolib.mlit.go.jp/ex-api/external"

PREFECTURE_CODES = {
    "Hokkaido": "01", "北海道": "01",
    "Aomori": "02", "青森県": "02",
    "Iwate": "03", "岩手県": "03",
    "Miyagi": "04", "宮城県": "04",
    "Akita": "05", "秋田県": "05",
    "Yamagata": "06", "山形県": "06",
    "Fukushima": "07", "福島県": "07",
    "Ibaraki": "08", "茨城県": "08",
    "Tochigi": "09", "栃木県": "09",
    "Gunma": "10", "群馬県": "10",
    "Saitama": "11", "埼玉県": "11",
    "Chiba": "12", "千葉県": "12",
    "Tokyo": "13", "東京都": "13",
    "Kanagawa": "14", "神奈川県": "14",
    "Niigata": "15", "新潟県": "15",
    "Toyama": "16", "富山県": "16",
    "Ishikawa": "17", "石川県": "17",
    "Fukui": "18", "福井県": "18",
    "Yamanashi": "19", "山梨県": "19",
    "Nagano": "20", "長野県": "20",
    "Gifu": "21", "岐阜県": "21",
    "Shizuoka": "22", "静岡県": "22",
    "Aichi": "23", "愛知県": "23",
    "Mie": "24", "三重県": "24",
    "Shiga": "25", "滋賀県": "25",
    "Kyoto": "26", "京都府": "26",
    "Osaka": "27", "大阪府": "27",
    "Hyogo": "28", "兵庫県": "28",
    "Nara": "29", "奈良県": "29",
    "Wakayama": "30", "和歌山県": "30",
    "Tottori": "31", "鳥取県": "31",
    "Shimane": "32", "島根県": "32",
    "Okayama": "33", "岡山県": "33",
    "Hiroshima": "34", "広島県": "34",
    "Yamaguchi": "35", "山口県": "35",
    "Tokushima": "36", "徳島県": "36",
    "Kagawa": "37", "香川県": "37",
    "Ehime": "38", "愛媛県": "38",
    "Kochi": "39", "高知県": "39",
    "Fukuoka": "40", "福岡県": "40",
    "Saga": "41", "佐賀県": "41",
    "Nagasaki": "42", "長崎県": "42",
    "Kumamoto": "43", "熊本県": "43",
    "Oita": "44", "大分県": "44",
    "Miyazaki": "45", "宮崎県": "45",
    "Kagoshima": "46", "鹿児島県": "46",
    "Okinawa": "47", "沖縄県": "47",
}


class MLITApiClient:
    """Client for MLIT Real Estate Information Library API."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({"Ocp-Apim-Subscription-Key": api_key})

    def get_transactions(
        self,
        year: int,
        area: str,
        quarter: Optional[int] = None,
        city: Optional[str] = None,
        station: Optional[str] = None,
        price_classification: str = "01",
        language: str = "en"
    ) -> List[Dict[str, Any]]:
        """
        Fetch real estate transactions from MLIT API.

        Args:
            year: Transaction year (e.g., 2023)
            area: Prefecture code (e.g., "13" for Tokyo)
            quarter: Quarter 1-4 (optional, fetches all if not specified)
            city: Municipality code (e.g., "13103" for Minato-ku)
            station: Station code
            price_classification: "01" for transaction prices, "02" for contract prices
            language: "en" or "ja"

        Returns:
            List of transaction records
        """
        params = {
            "year": year,
            "area": area,
            "priceClassification": price_classification,
            "language": language,
        }

        if quarter:
            params["quarter"] = quarter
        if city:
            params["city"] = city
        if station:
            params["station"] = station

        try:
            resp = self.session.get(
                f"{MLIT_API_BASE}/XIT001",
                params=params,
                timeout=30
            )
            if resp.status_code in (400, 404):
                # Data not available for this period (400 = invalid period, 404 = not yet published)
                return None
            resp.raise_for_status()
            data = resp.json()
            return data.get("data", [])
        except requests.RequestException as e:
            raise Exception(f"API request failed: {e}")

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://localhost/mlit_realestate"
)

API_KEY = os.environ.get("MLIT_API_KEY", "YOUR_API_KEY_HERE")

REQUESTS_PER_SECOND = 2
MIN_REQUEST_INTERVAL = 1.0 / REQUESTS_PER_SECOND

PROPERTY_TYPE_MAP = {
    "Pre-owned Condominiums": 1,
    "Pre-owned Condominiums, etc.": 1,
    "Residential Land": 2,
    "Residential Land(Land Only)": 2,
    "Residential Land and Building": 3,
    "Residential Land(Land and Building)": 3,
    "Agricultural Land": 4,
    "Forest Land": 5,
    "Pre-owned House": 6,
    "Office": 7,
    "Shop": 8,
    "Warehouse": 9,
    "Factory": 10,
}


def parse_building_year(year_str: str) -> Optional[int]:
    if not year_str:
        return None
    try:
        year = int(year_str)
        if 1900 <= year <= 2100:
            return year
    except (ValueError, TypeError):
        pass
    year_str = str(year_str)
    if "令和" in year_str or "Reiwa" in year_str:
        try:
            num = int(''.join(filter(str.isdigit, year_str)))
            return 2018 + num
        except ValueError:
            pass
    elif "平成" in year_str or "Heisei" in year_str:
        try:
            num = int(''.join(filter(str.isdigit, year_str)))
            return 1988 + num
        except ValueError:
            pass
    elif "昭和" in year_str or "Showa" in year_str:
        try:
            num = int(''.join(filter(str.isdigit, year_str)))
            return 1925 + num
        except ValueError:
            pass
    return None


def parse_numeric(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        cleaned = str(value).replace(",", "").strip()
        if cleaned:
            return float(cleaned)
    except (ValueError, TypeError):
        pass
    return None


def generate_record_hash(record: Dict[str, Any]) -> str:
    key_fields = [
        record.get("MunicipalityCode"),
        record.get("DistrictName"),
        record.get("TradePrice"),
        record.get("Area"),
        record.get("Period"),
        record.get("BuildingYear"),
        record.get("Type"),
    ]
    key_string = "|".join(str(f) for f in key_fields)
    return hashlib.sha256(key_string.encode()).hexdigest()[:32]


def transform_record(
    record: Dict[str, Any],
    prefecture_code: str,
    year: int,
    quarter: Optional[int] = None
) -> Dict[str, Any]:
    source_hash = generate_record_hash(record)

    muni_code = record.get("MunicipalityCode", "")
    if muni_code and len(muni_code) == 5:
        municipality_code = muni_code
    else:
        municipality_code = None

    property_type_raw = record.get("Type", "")
    property_type_id = PROPERTY_TYPE_MAP.get(property_type_raw)

    trade_price = parse_numeric(record.get("TradePrice"))
    unit_price = parse_numeric(record.get("UnitPrice"))
    area_m2 = parse_numeric(record.get("Area"))
    total_floor_area = parse_numeric(record.get("TotalFloorArea"))
    frontage = parse_numeric(record.get("Frontage"))
    road_width = parse_numeric(record.get("Breadth"))
    coverage_ratio = parse_numeric(record.get("CoverageRatio"))
    floor_area_ratio = parse_numeric(record.get("FloorAreaRatio"))
    building_year = parse_building_year(record.get("BuildingYear"))

    return {
        "source_hash": source_hash,
        "price_classification": record.get("PriceCategory", "01")[:2] if record.get("PriceCategory") else "01",
        "prefecture_code": prefecture_code,
        "municipality_code": municipality_code,
        "municipality_name": record.get("Municipality", ""),
        "district_name": record.get("DistrictName"),
        "property_type_id": property_type_id,
        "property_type_raw": property_type_raw,
        "trade_price": int(trade_price) if trade_price else None,
        "unit_price": int(unit_price) if unit_price else None,
        "area_m2": area_m2,
        "total_floor_area_m2": total_floor_area,
        "floor_plan": record.get("FloorPlan"),
        "building_year": building_year,
        "structure": record.get("Structure"),
        "land_shape": record.get("LandShape"),
        "frontage_m": frontage,
        "road_direction": record.get("Direction"),
        "road_type": record.get("Classification"),
        "road_width_m": road_width,
        "city_planning": record.get("CityPlanning"),
        "coverage_ratio": int(coverage_ratio) if coverage_ratio else None,
        "floor_area_ratio": int(floor_area_ratio) if floor_area_ratio else None,
        "transaction_year": year,
        "transaction_quarter": quarter,
        "transaction_period": record.get("Period"),
        "renovation": record.get("Renovation"),
        "remarks": record.get("Remarks"),
    }


def get_db_connection():
    return psycopg2.connect(DATABASE_URL)


def ensure_prefecture_exists(conn, code: str, name_en: str):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO prefectures (code, name_ja, name_en)
            VALUES (%s, %s, %s)
            ON CONFLICT (code) DO NOTHING
        """, (code, name_en, name_en))
    conn.commit()


def ensure_municipality_exists(conn, code: str, prefecture_code: str, name: str):
    if not code or len(code) != 5:
        return
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO municipalities (code, prefecture_code, name_ja, name_en)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (code) DO NOTHING
        """, (code, prefecture_code, name, name))
    conn.commit()


def insert_transactions(conn, records: List[Dict[str, Any]], prefecture_code: str) -> int:
    if not records:
        return 0

    # First, ensure all municipalities exist
    seen_municipalities = set()
    for record in records:
        muni_code = record.get("municipality_code")
        muni_name = record.get("municipality_name", "")
        if muni_code and muni_code not in seen_municipalities:
            ensure_municipality_exists(conn, muni_code, prefecture_code, muni_name)
            seen_municipalities.add(muni_code)

    columns = [
        "source_hash", "price_classification", "prefecture_code",
        "municipality_code", "district_name", "property_type_id",
        "property_type_raw", "trade_price", "unit_price", "area_m2",
        "total_floor_area_m2", "floor_plan", "building_year", "structure",
        "land_shape", "frontage_m", "road_direction", "road_type",
        "road_width_m", "city_planning", "coverage_ratio", "floor_area_ratio",
        "transaction_year", "transaction_quarter", "transaction_period",
        "renovation", "remarks"
    ]

    values = [
        tuple(record.get(col) for col in columns)
        for record in records
    ]

    insert_sql = f"""
        INSERT INTO transactions ({', '.join(columns)})
        VALUES %s
        ON CONFLICT (source_hash) DO NOTHING
    """

    with conn.cursor() as cur:
        execute_values(cur, insert_sql, values, page_size=1000)
        inserted = cur.rowcount

    conn.commit()
    return inserted


def ingest_prefecture_year(
    client: MLITApiClient,
    conn,
    prefecture_code: str,
    year: int,
    quarters: Optional[List[int]] = None
) -> int:
    total_inserted = 0

    pref_name = next(
        (k for k, v in PREFECTURE_CODES.items() if v == prefecture_code and k.isascii()),
        f"Prefecture {prefecture_code}"
    )
    ensure_prefecture_exists(conn, prefecture_code, pref_name)

    quarters_to_fetch = quarters or [1, 2, 3, 4]

    for quarter in quarters_to_fetch:
        print(f"    Q{quarter}...", end=" ", flush=True)

        try:
            transactions = client.get_transactions(
                year=year,
                area=prefecture_code,
                quarter=quarter,
                language="en"
            )

            if transactions is None:
                print("not yet available")
            elif transactions:
                records = [
                    transform_record(t, prefecture_code, year, quarter)
                    for t in transactions
                ]
                inserted = insert_transactions(conn, records, prefecture_code)
                total_inserted += inserted
                print(f"{len(transactions)} fetched, {inserted} new")
            else:
                print("0 records")

        except Exception as e:
            print(f"Error: {e}")

        time.sleep(MIN_REQUEST_INTERVAL)

    return total_inserted


def ingest_full_history(
    client: MLITApiClient,
    conn,
    start_year: int = 2005,
    prefectures: Optional[List[str]] = None
):
    current_year = datetime.now().year
    years = range(start_year, current_year + 1)

    if prefectures is None:
        prefectures = sorted(set(PREFECTURE_CODES.values()))

    total = 0

    for prefecture_code in prefectures:
        pref_name = next(
            (k for k, v in PREFECTURE_CODES.items() if v == prefecture_code and k.isascii()),
            prefecture_code
        )
        print(f"\n{'='*60}")
        print(f"Prefecture: {pref_name} ({prefecture_code})")
        print('='*60)

        for year in years:
            print(f"\n  Year {year}:")
            inserted = ingest_prefecture_year(client, conn, prefecture_code, year)
            total += inserted

    print(f"\n{'='*60}")
    print(f"COMPLETE: {total:,} total records inserted")
    print('='*60)


def find_latest_available_quarter(client: MLITApiClient) -> tuple:
    """Find the latest quarter with available data by probing Tokyo."""
    now = datetime.now()
    year = now.year
    quarter = ((now.month - 1) // 3)
    if quarter == 0:
        quarter = 4
        year -= 1

    # Try up to 4 quarters back to find available data
    for _ in range(4):
        print(f"  Checking {year} Q{quarter}...", end=" ", flush=True)
        result = client.get_transactions(year=year, area="13", quarter=quarter, language="en")
        if result is not None:
            print("available")
            return (year, quarter)
        print("not available")
        quarter -= 1
        if quarter == 0:
            quarter = 4
            year -= 1
        time.sleep(0.5)

    return (None, None)


def ingest_incremental(client: MLITApiClient, conn):
    print("Finding latest available quarter...")
    year, quarter = find_latest_available_quarter(client)

    if year is None:
        print("No recent data available. Try again later.")
        return

    print(f"\nIncremental update: {year} Q{quarter}")

    prefectures = sorted(set(PREFECTURE_CODES.values()))
    total = 0

    for prefecture_code in prefectures:
        pref_name = next(
            (k for k, v in PREFECTURE_CODES.items() if v == prefecture_code and k.isascii()),
            prefecture_code
        )
        print(f"\n{pref_name}:", end=" ")

        inserted = ingest_prefecture_year(
            client, conn, prefecture_code, year, quarters=[quarter]
        )
        total += inserted

    print(f"\n\nTotal inserted: {total:,}")

    # Also refresh FX rates
    print("\nRefreshing FX rates...")
    refresh_fx_rates(conn)


# =============================================================================
# FX RATE REFRESH
# =============================================================================

FX_API_BASE = "https://api.frankfurter.app"
FX_CURRENCIES = ["USD", "EUR", "GBP"]


def fetch_fx_rate(rate_date: date, currency: str, max_retries: int = 3) -> Optional[float]:
    """Fetch FX rate for a specific date and currency from Frankfurter API."""
    for attempt in range(max_retries):
        try:
            resp = requests.get(
                f"{FX_API_BASE}/{rate_date.isoformat()}",
                params={"from": "JPY", "to": currency},
                timeout=10
            )

            if resp.status_code == 200:
                data = resp.json()
                if 'rates' in data and currency in data['rates']:
                    return data['rates'][currency]
            elif resp.status_code == 404:
                # Date not available (weekend/holiday) - try previous day
                from datetime import timedelta
                alt_date = rate_date - timedelta(days=1)
                return fetch_fx_rate(alt_date, currency, max_retries=1)

        except requests.RequestException as e:
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
            print(f"  Warning: Failed to fetch {currency} for {rate_date}: {e}")

    return None


def refresh_fx_rates(conn, start_year: int = 2005):
    """Refresh FX rates in the database - fetch any missing rates."""
    current_year = date.today().year
    current_quarter = (date.today().month - 1) // 3 + 1

    # Generate all year/quarter combinations
    quarters_to_check = []
    for year in range(start_year, current_year + 1):
        for quarter in range(1, 5):
            # Don't fetch future quarters
            if year > current_year or (year == current_year and quarter > current_quarter):
                continue
            quarters_to_check.append((year, quarter))

    # Check which rates are missing
    cur = conn.cursor()

    rates_to_insert = []
    missing_count = 0

    for year, quarter in quarters_to_check:
        # Use mid-quarter date
        month = (quarter - 1) * 3 + 2
        rate_date = date(year, month, 15)

        for currency in FX_CURRENCIES:
            # Check if rate exists
            cur.execute("""
                SELECT rate FROM fx_rates
                WHERE currency = %s AND year = %s AND quarter = %s
            """, (currency, year, quarter))

            if cur.fetchone() is None:
                missing_count += 1
                # Fetch from API
                rate = fetch_fx_rate(rate_date, currency)

                if rate is not None:
                    rates_to_insert.append((currency, year, quarter, rate, rate_date))
                    print(f"  Fetched {year} Q{quarter} {currency}: {rate:.8f}")

                time.sleep(0.2)  # Rate limiting

    # Insert new rates
    if rates_to_insert:
        execute_values(
            cur,
            """
            INSERT INTO fx_rates (currency, year, quarter, rate, rate_date)
            VALUES %s
            ON CONFLICT (currency, year, quarter)
            DO UPDATE SET rate = EXCLUDED.rate, rate_date = EXCLUDED.rate_date, updated_at = NOW()
            """,
            rates_to_insert
        )
        conn.commit()
        print(f"  Inserted {len(rates_to_insert)} new FX rates")
    else:
        print("  All FX rates up to date")

    cur.close()


def main():
    parser = argparse.ArgumentParser(description="MLIT Data Ingestion Pipeline")

    parser.add_argument("--full", action="store_true", help="Full historical import (2005-present)")
    parser.add_argument("--incremental", action="store_true", help="Import only latest quarter")
    parser.add_argument("--year", type=int, help="Specific year to import")
    parser.add_argument("--prefecture", type=str, help="Prefecture code (e.g., 13 for Tokyo)")
    parser.add_argument("--quarter", type=int, choices=[1, 2, 3, 4], help="Specific quarter (1-4)")
    parser.add_argument("--refresh-fx-only", action="store_true", help="Only refresh FX rates (no API key needed)")

    args = parser.parse_args()

    conn = get_db_connection()

    try:
        # FX-only refresh doesn't need API key
        if args.refresh_fx_only:
            print("Refreshing FX rates...")
            refresh_fx_rates(conn)
            return

        # All other operations need API key
        if API_KEY == "YOUR_API_KEY_HERE":
            print("Error: API key not set")
            print("Set the MLIT_API_KEY environment variable")
            sys.exit(1)

        client = MLITApiClient(API_KEY)

        if args.full:
            prefectures = [args.prefecture] if args.prefecture else None
            ingest_full_history(client, conn, prefectures=prefectures)
            # Also refresh FX rates after full import
            print("\nRefreshing FX rates...")
            refresh_fx_rates(conn)

        elif args.incremental:
            ingest_incremental(client, conn)

        elif args.year:
            prefectures = [args.prefecture] if args.prefecture else sorted(set(PREFECTURE_CODES.values()))
            quarters = [args.quarter] if args.quarter else None

            for pref in prefectures:
                print(f"\nImporting {args.year} for prefecture {pref}")
                ingest_prefecture_year(client, conn, pref, args.year, quarters)

        else:
            parser.print_help()
            print("\nExamples:")
            print("  python ingest_data.py --full                      # Full import")
            print("  python ingest_data.py --year 2023                 # Single year")
            print("  python ingest_data.py --year 2023 --prefecture 13 # Tokyo 2023")
            print("  python ingest_data.py --incremental               # Latest quarter")
            print("  python ingest_data.py --refresh-fx-only           # Refresh FX rates only")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
