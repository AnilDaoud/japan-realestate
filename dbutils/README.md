# MLIT Real Estate Data Pipeline

Tools for fetching Japanese real estate transaction data from the MLIT API and storing it in PostgreSQL for analysis and charting.

## Files

| File | Description |
|------|-------------|
| `mlit_api_client.py` | Python client for MLIT Real Estate Information Library API |
| `schema.sql` | PostgreSQL schema with indexes and materialized views |
| `ingest_data.py` | Data ingestion pipeline (API → PostgreSQL) |

## Quick Start

### 1. Get API Key

Apply for a free API key at: https://www.reinfolib.mlit.go.jp/api/request/

You'll receive the key via email in 2-3 days.

### 2. Set Environment Variables

```bash
export MLIT_API_KEY="your-api-key-here"
export DATABASE_URL="postgresql://user:pass@localhost/mlit_realestate"
```

### 3. Create Database

```bash
# Create database
createdb mlit_realestate

# Apply schema
psql mlit_realestate < schema.sql
```

### 4. Test API Connection

```bash
python mlit_api_client.py
```

### 5. Ingest Data

```bash
# Import all Tokyo data for 2023
python ingest_data.py --year 2023 --prefecture 13

# Import all historical data (slow - all prefectures since 2005)
python ingest_data.py --full

# Incremental update (latest quarter only)
python ingest_data.py --incremental
```

## API Reference

### Endpoints

| Endpoint | Purpose |
|----------|---------|
| `XIT001` | Transaction/contract prices |
| `XIT002` | Municipality list |
| `XIT003` | Station list |

### Query Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `year` | Transaction year | `2023` |
| `quarter` | Quarter (1-4) | `4` |
| `area` | Prefecture code | `13` (Tokyo) |
| `city` | Municipality code | `13103` (Minato-ku) |
| `station` | Station code | `003785` |
| `priceClassification` | `01`=transaction, `02`=contract | `01` |
| `language` | `en` or `ja` | `en` |

## Prefecture Codes

| Code | Prefecture | Code | Prefecture |
|------|------------|------|------------|
| 01 | Hokkaido | 25 | Shiga |
| 02 | Aomori | 26 | Kyoto |
| 03 | Iwate | 27 | Osaka |
| 04 | Miyagi | 28 | Hyogo |
| 05 | Akita | 29 | Nara |
| 06 | Yamagata | 30 | Wakayama |
| 07 | Fukushima | 31 | Tottori |
| 08 | Ibaraki | 32 | Shimane |
| 09 | Tochigi | 33 | Okayama |
| 10 | Gunma | 34 | Hiroshima |
| 11 | Saitama | 35 | Yamaguchi |
| 12 | Chiba | 36 | Tokushima |
| 13 | **Tokyo** | 37 | Kagawa |
| 14 | Kanagawa | 38 | Ehime |
| 15 | Niigata | 39 | Kochi |
| 16 | Toyama | 40 | Fukuoka |
| 17 | Ishikawa | 41 | Saga |
| 18 | Fukui | 42 | Nagasaki |
| 19 | Yamanashi | 43 | Kumamoto |
| 20 | Nagano | 44 | Oita |
| 21 | Gifu | 45 | Miyazaki |
| 22 | Shizuoka | 46 | Kagoshima |
| 23 | Aichi | 47 | Okinawa |
| 24 | Mie | | |

## Tokyo 23 Wards (City Codes)

| Code | Ward | Code | Ward |
|------|------|------|------|
| 13101 | Chiyoda | 13113 | Shibuya |
| 13102 | Chuo | 13114 | Nakano |
| 13103 | **Minato** | 13115 | Suginami |
| 13104 | Shinjuku | 13116 | Toshima |
| 13105 | Bunkyo | 13117 | Kita |
| 13106 | Taito | 13118 | Arakawa |
| 13107 | Sumida | 13119 | Itabashi |
| 13108 | Koto | 13120 | Nerima |
| 13109 | Shinagawa | 13121 | Adachi |
| 13110 | Meguro | 13122 | Katsushika |
| 13111 | Ota | 13123 | Edogawa |
| 13112 | Setagaya | | |

## Sample Queries

### Historical Price Trend (Minato-ku Apartments)

```sql
SELECT
    transaction_year,
    transaction_quarter,
    COUNT(*) as transactions,
    ROUND(AVG(unit_price)) as avg_price_per_m2,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY unit_price)) as median
FROM transactions
WHERE municipality_code = '13103'
  AND property_type_id = 1  -- Condos
  AND unit_price IS NOT NULL
GROUP BY transaction_year, transaction_quarter
ORDER BY transaction_year, transaction_quarter;
```

### Price by Ward Comparison

```sql
SELECT
    m.name_en as ward,
    COUNT(*) as transactions,
    ROUND(AVG(t.unit_price)) as avg_price_m2,
    ROUND(AVG(t.trade_price) / 1000000, 1) as avg_price_million
FROM transactions t
JOIN municipalities m ON t.municipality_code = m.code
WHERE t.prefecture_code = '13'
  AND t.transaction_year = 2023
  AND t.property_type_id = 1
GROUP BY m.name_en
ORDER BY avg_price_m2 DESC;
```

### Age vs Price Regression Data

```sql
SELECT
    building_age,
    unit_price,
    area_m2,
    floor_number
FROM transactions
WHERE municipality_code = '13103'
  AND property_type_id = 1
  AND transaction_year >= 2020
  AND building_age BETWEEN 0 AND 50
  AND unit_price BETWEEN 500000 AND 3000000;
```

## Data Fields Reference

| API Field | DB Column | Description |
|-----------|-----------|-------------|
| TradePrice | trade_price | Total transaction price (JPY) |
| UnitPrice | unit_price | Price per m² (JPY) |
| Area | area_m2 | Unit/land area |
| TotalFloorArea | total_floor_area_m2 | Total floor area |
| FloorPlan | floor_plan | Layout (1LDK, 2DK, etc.) |
| BuildingYear | building_year | Year of construction |
| Structure | structure | RC, SRC, Wood, Steel, etc. |
| CityPlanning | city_planning | Zoning designation |
| CoverageRatio | coverage_ratio | Building coverage % |
| FloorAreaRatio | floor_area_ratio | Floor area ratio % |
| Period | transaction_period | Transaction quarter |

## Performance Tips

1. **Use materialized views** for dashboard queries:
   ```sql
   REFRESH MATERIALIZED VIEW CONCURRENTLY mv_monthly_prices;
   ```

2. **Filter on indexed columns first**: prefecture → municipality → year

3. **Use category columns** for histograms: `price_category`, `size_category`, `age_category`

4. **Limit date ranges** when possible - full scans are slow

## Next Steps

- [ ] Build REST API (FastAPI) for chart data
- [ ] Add station data import
- [ ] Create frontend with Plotly/ECharts
- [ ] Set up scheduled ingestion (cron)
- [ ] Add rent data for yield calculations
