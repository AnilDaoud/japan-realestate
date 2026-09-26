# Japan Real Estate Analytics - API

FastAPI-based REST backend for querying Japanese real estate transaction data. Designed for programmatic access and integration with external agents (MCPs, AI assistants, etc.).

## Quick Start

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Start API (requires PostgreSQL running)
uvicorn api:app --reload --port 8000

# API documentation: http://localhost:8000/docs
```

### Docker

```bash
docker compose up -d

# API will be at http://localhost:8000
# Streamlit app still at http://localhost:9001
```

## API Endpoints

### Root & Health

- `GET /` — API info and endpoint overview
- `GET /health` — Health check with database connection test

### Reference Data

These endpoints return available dimensions for filtering:

- `GET /prefectures` — All prefectures with codes
- `GET /municipalities?prefecture_code=13` — Cities/wards for a prefecture
- `GET /districts?municipality_codes=13101,13102` — Districts (comma-separated municipalities)
- `GET /property-types` — All property types (Apartment, House, Land, etc.)
- `GET /structures` — Building structure types (RC, SRC, Wood, etc.)
- `GET /floor-plans` — Floor plan types (1K, 2LDK, etc.)
- `GET /year-range` — Min/max transaction years available
- `GET /building-year-range` — Min/max building construction years

### Statistics

- `GET /stats` — Record counts by year and quarter
- `GET /stats/summary` — Overall database statistics (total records, date range, etc.)

### Transactions

Query individual transactions with flexible filtering:

```
GET /transactions
  ?prefecture_code=13
  &municipality_codes=13101,13102
  &districts=Shibuya,Shinjuku
  &property_types=Apartment,House
  &year_min=2020
  &year_max=2024
  &price_min=10000000
  &price_max=500000000
  &area_min=20
  &area_max=150
  &limit=1000
  &offset=0
```

Returns paginated transaction data with all fields (location, price, area, year built, structure, floor plan, etc.).

### Price Analysis

- `GET /price-trends` — Historical price trends (volume, avg/median price, unit price)
  - Query params: `prefecture_code`, `municipality_codes`, `districts`, `property_types`, `frequency` (Quarterly|Yearly)

- `GET /median-price` — Latest median price for an area
  - Query params: `prefecture_code`, `municipality_codes`, `property_types`
  - Returns: median_price, median_unit_price, avg_price, transaction_count

- `GET /price-by-district` — Prices grouped by district (sorted by median price)
  - Query params: `prefecture_code`, `municipality_codes`, `property_types`, `limit` (default 50, max 500)

## Response Format

All endpoints return JSON. Transaction queries return paginated results:

```json
{
  "count": 1000,
  "limit": 1000,
  "offset": 0,
  "data": [
    {
      "id": 12345,
      "prefecture_code": "13",
      "prefecture_name": "Tokyo",
      "municipality_code": "13101",
      "municipality_name": "Chiyoda Ward",
      "district": "Marunouchi",
      "property_type": "Apartment",
      "transaction_price": 65000000,
      "area": 72.5,
      "year_built": 2015,
      "structure_type": "RC",
      "floor_plan": "2LDK",
      "transaction_year": 2024,
      "transaction_quarter": 2,
      "...": "other fields"
    }
  ]
}
```

## Query Examples

### Example 1: Tokyo Shibuya apartments (last 2 years)

```bash
curl "http://localhost:8000/price-by-district?prefecture_code=13&municipality_codes=13104&property_types=Apartment&limit=20"
```

### Example 2: Price trends for Tokyo (yearly)

```bash
curl "http://localhost:8000/price-trends?prefecture_code=13&frequency=Yearly"
```

### Example 3: Houses in Kyoto under ¥50M (2023-2024)

```bash
curl "http://localhost:8000/transactions?prefecture_code=26&property_types=House&year_min=2023&year_max=2024&price_max=50000000"
```

### Example 4: Recent transactions in Shibuya (¥60M-¥100M range)

```bash
curl "http://localhost:8000/transactions?municipality_codes=13104&price_min=60000000&price_max=100000000&year_min=2024&limit=100"
```

## Python Client

Use `api_client.py` for convenient Python access:

```python
from api_client import APIClient

client = APIClient("http://localhost:8000")

# Get prefectures
prefs = client.get_prefectures()

# Get price trends for Tokyo
trends = client.get_price_trends(prefecture_code="13", frequency="Yearly")

# Get transactions in Shibuya
transactions = client.get_transactions(
    municipality_codes="13104",
    year_min=2024,
    limit=500
)

# Check health
status = client.health_check()
```

## Integration with MCP

The API is designed to be wrapped by an MCP (Model Context Protocol) server for LLM agents to query real estate data. See `MCP.md` for the MCP implementation.

## Pagination

Transaction and listing endpoints support pagination:

- `limit` — Items per page (default 1000, max 10000)
- `offset` — Items to skip (default 0)

Example: `/transactions?limit=500&offset=500` returns items 501-1000.

## Filtering

Most endpoints accept these filters (all optional):

- `prefecture_code` — Prefecture code (e.g., "13" for Tokyo)
- `municipality_codes` — Comma-separated municipality codes
- `districts` — Comma-separated district names
- `property_types` — Comma-separated types (Apartment, House, Land, etc.)
- `year_min`, `year_max` — Transaction year range
- `price_min`, `price_max` — Price range in JPY
- `area_min`, `area_max` — Area range in m²

Omit any filter to include all values.

## Performance Notes

- Endpoints are not cached — use the client's built-in caching or add your own if calling repeatedly
- Large result sets (>5000 records) may take several seconds
- For aggregated data (price trends, statistics), filtering is more efficient than fetching all transactions
- Use `limit` and `offset` for pagination of large result sets

## Deployment

### Environment Variables

- `DATABASE_URL` — PostgreSQL connection string (default: `postgresql://localhost/mlit_realestate`)

### Production Considerations

1. **Authentication**: Add API key validation if exposing publicly
2. **Rate Limiting**: Add rate limits per IP/API key
3. **CORS**: Currently allows all origins; restrict in production
4. **Caching**: Consider adding Redis layer for frequently-accessed data
5. **Connection Pooling**: Streamlit app and API both use direct connections; consider pgbouncer

## Development

### Adding New Endpoints

1. Add query function in `api.py`
2. Use `run_query()` helper for database access
3. Define query parameters with FastAPI type hints
4. Return JSON-serializable data
5. Add corresponding method to `api_client.py`

### Testing

```bash
# Test endpoint directly
curl http://localhost:8000/stats/summary

# Test with client
python -c "from api_client import APIClient; print(APIClient().get_stats_summary())"
```
