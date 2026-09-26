# API Layer Setup Guide

This guide covers setting up and testing the new FastAPI backend that decouples the database layer from the Streamlit UI.

## Architecture Overview

**Before (Monolithic):**
```
Streamlit App (app.py)
    ↓
PostgreSQL Database
```

**After (Layered):**
```
External Agents / MCP Clients
    ↓
FastAPI (api.py)    ←→    PostgreSQL Database
    ↑
Streamlit App (app.py)
```

The API layer allows:
1. Direct agent/MCP access to data
2. Future Streamlit refactoring to use API instead of direct DB queries
3. Multi-client support without database connection overload
4. Easier caching and performance optimization

## Quick Start with Docker

### Build and Start

```bash
# Install dependencies and start all services
docker compose up -d

# Verify services are running
docker compose ps
```

You should see:
- `japan-realestate-api` running on port 8000
- `japan-realestate-app` running on port 9001
- `japan-realestate-db` (PostgreSQL)

### Test the API

```bash
# Health check
curl http://localhost:8000/health

# Get API documentation (interactive Swagger UI)
open http://localhost:8000/docs

# Get prefectures
curl http://localhost:8000/prefectures

# Get Tokyo statistics
curl "http://localhost:8000/median-price?prefecture_code=13"
```

### View Logs

```bash
# All services
docker compose logs -f

# Just the API
docker compose logs -f api

# Just the Streamlit app
docker compose logs -f app
```

## Local Development (without Docker)

If you want to develop locally:

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Start PostgreSQL

```bash
# Option A: Docker (just the DB)
docker run -d \
  --name japan-realestate-dev-db \
  -e POSTGRES_DB=mlit_realestate \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  postgres:15-alpine

# Option B: Use existing docker compose (skip API/Streamlit services)
docker compose up -d db

# Initialize schema
psql -h localhost -U postgres -d mlit_realestate < dbutils/schema_optimized.sql
```

### 3. Start the API

```bash
export DATABASE_URL="postgresql://postgres:postgres@localhost/mlit_realestate"
uvicorn api:app --reload --port 8000
```

Open http://localhost:8000/docs to see interactive API documentation.

### 4. Start Streamlit (separate terminal)

```bash
export DATABASE_URL="postgresql://postgres:postgres@localhost/mlit_realestate"
streamlit run app.py --server.port=9001
```

## API Endpoints

### Documentation

- **Swagger UI**: http://localhost:8000/docs (interactive)
- **ReDoc**: http://localhost:8000/redoc (read-only)
- **OpenAPI Schema**: http://localhost:8000/openapi.json

### Root Endpoint

```bash
curl http://localhost:8000/
```

Returns available endpoints by category.

### Example Queries

**1. List all prefectures:**
```bash
curl http://localhost:8000/prefectures
```

**2. Get Tokyo municipalities:**
```bash
curl http://localhost:8000/municipalities?prefecture_code=13
```

**3. Get recent apartment prices in Tokyo (top districts):**
```bash
curl "http://localhost:8000/price-by-district?prefecture_code=13&property_types=Apartment"
```

**4. Search for houses in Tokyo under ¥50M (2024):**
```bash
curl "http://localhost:8000/transactions?prefecture_code=13&property_types=House&year_min=2024&price_max=50000000&limit=50"
```

**5. Get yearly price trends for Tokyo:**
```bash
curl "http://localhost:8000/price-trends?prefecture_code=13&frequency=Yearly"
```

## Using the Python Client

The `api_client.py` module provides a convenient Python interface:

```python
from api_client import APIClient

# Initialize client (adjust URL if not localhost)
client = APIClient("http://localhost:8000")

# Get prefectures
prefs = client.get_prefectures()
print(prefs)

# Get Tokyo stats
stats = client.get_stats_summary()
print(f"Total records: {stats['total_records']}")

# Search transactions
results = client.get_transactions(
    prefecture_code="13",
    property_types="Apartment",
    year_min=2024,
    limit=100
)
print(f"Found {results['count']} apartments in Tokyo (2024)")

# Get price trends
trends = client.get_price_trends(
    prefecture_code="13",
    frequency="Yearly"
)
for trend in trends:
    print(f"{trend['transaction_year']}: {trend['volume']} transactions, " 
          f"¥{trend['median_price']:,.0f} median")
```

## MCP Integration

To use this data with AI agents via MCP, see `MCP.md`.

The `mcp_server.py` module defines 8 tools that agents can call:
1. search_transactions
2. get_price_trends
3. get_district_prices
4. get_median_price
5. list_prefectures
6. list_municipalities
7. list_property_types
8. get_statistics

## Performance Tips

### For Streamlit App

To refactor the Streamlit app to use the API instead of direct DB queries:

```python
# Current (direct DB):
@st.cache_data(ttl=86400)
def get_price_trends(filters):
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT ...", ...)
        return pd.DataFrame(cur.fetchall())

# New (via API):
from api_client import APIClient
client = APIClient("http://localhost:8000")

@st.cache_data(ttl=86400)
def get_price_trends(filters):
    data = client.get_price_trends(**filters)
    return pd.DataFrame(data)
```

### Caching Strategy

- API responses are **not cached** by default
- Streamlit's `@st.cache_data()` can cache API results
- Consider adding Redis for shared caching across multiple instances

### Connection Pooling

Currently:
- Streamlit app: Direct connection per session
- API: New connection per request

For production with many users:
- Wrap PostgreSQL with pgbouncer
- Add connection pooling to API (SQLAlchemy)

## Monitoring

### Health Check

```bash
curl http://localhost:8000/health
```

Returns:
```json
{
  "status": "healthy",
  "timestamp": "2024-09-26T12:34:56.789Z"
}
```

### Database Statistics

```bash
curl http://localhost:8000/stats/summary
```

Returns overall database size and date range.

## Troubleshooting

### API not starting

```bash
# Check logs
docker compose logs api

# Verify database is running
docker compose logs db

# Test database connection manually
psql "postgresql://postgres:postgres@localhost:5432/mlit_realestate" -c "SELECT COUNT(*) FROM transactions;"
```

### "Connection refused" to API

```bash
# Check if port 8000 is in use
lsof -i :8000

# Check if API container is running
docker compose ps api

# Restart API
docker compose restart api
```

### Slow queries

1. Check if database queries have proper indexes:
   ```bash
   psql -d mlit_realestate -c "\d transactions"
   ```

2. Monitor query performance:
   ```bash
   # In PostgreSQL
   EXPLAIN ANALYZE SELECT ...;
   ```

3. Add query filters (location, date, type) to reduce result sets

### Database connection issues

```bash
# Check postgres is running
docker compose logs db

# Restart database
docker compose restart db

# Verify schema was created
psql -d mlit_realestate -c "\dt"
```

## Development Workflow

### Making API Changes

1. Edit `api.py`
2. With `--reload` flag, uvicorn auto-restarts:
   ```bash
   uvicorn api:app --reload --port 8000
   ```
3. Test endpoint in http://localhost:8000/docs
4. Add corresponding method to `api_client.py`

### Adding New Endpoints

Example: Add endpoint to get price statistics

```python
# In api.py
@app.get("/price-statistics")
def get_price_statistics(prefecture_code: str = Query(...)):
    """Get price statistics for a location."""
    query = """
    SELECT
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY transaction_price) as q1,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY transaction_price) as median,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY transaction_price) as q3,
        STDDEV(transaction_price) as std_dev,
        MIN(transaction_price) as min_price,
        MAX(transaction_price) as max_price
    FROM transactions
    WHERE prefecture_code = %s
    """
    results = run_query(query, (prefecture_code,))
    return results[0] if results else {}
```

Then add to `api_client.py`:

```python
def get_price_statistics(self, prefecture_code: str) -> Dict:
    """Get price statistics."""
    return self.get(f"/price-statistics?prefecture_code={prefecture_code}")
```

## Next Steps

### Phase 1: API Layer (✓ Done)
- FastAPI service running
- Core endpoints for data access
- Python client library
- MCP tool definitions

### Phase 2: Streamlit Refactoring
- Migrate Streamlit queries to use API client
- Remove direct database connections from app.py
- Test all tabs still work correctly

### Phase 3: MCP Server
- Implement full MCP SDK integration
- Deploy as standalone service or library
- Test with Claude or other agents

### Phase 4: Optimization
- Add caching layer (Redis)
- Connection pooling
- Query performance tuning
- Rate limiting for public API

## Documentation

- **API.md** — Full REST API reference
- **MCP.md** — MCP tool definitions and examples
- **API_SETUP.md** — This file (setup and troubleshooting)

## Support

For issues:
1. Check logs: `docker compose logs -f`
2. Verify database: `psql -d mlit_realestate -c "SELECT COUNT(*) FROM transactions;"`
3. Test API: `curl http://localhost:8000/health`
4. See GitHub issues: https://github.com/AnilDaoud/japan-realestate/issues
