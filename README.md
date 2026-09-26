# Japan Real Estate Analytics

A Streamlit dashboard + FastAPI backend for exploring Japanese real estate transaction data from the MLIT (Ministry of Land, Infrastructure, Transport and Tourism) Real Estate Information Library. Designed for interactive analysis, programmatic access, and integration with AI agents.

![Python](https://img.shields.io/badge/python-3.8+-blue.svg)
![Streamlit](https://img.shields.io/badge/streamlit-1.0+-red.svg)
![FastAPI](https://img.shields.io/badge/fastapi-0.104+-green.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

## Features

- **5.8M+ transactions** from all 47 prefectures dating back to 2005
- **Interactive charts**: Time series, histograms, scatter plots with regression
- **Price comparisons**: By ward/city with bar charts and treemaps
- **Age cohort analysis**: Track how prices evolve for buildings of different ages
- **Property valuation**: Estimate values, check listings, track depreciation
- **Multi-currency support**: JPY, USD, EUR, GBP with historical FX rates
- **Flexible units**: Price per m² or per tsubo
- **REST API**: FastAPI backend for programmatic access to all data
- **AI Agent Ready**: MCP integration for Claude, GPT, and other agents to query real estate data
- **Python Client**: Simple library for accessing the API from Python scripts

---

## Quick Start

### Simplest: Docker

```bash
docker compose up -d
```

That's it. Wait ~10 seconds for the database to initialize, then open:
- **Dashboard (UI):** http://localhost:9001
- **API Docs:** http://localhost:8000/docs

| Service | Port | Purpose |
|---------|------|---------|
| Streamlit Dashboard | **9001** | Interactive charts & exploration UI |
| FastAPI Backend | **8000** | REST API for programmatic access |
| PostgreSQL | 5432 | Database (internal only) |

### Local Development (without Docker)

```bash
# Install dependencies
pip install -r requirements.txt

# Terminal 1: Start database (Docker only)
docker compose up -d db

# Terminal 2: Start API
export DATABASE_URL="postgresql://postgres:postgres@localhost/mlit_realestate"
uvicorn api:app --reload --port 8000

# Terminal 3: Start Dashboard
streamlit run app.py --server.port=9001
```

Then open:
- **Dashboard:** http://localhost:9001
- **API Docs:** http://localhost:8000/docs

---

## Using the API (Port 8000)

The **FastAPI backend (port 8000)** provides a public REST API. **No authentication required!** Rate limiting is IP-based.

### Rate Limits

- **100 requests per minute per IP** (1.67 requests/second)
- **Burst allowed**: Up to 200 requests before rate limiting
- **Returns HTTP 429** when limit exceeded
- Rate limit headers included in all responses:
  - `X-RateLimit-Limit`: 100
  - `X-RateLimit-Remaining`: requests left in current minute
  - `X-RateLimit-Reset`: Unix timestamp when limit resets

### 1. Interactive Docs

Open http://localhost:8000/docs in your browser. Try queries directly in the Swagger UI.

### 2. Python Client

```python
from api_client import APIClient

client = APIClient("http://localhost:8000")

# Get prefectures
prefectures = client.get_prefectures()

# Get Tokyo apartment prices (top districts)
prices = client.get_price_by_district(
    prefecture_code="13",
    property_types="Apartment",
    limit=20
)

# Search transactions
transactions = client.get_transactions(
    prefecture_code="13",
    property_types="Apartment",
    year_min=2024,
    price_max=50000000,
    limit=100
)

# Get price trends
trends = client.get_price_trends(
    prefecture_code="13",
    frequency="Yearly"
)
```

### 3. Direct HTTP Calls

```bash
# Health check
curl http://localhost:8000/health

# List prefectures
curl http://localhost:8000/prefectures

# Get Tokyo stats
curl "http://localhost:8000/median-price?prefecture_code=13"

# Search for houses under ¥50M (2024)
curl "http://localhost:8000/transactions?prefecture_code=26&property_types=House&year_min=2024&price_max=50000000"

# Get price trends (yearly)
curl "http://localhost:8000/price-trends?prefecture_code=13&frequency=Yearly"
```

### Check Rate Limit Status

Response headers show your rate limit status:

```bash
curl -i http://localhost:8000/prefectures

# Look for:
# X-RateLimit-Limit: 100
# X-RateLimit-Remaining: 99
# X-RateLimit-Reset: 1695206460
```

---

## Dashboard Tabs

| Tab | Description |
|-----|-------------|
| **Charts** | Time series, histogram, and scatter plots of price trends |
| **Map** | Price comparison by ward/city with visualizations |
| **Districts** | Price trends and YoY changes by district within selected area |
| **Cohorts** | Analyze prices by building age, property size, or total price |
| **Valuation** | Estimate property values, check listings, track depreciation |
| **Raw Data** | Browse and download transaction records |

## Filters

- **Location**: Prefecture, ward/city, district, nearest station
- **Property**: Type, structure (RC, wood, etc.), floor plan (LDK layouts)
- **Size**: Area range (m²)
- **Price**: Total price, price per m²
- **Date**: Transaction year, year built

---

## Data Management

### Import Data

On first run, the database is empty. Import your data backup:

```bash
docker exec -i japan-realestate-db psql -U postgres -d mlit_realestate < backup.sql
```

Or ingest fresh data from the MLIT API:

```bash
docker exec -it japan-realestate-app python dbutils/ingest_data.py --full
```

### Getting an MLIT API Key

Apply for a free MLIT API key at: https://www.reinfolib.mlit.go.jp/api/request/

You'll receive the key via email in 2-3 days.

### Updating Data

MLIT publishes new transaction data quarterly (late April, July, October, January) and occasionally updates historical data. To fetch updates:

```bash
docker exec japan-realestate-app python dbutils/ingest_data.py --full
```

### Automated Monthly Updates

Set up a cron job for automatic monthly re-ingestion:

```bash
mkdir -p ~/japan-realestate/logs
crontab -e
```

Add this line (runs 1st of month at 3am). Note: `\%` escaping is required in crontab:

```
0 3 1 * * docker exec japan-realestate-app python dbutils/ingest_data.py --full > ~/japan-realestate/logs/ingest-$(date +\%Y\%m\%d).log 2>&1
```

To run manually from terminal (no escaping needed):

```bash
docker exec japan-realestate-app python dbutils/ingest_data.py --full > ~/japan-realestate/logs/ingest-$(date +%Y%m%d).log 2>&1
```

Check what was added after each run:

```sql
SELECT transaction_year, COUNT(*) as new_records
FROM transactions
WHERE created_at > NOW() - INTERVAL '1 day'
GROUP BY transaction_year ORDER BY 1;
```

### Useful Commands

```bash
# View logs
docker compose logs -f

# Restart app after code changes
docker compose up -d --build

# Stop everything
docker compose down

# Stop and remove data volume
docker compose down -v
```

---

## API Documentation

### REST Endpoints

- **`GET /`** — API info and endpoints
- **`GET /health`** — Health check
- **`GET /prefectures`** — All prefectures
- **`GET /municipalities?prefecture_code=13`** — Cities/wards
- **`GET /districts?municipality_codes=13101,13102`** — Neighborhoods
- **`GET /transactions?...`** — Query transactions (paginated)
- **`GET /price-trends?...`** — Historical trends (volume, prices)
- **`GET /median-price?...`** — Latest statistics
- **`GET /price-by-district?...`** — Prices by neighborhood
- **`GET /stats`** — Records by year/quarter
- **`GET /stats/summary`** — Overall statistics

All endpoints have full documentation at http://localhost:8000/docs

### Query Parameters

All endpoints support flexible filtering:
- `prefecture_code` — Prefecture code ("13" for Tokyo, "26" for Kyoto, etc.)
- `municipality_codes` — Comma-separated codes
- `districts` — Comma-separated district names
- `property_types` — Types (Apartment, House, Land, etc.)
- `year_min`, `year_max` — Year range
- `price_min`, `price_max` — Price range in JPY
- `area_min`, `area_max` — Area range in m²
- `limit` — Results per page (default 100-1000)
- `offset` — Pagination offset

### Response Format

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

### Query Examples

**1. Tokyo Shibuya apartments (last 2 years)**

```bash
curl "http://localhost:8000/price-by-district?prefecture_code=13&municipality_codes=13104&property_types=Apartment&limit=20"
```

**2. Price trends for Tokyo (yearly)**

```bash
curl "http://localhost:8000/price-trends?prefecture_code=13&frequency=Yearly"
```

**3. Houses in Kyoto under ¥50M (2023-2024)**

```bash
curl "http://localhost:8000/transactions?prefecture_code=26&property_types=House&year_min=2023&year_max=2024&price_max=50000000"
```

**4. Recent transactions in Shibuya (¥60M-¥100M range)**

```bash
curl "http://localhost:8000/transactions?municipality_codes=13104&price_min=60000000&price_max=100000000&year_min=2024&limit=100"
```

### Pagination

Transaction and listing endpoints support pagination:

- `limit` — Items per page (default 1000, max 10000)
- `offset` — Items to skip (default 0)

Example: `/transactions?limit=500&offset=500` returns items 501-1000.

### Performance Notes

- Endpoints are not cached — use the client's built-in caching or add your own if calling repeatedly
- Large result sets (>5000 records) may take several seconds
- For aggregated data (price trends, statistics), filtering is more efficient than fetching all transactions
- Use `limit` and `offset` for pagination of large result sets

---

## MCP (Model Context Protocol) Integration

The API includes MCP tool endpoints for AI agents to discover and call tools directly.

### Using MCP Endpoints

**1. Get available tools**
```bash
curl http://localhost:8000/mcp/tools
```

**2. Call a tool**
```bash
curl -X POST http://localhost:8000/mcp/call/search_transactions \
  -H "Content-Type: application/json" \
  -d '{
    "prefecture_code": "13",
    "property_types": "Apartment",
    "price_max": 50000000,
    "limit": 10
  }'
```

### Available MCP Tools

1. **search_transactions** — Search Japanese real estate transactions with flexible filtering
2. **get_price_trends** — Get historical price trends (time series) for a location
3. **get_district_prices** — Get median prices by district, ranked highest to lowest
4. **get_median_price** — Get overall median and average prices for a location
5. **list_prefectures** — Get all Japanese prefectures
6. **list_municipalities** — Get cities/wards for a prefecture
7. **list_property_types** — Get all available property types
8. **get_statistics** — Get overall database statistics

### Tool Examples

#### Find affordable apartments in Tokyo

```
Tool: search_transactions
Inputs:
  prefecture_code: "13"
  property_types: "Apartment"
  price_min: 30000000
  price_max: 50000000
  limit: 50
```

#### Analyze Kyoto district prices

```
Tool: get_district_prices
Inputs:
  prefecture_code: "26"
  property_types: "House"
  limit: 20
```

#### Track Tokyo condo price trends

```
Tool: get_price_trends
Inputs:
  prefecture_code: "13"
  property_types: "Apartment"
  frequency: "Yearly"
```

#### Compare prices in Shibuya vs Shinjuku

```
Tool: search_transactions
Inputs:
  districts: "Shibuya,Shinjuku"
  property_types: "Apartment"
  year_min: 2023
  limit: 100
```

### Integration with Claude API

```python
from anthropic import Anthropic

client = Anthropic()

tools = [
    {
        "name": "search_transactions",
        "description": "Search Japanese real estate transactions",
        "input_schema": {
            # ... (see MCP documentation for full schema)
        }
    },
    # ... other tools
]

response = client.messages.create(
    model="claude-opus-5-5",
    max_tokens=1024,
    tools=tools,
    messages=[
        {
            "role": "user",
            "content": "What are the most expensive neighborhoods in Tokyo?"
        }
    ]
)
```

---

## Claude Integration Examples

### Example 1: Claude API with Tool Use

```python
import anthropic
import json
import httpx

client = anthropic.Anthropic()

tools = [
    {
        "name": "search_transactions",
        "description": "Search Japanese real estate transactions with filters",
        "input_schema": {
            "type": "object",
            "properties": {
                "prefecture_code": {
                    "type": "string",
                    "description": "Prefecture code (e.g., '13' for Tokyo)"
                },
                "property_types": {
                    "type": "string",
                    "description": "Comma-separated types"
                },
                "price_min": {"type": "number"},
                "price_max": {"type": "number"},
                "year_min": {"type": "integer"},
                "year_max": {"type": "integer"},
                "limit": {"type": "integer", "default": 100}
            }
        }
    }
]

def call_api(tool_name: str, tool_input: dict):
    """Call the Japan Real Estate API"""
    params = {k: v for k, v in tool_input.items() if v is not None}
    
    if tool_name == "search_transactions":
        url = "http://localhost:8000/transactions"
    
    response = httpx.get(url, params=params)
    return response.json()

def run_claude_analysis(user_query: str) -> str:
    """Run an analysis with Claude using tool use"""
    
    messages = [{"role": "user", "content": user_query}]
    
    while True:
        response = client.messages.create(
            model="claude-opus-5-5",
            max_tokens=2048,
            tools=tools,
            messages=messages
        )
        
        if response.stop_reason == "end_turn":
            for block in response.content:
                if hasattr(block, "text"):
                    return block.text
        
        if response.stop_reason == "tool_use":
            messages.append({"role": "assistant", "content": response.content})
            
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    result = call_api(block.name, block.input)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": json.dumps(result)
                    })
            
            messages.append({"role": "user", "content": tool_results})

# Example usage
result = run_claude_analysis(
    "What are the most expensive neighborhoods in Tokyo for apartments? "
    "Show me the top 5 with median prices."
)
print(result)
```

### Example 2: Simple Python Script

```python
from api_client import APIClient
import pandas as pd

client = APIClient("http://localhost:8000")

# Get statistics
stats = client.get_stats_summary()
print(f"Database: {stats['total_records']:,} transactions")
print(f"Date range: {stats['earliest_year']} to {stats['latest_year']}")

# Get Tokyo district prices
districts = client.get_price_by_district(
    prefecture_code="13",
    property_types="Apartment",
    limit=10
)

df = pd.DataFrame(districts)
print("\nTop 10 Most Expensive Tokyo Neighborhoods (Apartments):")
print(df[["district", "median_price", "transaction_count"]].to_string(index=False))

# Get price trends
trends = client.get_price_trends(prefecture_code="13", frequency="Yearly")
df_trends = pd.DataFrame(trends)
print("\nTokyo Apartment Price Trends (Yearly):")
print(df_trends[["transaction_year", "volume", "median_price"]].to_string(index=False))
```

### Example 3: Real Estate Investment Analysis

```python
from api_client import APIClient

client = APIClient("http://localhost:8000")

def analyze_prefecture(code, name):
    """Analyze a prefecture's market"""
    price_stats = client.get_median_price(prefecture_code=code)
    trends = client.get_price_trends(prefecture_code=code, frequency="Yearly")
    districts = client.get_price_by_district(prefecture_code=code, limit=5)
    
    return {
        "name": name,
        "median_price": price_stats.get("median_price", 0),
        "transaction_count": price_stats.get("transaction_count", 0),
        "trends": trends,
        "top_districts": districts
    }

# Compare prefectures
tokyo = analyze_prefecture("13", "Tokyo")
kyoto = analyze_prefecture("26", "Kyoto")

print(f"Tokyo median: ¥{tokyo['median_price']:,} ({tokyo['transaction_count']} transactions)")
print(f"Kyoto median: ¥{kyoto['median_price']:,} ({kyoto['transaction_count']} transactions)")

price_ratio = tokyo['median_price'] / kyoto['median_price']
print(f"\nTokyo is {price_ratio:.1f}x more expensive than Kyoto")
```

---

## Rate Limiting

The public API uses **IP-based rate limiting** for fairness.

### Limits

| Limit | Value |
|-------|-------|
| Requests per minute | 100 |
| Burst allowed | 200 |
| Window | 1 minute |
| Status code when exceeded | 429 (Too Many Requests) |

### What counts?

All API requests count toward the limit, except `/health`.

### What to do if rate limited?

1. **Wait 1 minute** — The limit resets automatically
2. **Reduce request frequency** — Batch queries when possible
3. **Use pagination** — Fetch less data per request with `limit` parameter
4. **Cache results** — Store responses locally to avoid repeated requests

### Example rate limit headers

Every response includes:

```
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 87
X-RateLimit-Reset: 1695206460
```

Meaning: 100 total, 87 requests remaining, resets at Unix timestamp 1695206460.

### nginx Configuration for Production

If deploying behind nginx:

```nginx
server {
    listen 443 ssl;
    server_name api.example.com;

    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;

    # Rate limit per IP: 100 req/min (burst up to 200)
    limit_req_zone $binary_remote_addr zone=api_limit:10m rate=100r/m;
    limit_req zone=api_limit burst=200 nodelay;

    # Security headers
    add_header X-Content-Type-Options "nosniff" always;
    add_header X-Frame-Options "DENY" always;
    add_header Strict-Transport-Security "max-age=31536000" always;

    # Proxy all requests
    location / {
        proxy_pass http://localhost:8000;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }
}
```

---

## System Architecture

### Component Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                        Clients                              │
├────────────┬──────────────────────┬────────────────────────┤
│            │                      │                        │
│      Streamlit UI           External Agents           Other Apps
│       (Browser)         (Claude, GPT, etc.)          (Python/JS)
│            │                      │                        │
└────────────┼──────────────────────┼────────────────────────┘
             │                      │
             │        ┌─────────────┴────────────┐
             │        │                          │
      ┌──────▼────────▼──────┐          ┌─────────▼─────────┐
      │   FastAPI Backend    │          │    MCP Server     │
      │   (api.py)           │          │ (mcp_server.py)   │
      │   Port: 8000         │          │ (Wraps API calls) │
      └──────┬───────────────┘          └────────────────────┘
             │
      ┌──────▼────────────────┐
      │ PostgreSQL Database   │
      │ (mlit_realestate)     │
      └───────────────────────┘
```

### Technology Stack

```
Client Layer
├── Streamlit (UI)
├── Claude API (LLM)
└── Python scripts

API Layer
├── FastAPI (web framework)
├── uvicorn (ASGI server)
├── pydantic (validation)
└── httpx (async HTTP)

Database Layer
├── PostgreSQL (data storage)
├── psycopg2 (Python driver)
└── Connection pooling

Container
└── Docker Compose (orchestration)
```

### Performance Characteristics

| Endpoint | Latency | Result Size | Cacheable |
|----------|---------|------------|-----------|
| Reference data | <10ms | Small | ✓ 24hr |
| Statistics | 100-500ms | Medium | ✓ 1hr |
| Price trends | 500ms-2s | Medium | ✓ 1hr |
| Transactions | 1-5s | Large (paginated) | ✓ 24hr |
| Median price | 100-500ms | Small | ✓ 1hr |

---

## Development

### Adding API Endpoints

1. Add function to `api.py`
2. Use `run_query()` helper for database access
3. Define parameters with FastAPI type hints
4. Add corresponding method to `api_client.py`

Example:
```python
@app.get("/custom-endpoint")
def get_custom_data(prefecture_code: str, limit: int = 50):
    query = "SELECT ... WHERE prefecture_code = %s"
    results = run_query(query, (prefecture_code,))
    return results
```

### Running Tests

```bash
# Health check
curl http://localhost:8000/health

# Test via client
python -c "from api_client import APIClient; print(APIClient().get_stats_summary())"
```

### Development Workflow

1. Edit `api.py`
2. With `--reload` flag, uvicorn auto-restarts:
   ```bash
   uvicorn api:app --reload --port 8000
   ```
3. Test endpoint in http://localhost:8000/docs
4. Add corresponding method to `api_client.py`

---

## Japanese Real Estate Terms

| Term | Meaning |
|------|---------|
| **Tsubo** (坪) | Traditional area unit. 1 tsubo ≈ 3.31 m² |
| **LDK** | L=Living, D=Dining, K=Kitchen. Example: 2LDK = 2 bedrooms + LDK |
| **Mansion** (マンション) | Concrete apartment/condo building (not a large house) |
| **Chome** (丁目) | District subdivision, like a block number |
| **RC/SRC** | Reinforced Concrete / Steel Reinforced Concrete |

---

## Data Source

This service uses the MLIT Real Estate Information Library API. The accuracy, completeness, and timeliness of the data is not guaranteed.

このサービスは、国土交通省不動産情報ライブラリのAPI機能を使用していますが、提供情報の最新性、正確性、完全性等が保証されたものではありません。

---

## License

MIT

## Contributing

An example instance of this project is hosted at https://anil.diwi.org/japan-realestate/

Issues and pull requests welcome. For issues:
1. Check logs: `docker compose logs -f`
2. Verify database: `psql -d mlit_realestate -c "SELECT COUNT(*) FROM transactions;"`
3. Test API: `curl http://localhost:8000/health`
4. See GitHub issues: https://github.com/AnilDaoud/japan-realestate/issues
