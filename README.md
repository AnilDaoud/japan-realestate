# Japan Real Estate Analytics

A Streamlit dashboard + FastAPI backend for exploring Japanese real estate transaction data from the MLIT (Ministry of Land, Infrastructure, Transport and Tourism) Real Estate Information Library. Designed for interactive analysis, programmatic access, and integration with AI agents.

![Python](https://img.shields.io/badge/python-3.8+-blue.svg)
![Streamlit](https://img.shields.io/badge/streamlit-1.0+-red.svg)
![FastAPI](https://img.shields.io/badge/fastapi-0.104+-green.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

## Features

- **6.1M+ transactions** from all 47 prefectures dating back to 2005
- **Interactive charts**: Time series, histograms, scatter plots with regression
- **Price comparisons**: By ward/city with bar charts and treemaps
- **Age cohort analysis**: Track how prices evolve for buildings of different ages
- **Property valuation**: Estimate values, check listings, track depreciation
- **Multi-currency support**: JPY, USD, EUR, GBP with historical FX rates
- **Flexible units**: Price per m² or per tsubo
- **REST API**: FastAPI backend for programmatic access to all data
- **AI Agent Ready**: MCP integration for Claude, GPT, and other agents to query real estate data
- **Python Client**: Simple library for accessing the API from Python scripts

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

## Using the API (Port 8000)

The **FastAPI backend (port 8000)** provides a REST API for programmatic access. Three ways to use it:

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

See [API Documentation](#api-documentation) for the full endpoint reference.

### Import Data

On first run, the database is empty. Import your data backup:

```bash
docker exec -i japan-realestate-db psql -U postgres -d mlit_realestate < backup.sql
```

Or ingest fresh data from the MLIT API:

```bash
docker exec -it japan-realestate-app python dbutils/ingest_data.py --full
```

### Access

Open http://localhost:9001 in your browser.

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

## Getting an API Key

Apply for a free MLIT API key at: https://www.reinfolib.mlit.go.jp/api/request/

You'll receive the key via email in 2-3 days.

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

## Japanese Real Estate Terms

| Term | Meaning |
|------|---------|
| **Tsubo** (坪) | Traditional area unit. 1 tsubo ≈ 3.31 m² |
| **LDK** | L=Living, D=Dining, K=Kitchen. Example: 2LDK = 2 bedrooms + LDK |
| **Mansion** (マンション) | Concrete apartment/condo building (not a large house) |
| **Chome** (丁目) | District subdivision, like a block number |
| **RC/SRC** | Reinforced Concrete / Steel Reinforced Concrete |

## Data Source

This service uses the MLIT Real Estate Information Library API. The accuracy, completeness, and timeliness of the data is not guaranteed.

このサービスは、国土交通省不動産情報ライブラリのAPI機能を使用していますが、提供情報の最新性、正確性、完全性等が保証されたものではありません。

## AI Agent Integration

Use the API with AI agents via the Python client or REST endpoints:

```python
# Claude API example with tool use
import anthropic

client = anthropic.Anthropic()

# Define Japan Real Estate API tools
tools = [
    {
        "name": "search_transactions",
        "description": "Search real estate transactions",
        "input_schema": {
            "type": "object",
            "properties": {
                "prefecture_code": {"type": "string"},
                "property_types": {"type": "string"},
                "price_min": {"type": "number"},
                "price_max": {"type": "number"}
            }
        }
    },
    # ... more tools (see mcp_server.py)
]

response = client.messages.create(
    model="claude-opus-5-5",
    max_tokens=1024,
    tools=tools,
    messages=[{
        "role": "user",
        "content": "What are the most expensive neighborhoods in Tokyo?"
    }]
)
```

See [CLAUDE_EXAMPLE.md](CLAUDE_EXAMPLE.md) for complete examples.

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

## Architecture

```
┌─────────────────────────────────────┐
│  Streamlit Dashboard (port 9001)    │
│  External Scripts                   │
│  AI Agents (Claude, GPT, etc.)      │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│  FastAPI Backend (port 8000)        │
│  - REST endpoints                   │
│  - Data validation                  │
│  - Request handling                 │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│  PostgreSQL Database                │
│  6.1M+ real estate transactions     │
└─────────────────────────────────────┘
```

## Security & Monitoring

### Protecting the Public API

Since the API is exposed via nginx, it includes multiple security layers:

**1. API Key Authentication**
```bash
# Every request requires an API key header
curl -H "X-API-Key: your-api-key" http://api.example.com/transactions
```

**2. Rate Limiting (per API key, per hour)**
- Claude Agent: 5,000 requests/hour
- Streamlit Dashboard: 2,000 requests/hour
- Public Demo: 100 requests/hour
- Returns HTTP 429 if exceeded

**3. nginx Protection**
- SSL/TLS encryption (required)
- IP-based rate limiting (10 req/sec per IP)
- Security headers (XSS, clickjacking protection)

**4. Request Validation**
- Pydantic validates all input parameters
- Prevents injection attacks

### Setup Production Security

**Enable security** (one-time):
```bash
export REQUIRE_API_KEY=true
export ADMIN_KEY="your-secure-admin-key"
export CORS_ORIGINS="https://yourapp.com"
docker compose restart api
```

**Create API keys** for each client:
```bash
ADMIN_KEY="your-secure-admin-key"

# Create key for Claude agent
curl -X POST http://localhost:8000/admin/keys/add \
  -H "X-API-Key: $ADMIN_KEY" \
  -d '{"name": "Claude", "quota_per_hour": 5000}'

# Create key for public demo (limited)
curl -X POST http://localhost:8000/admin/keys/add \
  -H "X-API-Key: $ADMIN_KEY" \
  -d '{"name": "Public Demo", "quota_per_hour": 100}'
```

**nginx configuration** (add to your server config):
```nginx
server {
    listen 443 ssl;
    server_name api.example.com;
    
    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;
    
    # Rate limiting
    limit_req_zone $http_x_api_key zone=api_limit:10m rate=100r/s;
    limit_req zone=api_limit burst=200 nodelay;
    
    # Security headers
    add_header X-Content-Type-Options "nosniff" always;
    add_header X-Frame-Options "DENY" always;
    add_header Strict-Transport-Security "max-age=31536000" always;
    
    # Require API key on all endpoints
    location / {
        if ($http_x_api_key = "") { return 403; }
        proxy_pass http://localhost:8000;
    }
    
    # Exception: health check (no key needed)
    location /health {
        proxy_pass http://localhost:8000;
    }
}
```

### Monitoring Usage

**View all metrics** (requires admin key):
```bash
ADMIN_KEY="your-secure-admin-key"
curl -H "X-API-Key: $ADMIN_KEY" http://localhost:8000/admin/metrics | jq
```

Response shows per-API-key: requests, errors, avg response time, error rate

**Check system status**:
```bash
curl -H "X-API-Key: $ADMIN_KEY" http://localhost:8000/admin/status | jq
```

**View logs in real-time**:
```bash
tail -f /var/log/japan-realestate/api.log

# Filter for errors
grep ERROR /var/log/japan-realestate/api.log

# Filter for rate limit violations
grep "Rate limit" /var/log/japan-realestate/api.log
```

### Managing API Keys

**Revoke a key** (if compromised):
```bash
curl -X POST http://localhost:8000/admin/keys/revoke \
  -H "X-API-Key: $ADMIN_KEY" \
  -d '{"api_key": "key_to_revoke"}'
```

**Check a key's usage before revoking**:
```bash
curl -H "X-API-Key: $ADMIN_KEY" \
  "http://localhost:8000/admin/metrics?api_key=claude-key" | jq
```

### Responding to Abuse

1. **Check logs**: `grep "Rate limit" /var/log/japan-realestate/api.log`
2. **Review metrics**: `curl -H "X-API-Key: $ADMIN_KEY" http://localhost:8000/admin/metrics`
3. **Revoke if needed**: `/admin/keys/revoke`
4. **Lower quota or rotate key** if legitimate user exceeded limit

For full security documentation, see `api_security.py` in the repository.

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

## License

MIT

## Contributing

An example instance of this project is hosted at https://anil.diwi.org/japan-realestate/

Issues and pull requests welcome.

For detailed architecture and development notes, see:
- [API.md](API.md) — Full API reference and examples
- [MCP.md](MCP.md) — MCP tool definitions
- [ARCHITECTURE.md](ARCHITECTURE.md) — System design and rationale
- [CLAUDE_EXAMPLE.md](CLAUDE_EXAMPLE.md) — AI integration examples
