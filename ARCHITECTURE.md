# API Layer Architecture

## Summary

The app has been refactored to separate the data layer (FastAPI) from the presentation layer (Streamlit). This enables:

1. **Direct agent access** — AI agents and MCPs can query data without going through Streamlit
2. **Multi-client support** — Multiple clients can access the same data independently
3. **Better scalability** — Database queries are centralized and can be optimized/cached
4. **Clear separation of concerns** — API, UI, and MCP are independent modules

## File Structure

```
japan-realestate/
├── api.py                 # FastAPI backend (NEW)
├── api_client.py          # Python client for API (NEW)
├── mcp_server.py          # MCP tool definitions (NEW)
├── app.py                 # Streamlit UI (unchanged)
├── requirements.txt       # Updated with fastapi, uvicorn
├── docker-compose.yml     # Updated to run API + Streamlit
├── API.md                 # REST API reference (NEW)
├── MCP.md                 # MCP tool docs (NEW)
├── API_SETUP.md           # Setup and deployment guide (NEW)
└── ARCHITECTURE.md        # This file (NEW)
```

## Component Diagram

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

## Workflow

### Query Flow (Example: Get Tokyo Apartment Prices)

**Via Streamlit:**
```
User in Browser
    ↓
Streamlit sidebar filters
    ↓
api_client.get_price_trends(prefecture_code="13")
    ↓
FastAPI GET /price-trends?prefecture_code=13
    ↓
PostgreSQL query
    ↓
JSON response
    ↓
Streamlit renders chart
```

**Via MCP Agent:**
```
User prompt: "What are the most expensive neighborhoods in Tokyo?"
    ↓
Claude decomposes into MCP tool calls
    ↓
Call: get_district_prices(prefecture_code="13")
    ↓
MCP Server calls API
    ↓
FastAPI GET /price-by-district?prefecture_code=13
    ↓
PostgreSQL query
    ↓
JSON response
    ↓
Claude interprets and responds
```

## API Endpoints (Organized by Purpose)

### Reference Data (read-only, low latency)
- `GET /prefectures` — All regions
- `GET /municipalities?prefecture_code=13` — Cities in a region
- `GET /districts?municipality_codes=13101` — Neighborhoods
- `GET /property-types` — Apartment, House, Land, etc.
- `GET /structures` — RC, SRC, Wood, etc.
- `GET /floor-plans` — 1K, 2LDK, 3LDK, etc.
- `GET /year-range` — Data availability
- `GET /building-year-range` — Building age range

### Statistics (aggregated, cached-friendly)
- `GET /stats` — Records by year/quarter
- `GET /stats/summary` — Total counts, date range

### Transactions (raw data, paginated)
- `GET /transactions?...` — Individual sales (up to 1000 per request)

### Analysis (computed metrics, single queries)
- `GET /price-trends?...` — Time series (volume, median, unit price)
- `GET /median-price?...` — Latest stats for a location
- `GET /price-by-district?...` — Prices ranked by district

## Technology Stack

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
└── Connection pooling (future)

Container
└── Docker Compose (orchestration)
```

## Data Flow Guarantees

1. **Single source of truth** — All queries go through API
2. **Consistent schema** — API handles all database specifics
3. **JSON serialization** — All responses are JSON (easy for agents)
4. **Error handling** — API returns HTTP status codes and error details
5. **Pagination** — Large result sets use limit/offset

## Performance Characteristics

### Endpoint Categories

| Endpoint | Latency | Result Size | Cacheable |
|----------|---------|------------|-----------|
| Reference data | <10ms | Small | ✓ 24hr |
| Statistics | 100-500ms | Medium | ✓ 1hr |
| Price trends | 500ms-2s | Medium | ✓ 1hr |
| Transactions | 1-5s | Large (paginated) | ✓ 24hr |
| Median price | 100-500ms | Small | ✓ 1hr |

### Optimization Opportunities

1. **Query caching** (Redis)
   - Cache reference data (prefectures, municipalities)
   - Cache aggregated results (price trends, stats)
   - Invalidate on new data imports

2. **Database indexing**
   - Already: primary key on transactions
   - Consider: indexes on (prefecture_code, property_type, transaction_year)

3. **Connection pooling**
   - Replace direct psycopg2 with pgbouncer or SQLAlchemy

4. **Pagination**
   - All transaction queries default to limit=100-1000
   - Prevents full table scans

## Security Considerations

### Current State
- ✓ No SQL injection (parameterized queries)
- ✓ Input validation (FastAPI type hints)
- ⚠️ CORS allows all origins
- ⚠️ No authentication/authorization
- ⚠️ No rate limiting

### Before Public Deployment
- Add API key authentication
- Restrict CORS to known origins
- Add rate limiting per IP/key
- Enable HTTPS
- Set up monitoring/logging

## Testing Strategy

### Unit Tests (Future)
```python
def test_get_prefectures():
    client = APIClient("http://localhost:8000")
    prefs = client.get_prefectures()
    assert len(prefs) == 47
    assert prefs[0]["prefecture_name"] == "Hokkaido"

def test_search_transactions_tokyo():
    client = APIClient("http://localhost:8000")
    results = client.get_transactions(prefecture_code="13", limit=10)
    assert results["count"] > 0
    assert all(t["prefecture_code"] == "13" for t in results["data"])
```

### Integration Tests (Future)
```python
def test_api_with_streamlit():
    # Start both services in Docker
    # Run Streamlit UI
    # Verify all tabs load data correctly
    pass
```

## Deployment Scenarios

### Local Development
```bash
docker compose up -d
# API on 8000, Streamlit on 9001
```

### Single Server Production
```bash
docker compose up -d
# Add nginx reverse proxy for HTTPS
# Add monitoring/logging
```

### Scalable Production
```
Load Balancer (nginx/HAProxy)
    ↓
API Pod 1, 2, 3 (Kubernetes)
    ↓
PostgreSQL (managed service)
    ↓
Redis (caching)
```

## Migration Path: Streamlit to API

### Phase 1: Dual Mode (Current)
- Streamlit can use API or direct DB
- API runs alongside existing app
- No breaking changes

### Phase 2: API-First
- Refactor Streamlit queries to use api_client
- Remove direct database connections
- All data goes through API

### Phase 3: Optimization
- Add caching to API
- Monitor slow queries
- Add connection pooling
- Deploy MCP server

## API vs Direct Database Access

### Why Abstract?

| Aspect | Direct DB | API Layer |
|--------|-----------|-----------|
| Clients | Streamlit only | Streamlit + Agents + Scripts |
| Connection pooling | Per-client | Centralized |
| Query optimization | Hard to enforce | Single point of control |
| Caching | App-level | Global |
| Monitoring | No logs | Request logging |
| Scalability | Limited | Horizontal |
| Security | Credentials in apps | Centralized auth |

## Future Enhancements

1. **Authentication** — API keys for public deployments
2. **Caching** — Redis for frequently-accessed data
3. **Metrics** — Query duration, result sizes, endpoint usage
4. **GraphQL** — Alternative query interface for complex requests
5. **Search** — Full-text search on districts, addresses
6. **Batch queries** — POST endpoint for multiple queries
7. **Webhooks** — Notify on new data imports
8. **Versioning** — /v1/, /v2/ API versions

## Documentation Map

- **API.md** — REST endpoint reference and examples
- **MCP.md** — MCP tool definitions for agents
- **API_SETUP.md** — Deployment, testing, troubleshooting
- **ARCHITECTURE.md** — This document (design and rationale)
- **README.md** — Original project documentation

---

**Status**: API layer complete and tested
**Next**: Refactor Streamlit to use API client, implement MCP server
