# MCP (Model Context Protocol) Server for Japan Real Estate Analytics

Allows AI agents and LLM assistants to query and analyze Japanese real estate data via standardized MCP tools.

## Overview

The MCP server exposes the REST API as callable tools that agents can use to:
- Search transactions with complex filters
- Analyze price trends over time
- Compare neighborhoods and districts
- Get market statistics
- Look up geographic hierarchies (prefectures → municipalities → districts)

## Available Tools

### 1. search_transactions
Search Japanese real estate transactions with flexible filtering.

```
Inputs:
  prefecture_code (str, optional): Prefecture code (e.g., "13" for Tokyo)
  municipality_codes (str, optional): Comma-separated codes
  districts (str, optional): Comma-separated district names
  property_types (str, optional): Comma-separated types (Apartment, House, Land)
  year_min (int, optional): Minimum transaction year
  year_max (int, optional): Maximum transaction year
  price_min (float, optional): Minimum price in JPY
  price_max (float, optional): Maximum price in JPY
  area_min (float, optional): Minimum area in m²
  area_max (float, optional): Maximum area in m²
  limit (int, default=100, max=1000): Results per page
  offset (int, default=0): Pagination offset

Output:
  {
    "count": 100,
    "limit": 100,
    "offset": 0,
    "data": [
      {
        "id": 12345,
        "prefecture_name": "Tokyo",
        "municipality_name": "Chiyoda Ward",
        "district": "Marunouchi",
        "property_type": "Apartment",
        "transaction_price": 65000000,
        "area": 72.5,
        "year_built": 2015,
        "structure_type": "RC",
        "floor_plan": "2LDK",
        "transaction_year": 2024,
        "transaction_quarter": 2
      },
      ...
    ]
  }
```

### 2. get_price_trends
Get historical price trends (time series) for a location.

```
Inputs:
  prefecture_code (str, required): Prefecture code
  municipality_codes (str, optional): Comma-separated codes
  districts (str, optional): Comma-separated district names
  property_types (str, optional): Comma-separated types
  frequency (str, default="Quarterly"): "Quarterly" or "Yearly"

Output:
  [
    {
      "transaction_year": 2024,
      "transaction_quarter": 2,
      "volume": 1234,
      "avg_price": 45000000,
      "median_price": 42000000,
      "avg_unit_price": 625000,
      "median_unit_price": 580000
    },
    ...
  ]
```

### 3. get_district_prices
Get median prices by district, ranked from highest to lowest. Useful for comparing neighborhoods.

```
Inputs:
  prefecture_code (str, required): Prefecture code
  municipality_codes (str, optional): Comma-separated codes
  property_types (str, optional): Comma-separated types
  limit (int, default=50, max=500): Top N districts to return

Output:
  [
    {
      "district": "Shibuya",
      "municipality_name": "Shibuya Ward",
      "transaction_count": 456,
      "median_price": 72000000,
      "median_unit_price": 850000,
      "avg_price": 75000000
    },
    ...
  ]
```

### 4. get_median_price
Get overall median and average prices for a location.

```
Inputs:
  prefecture_code (str, required): Prefecture code
  municipality_codes (str, optional): Comma-separated codes
  property_types (str, optional): Comma-separated types

Output:
  {
    "median_price": 45000000,
    "median_unit_price": 625000,
    "avg_price": 48000000,
    "transaction_count": 5432
  }
```

### 5. list_prefectures
Get all Japanese prefectures.

```
Output:
  [
    {"prefecture_code": "01", "prefecture_name": "Hokkaido"},
    {"prefecture_code": "13", "prefecture_name": "Tokyo"},
    {"prefecture_code": "26", "prefecture_name": "Kyoto"},
    ...
  ]
```

### 6. list_municipalities
Get cities/wards for a prefecture.

```
Inputs:
  prefecture_code (str, required): Prefecture code

Output:
  [
    {"municipality_code": "13101", "municipality_name": "Chiyoda Ward", "prefecture_code": "13"},
    {"municipality_code": "13102", "municipality_name": "Chuo Ward", "prefecture_code": "13"},
    ...
  ]
```

### 7. list_property_types
Get all available property types.

```
Output:
  ["Apartment", "House", "Land", "Business Building", ...]
```

### 8. get_statistics
Get overall database statistics.

```
Output:
  {
    "total_records": 6100000,
    "prefectures": 47,
    "municipalities": 1800,
    "districts": 28000,
    "earliest_year": 2005,
    "latest_year": 2024,
    "earliest_quarter": 1,
    "latest_quarter": 3
  }
```

## Usage Examples

### Example 1: Find affordable apartments in Tokyo

```
Tool: search_transactions
Inputs:
  prefecture_code: "13"
  property_types: "Apartment"
  price_min: 30000000
  price_max: 50000000
  limit: 50
```

### Example 2: Analyze Kyoto district prices

```
Tool: get_district_prices
Inputs:
  prefecture_code: "26"
  property_types: "House"
  limit: 20
```

### Example 3: Track Tokyo condo price trends

```
Tool: get_price_trends
Inputs:
  prefecture_code: "13"
  property_types: "Apartment"
  frequency: "Yearly"
```

### Example 4: Compare prices in Shibuya vs Shinjuku

```
Tool: search_transactions
Inputs:
  districts: "Shibuya,Shinjuku"
  property_types: "Apartment"
  year_min: 2023
  limit: 100
```

## Integration Guide

### With Claude API

```python
from anthropic import Anthropic

client = Anthropic()

tools = [
    {
        "name": "search_transactions",
        "description": "Search Japanese real estate transactions",
        "input_schema": {
            # ... (see mcp_server.py TOOLS definition)
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

### With MCP SDK (Upcoming)

When Anthropic releases the official MCP SDK:

```python
from mcp import Server
from mcp_server import TOOLS

server = Server("japan-realestate")

# Register tools with server
for tool in TOOLS:
    server.register_tool(tool)

# Start server
server.run()
```

### Standalone HTTP

If hosting as a standalone MCP server over HTTP/SSE:

```bash
# Start MCP server on port 3000
python -m mcp_server.main --port 3000

# Claude Code or other clients connect to:
# http://localhost:3000
```

## Example Agent Prompts

### Real Estate Analyst

> "You are a Japanese real estate analyst. Use the available tools to analyze property markets and provide insights. When asked about a location, always check recent transaction data, price trends, and compare with neighboring districts."

### Investment Advisor

> "You help investors find undervalued properties in Japan. Always search recent transactions, compare price trends over the last 3 years, and highlight districts with rising prices or good value."

### Data Explorer

> "Help users explore Japanese real estate data. Start by listing available prefectures, then dig into specific locations they're interested in. Always show median prices, price trends, and top districts."

## Development Notes

### Adding New Tools

1. Add tool definition to `TOOLS` list in `mcp_server.py`
2. Implement async function that calls the REST API
3. Integrate with MCP SDK when available
4. Update this documentation with examples

### Filtering Best Practices

- Always start with prefecture_code (required for most tools)
- Use municipality_codes to narrow down to specific cities/wards
- Use districts for detailed neighborhood analysis
- Combine property_types with price ranges for targeted searches
- Use year ranges to analyze trends or focus on recent data

### Performance Considerations

- search_transactions can return up to 1000 items; use limit/offset for pagination
- get_price_trends is fast (aggregated data)
- get_district_prices can return many districts; use limit parameter
- For large searches, filter aggressively on location and date

## Deployment

### Local Development

```bash
pip install httpx  # For async HTTP calls
python mcp_server.py
```

### Production (with MCP SDK)

```bash
# Containerized
docker build -f Dockerfile.mcp -t japan-realestate-mcp .
docker run -p 3000:3000 japan-realestate-mcp

# Or as library
pip install japan-realestate-mcp
```

## Data Interpretation

### Price Fields

- `transaction_price` — Total sale price in Japanese Yen (JPY)
- `median_unit_price` — Price per m² (useful for comparing across property sizes)
- `median_price` — Median transaction price for the area

### Area Units

- All areas in m² (square meters)
- 1 tsubo ≈ 3.31 m² (traditional Japanese unit)

### Property Types

Common types: Apartment (Mansion), House, Land, Business Building, Factory, Farm

### Structure Types

- RC — Reinforced Concrete
- SRC — Steel Reinforced Concrete
- Wood — Wooden construction

### Floor Plans

- K — Kitchen
- LDK — Living/Dining/Kitchen
- Examples: 1K, 2LDK, 3LDK

## Troubleshooting

### "No results found"

Check that:
1. Prefecture code exists (use list_prefectures)
2. Municipality/district names are spelled correctly
3. Date range contains data (use get_statistics to check year range)
4. Property type matches available types

### "Connection refused"

Ensure:
1. REST API is running on port 8000
2. PostgreSQL database is accessible
3. Check API health: GET http://localhost:8000/health

### Slow queries

1. Add more specific filters (location, date, property type)
2. Reduce limit parameter
3. Use get_price_trends instead of search_transactions for trends
4. Use get_district_prices for neighborhood comparisons

## Future Enhancements

- Valuation estimation tool
- Depreciation curve analysis
- Seasonal trends
- Auction vs. non-arms-length transaction filtering
- Zoning/density analysis
- Japanese language support for districts/municipalities
