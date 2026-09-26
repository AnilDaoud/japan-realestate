# Using Japan Real Estate API with Claude

Example of how to integrate this API with Claude (Claude API or Claude Code) for real estate analysis.

## Setup

1. Start the API:
   ```bash
   docker compose up -d api
   ```

2. The API will be available at `http://localhost:8000`

3. Interactive docs: `http://localhost:8000/docs`

## Example 1: Claude API with Tool Use

Using the Anthropic Python SDK to let Claude query the API:

```python
import anthropic
import json
import httpx
from typing import Any

client = anthropic.Anthropic()

# Define tools for Claude to use
tools = [
    {
        "name": "search_transactions",
        "description": "Search Japanese real estate transactions with filters for location, price, area, year",
        "input_schema": {
            "type": "object",
            "properties": {
                "prefecture_code": {
                    "type": "string",
                    "description": "Prefecture code like '13' for Tokyo, '26' for Kyoto"
                },
                "property_types": {
                    "type": "string",
                    "description": "Comma-separated property types like 'Apartment,House'"
                },
                "price_min": {
                    "type": "number",
                    "description": "Minimum price in JPY"
                },
                "price_max": {
                    "type": "number",
                    "description": "Maximum price in JPY"
                },
                "year_min": {
                    "type": "integer",
                    "description": "Minimum transaction year"
                },
                "year_max": {
                    "type": "integer",
                    "description": "Maximum transaction year"
                },
                "limit": {
                    "type": "integer",
                    "description": "Number of results (default 100)",
                    "default": 100
                }
            }
        }
    },
    {
        "name": "get_price_trends",
        "description": "Get historical price trends (time series) for a location",
        "input_schema": {
            "type": "object",
            "properties": {
                "prefecture_code": {
                    "type": "string",
                    "description": "Prefecture code"
                },
                "frequency": {
                    "type": "string",
                    "enum": ["Quarterly", "Yearly"],
                    "description": "Time series frequency",
                    "default": "Yearly"
                }
            },
            "required": ["prefecture_code"]
        }
    },
    {
        "name": "get_district_prices",
        "description": "Get median prices by district, ranked highest to lowest",
        "input_schema": {
            "type": "object",
            "properties": {
                "prefecture_code": {
                    "type": "string",
                    "description": "Prefecture code"
                },
                "limit": {
                    "type": "integer",
                    "description": "Number of top districts",
                    "default": 50
                }
            },
            "required": ["prefecture_code"]
        }
    }
]

def call_api(tool_name: str, tool_input: dict) -> Any:
    """Call the Japan Real Estate API based on tool name"""
    params = {k: v for k, v in tool_input.items() if v is not None}
    
    if tool_name == "search_transactions":
        url = "http://localhost:8000/transactions"
    elif tool_name == "get_price_trends":
        url = "http://localhost:8000/price-trends"
    elif tool_name == "get_district_prices":
        url = "http://localhost:8000/price-by-district"
    else:
        return {"error": f"Unknown tool: {tool_name}"}
    
    response = httpx.get(url, params=params)
    return response.json()

def run_claude_analysis(user_query: str) -> str:
    """Run an analysis with Claude using tool use"""
    
    messages = [
        {"role": "user", "content": user_query}
    ]
    
    system = """You are a Japanese real estate analyst with access to transaction data.
Use the available tools to research questions about the market. Always cite data
when making claims and provide specific numbers. Be thoughtful about what filters
and analysis would best answer the user's question."""
    
    while True:
        response = client.messages.create(
            model="claude-opus-5-5",
            max_tokens=2048,
            system=system,
            tools=tools,
            messages=messages
        )
        
        # Check if we're done
        if response.stop_reason == "end_turn":
            # Extract final text response
            for block in response.content:
                if hasattr(block, "text"):
                    return block.text
            return "No response generated"
        
        # Process tool calls
        if response.stop_reason == "tool_use":
            # Add assistant's response to messages
            messages.append({"role": "assistant", "content": response.content})
            
            # Process each tool call
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    print(f"Calling tool: {block.name}")
                    print(f"Input: {json.dumps(block.input, indent=2)}")
                    
                    result = call_api(block.name, block.input)
                    
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": json.dumps(result)
                    })
            
            # Add tool results to messages
            messages.append({"role": "user", "content": tool_results})
        else:
            # Unexpected stop reason
            return f"Unexpected stop reason: {response.stop_reason}"


# Example usage
if __name__ == "__main__":
    # Query 1: Market analysis
    result = run_claude_analysis(
        "What are the most expensive neighborhoods in Tokyo for apartments? "
        "Show me the top 5 with median prices."
    )
    print("Analysis 1:")
    print(result)
    print("\n" + "="*80 + "\n")
    
    # Query 2: Investment opportunity
    result = run_claude_analysis(
        "Find affordable houses in Kyoto under ¥30 million from 2024. "
        "How many transactions were there?"
    )
    print("Analysis 2:")
    print(result)
    print("\n" + "="*80 + "\n")
    
    # Query 3: Trend analysis
    result = run_claude_analysis(
        "Show me Tokyo apartment price trends over the last 5 years. "
        "Are prices rising or falling?"
    )
    print("Analysis 3:")
    print(result)
```

## Example 2: Simple Python Script

Using the Python client directly (no Claude):

```python
from api_client import APIClient
import pandas as pd

client = APIClient("http://localhost:8000")

# Get statistics
stats = client.get_stats_summary()
print(f"Database: {stats['total_records']:,} transactions")
print(f"Date range: {stats['earliest_year']} to {stats['latest_year']}")
print(f"Prefectures: {stats['prefectures']}")

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
trends = client.get_price_trends(
    prefecture_code="13",
    frequency="Yearly"
)

df_trends = pd.DataFrame(trends)
print("\nTokyo Apartment Price Trends (Yearly):")
print(df_trends[["transaction_year", "volume", "median_price"]].to_string(index=False))

# Calculate price change
if len(df_trends) >= 2:
    latest = df_trends.iloc[0]
    previous = df_trends.iloc[1]
    pct_change = ((latest["median_price"] - previous["median_price"]) / previous["median_price"]) * 100
    print(f"\nYear-over-year change: {pct_change:+.1f}%")
```

## Example 3: Claude Code Integration

Using this directly in Claude Code:

```python
# In your Claude Code session
from api_client import APIClient

client = APIClient("http://localhost:8000")

# Get prefectures
prefectures = client.get_prefectures()
print(f"Available prefectures: {len(prefectures)}")

# Look up Tokyo (code 13)
tokyo_muni = client.get_municipalities("13")
print(f"Tokyo municipalities: {len(tokyo_muni)}")
for m in tokyo_muni[:5]:
    print(f"  {m['municipality_name']}")

# Get apartment prices in Shibuya
shibuya_prices = client.get_transactions(
    municipality_codes="13104",  # Shibuya
    property_types="Apartment",
    year_min=2024,
    limit=20
)

print(f"\nShibuya apartment transactions (2024): {shibuya_prices['count']}")
for txn in shibuya_prices['data'][:3]:
    print(f"  ¥{txn['transaction_price']:,} for {txn['area']} m²")
```

## Example 4: Using with Claude Code for Analysis

```python
# Analyze Tokyo vs Kyoto real estate markets

from api_client import APIClient
import json

client = APIClient("http://localhost:8000")

def analyze_prefecture(code, name):
    """Analyze a prefecture's market"""
    
    # Get median price
    price_stats = client.get_median_price(prefecture_code=code)
    
    # Get price trends
    trends = client.get_price_trends(
        prefecture_code=code,
        frequency="Yearly"
    )
    
    # Get top districts
    districts = client.get_price_by_district(
        prefecture_code=code,
        limit=5
    )
    
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

print("\nTop Tokyo Districts:")
for d in tokyo['top_districts'][:3]:
    print(f"  {d['district']}: ¥{d['median_price']:,}/m²")

print("\nTop Kyoto Districts:")
for d in kyoto['top_districts'][:3]:
    print(f"  {d['district']}: ¥{d['median_price']:,}/m²")
```

## Example 5: Real Estate Investment Query

Asking Claude to find investment opportunities:

```python
# Ask Claude to find undervalued properties

query = """
I'm looking for investment opportunities in Japanese real estate.
Find me:
1. Prefectures where apartment prices have been stable or rising
2. Areas with good transaction volume (more than 100 sales/year)
3. Price ranges between ¥20-50 million
4. Recent data (2024 transactions only)

Provide specific numbers and district recommendations.
"""

# Claude would use the tools to:
# 1. Call get_price_trends for major prefectures (Yearly frequency)
# 2. Call search_transactions with specific price and year filters
# 3. Get district prices to compare
# 4. Summarize findings with concrete numbers
```

## API Rate Limits & Performance

- No built-in rate limiting (yet)
- Typical response times:
  - Reference data: <10ms
  - Transactions: 1-5 seconds
  - Price trends: 500ms-2s
  - District prices: 1-2 seconds

For production use with Claude, consider:
- Caching frequently-used queries
- Limiting result sets with `limit` parameter
- Using yearly frequency for trends instead of quarterly

## Deployment Considerations

### Local Development
```bash
docker compose up -d api
# API at http://localhost:8000
```

### Production
- Use environment variable: `API_BASE_URL=https://api.realestate.example.com`
- Add authentication if exposing publicly
- Set up monitoring/logging
- Consider caching layer (Redis)

## Next Steps

1. **Test with Claude Code**: Try the examples above
2. **Refactor Streamlit**: Migrate app.py to use api_client
3. **Deploy MCP Server**: Wrap API as Model Context Protocol
4. **Add Caching**: Redis for frequently-accessed data
5. **Monitor**: Track API usage and performance
