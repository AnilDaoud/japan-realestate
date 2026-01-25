# Japan Real Estate Analytics

A Streamlit dashboard for exploring Japanese real estate transaction data from the MLIT (Ministry of Land, Infrastructure, Transport and Tourism) Real Estate Information Library.

![Python](https://img.shields.io/badge/python-3.8+-blue.svg)
![Streamlit](https://img.shields.io/badge/streamlit-1.0+-red.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

## Features

- **6.1M+ transactions** from all 47 prefectures dating back to 2005
- **Interactive charts**: Time series, histograms, scatter plots with regression
- **Price comparisons**: By ward/city with bar charts and treemaps
- **Age cohort analysis**: Track how prices evolve for buildings of different ages
- **Property valuation**: Estimate values, check listings, track depreciation
- **Multi-currency support**: JPY, USD, EUR, GBP with historical FX rates
- **Flexible units**: Price per m² or per tsubo

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Set Up Database

```bash
# Create PostgreSQL database
createdb mlit_realestate

# Apply schema
psql mlit_realestate < dbutils/schema_optimized.sql

# Ingest data (requires MLIT API key)
export MLIT_API_KEY="your-api-key"
python dbutils/ingest_data.py --full
```

### 3. Run the Dashboard

```bash
export DATABASE_URL="postgresql://localhost/mlit_realestate"
streamlit run app.py
```

Open http://localhost:8501 in your browser.

## Getting an API Key

Apply for a free MLIT API key at: https://www.reinfolib.mlit.go.jp/api/request/

You'll receive the key via email in 2-3 days.

## Dashboard Tabs

| Tab | Description |
|-----|-------------|
| **Charts** | Time series, histogram, and scatter plots of price trends |
| **Map** | Price comparison by ward/city with visualizations |
| **Age Cohorts** | Track prices for buildings of different ages over time |
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

## License

MIT

## Contributing

An example instance of this project is hosted at https://anil.diwi.org/japan-realestate/
Issues and pull requests welcome.
