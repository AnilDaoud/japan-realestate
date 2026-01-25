-- MLIT Real Estate Transaction Database Schema (Optimized)
-- =========================================================
-- This schema is optimized for the Streamlit dashboard app.
-- Key optimizations:
--   1. Composite indexes matching actual query patterns
--   2. Covering indexes to avoid table lookups
--   3. Partial indexes for common filter conditions
--   4. Materialized views for expensive aggregations
--   5. Removed unused columns (walk_minutes, building_floors have 0% data)

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- =============================================================================
-- REFERENCE TABLES
-- =============================================================================

CREATE TABLE prefectures (
    code        CHAR(2) PRIMARY KEY,
    name_ja     VARCHAR(10) NOT NULL,
    name_en     VARCHAR(20) NOT NULL
);

CREATE TABLE municipalities (
    code            CHAR(5) PRIMARY KEY,
    prefecture_code CHAR(2) NOT NULL REFERENCES prefectures(code),
    name_ja         VARCHAR(50) NOT NULL,
    name_en         VARCHAR(100),
    UNIQUE(prefecture_code, name_ja)
);

CREATE INDEX idx_municipalities_prefecture ON municipalities(prefecture_code);

-- Train Lines
CREATE TABLE train_lines (
    id          SERIAL PRIMARY KEY,
    name_ja     VARCHAR(100) NOT NULL,
    name_en     VARCHAR(100),
    UNIQUE(name_ja)
);

-- Train Stations
CREATE TABLE stations (
    code        VARCHAR(10) PRIMARY KEY,
    name_ja     VARCHAR(100) NOT NULL,
    name_en     VARCHAR(100),
    municipality_code CHAR(5) REFERENCES municipalities(code),
    latitude    DECIMAL(9, 6),
    longitude   DECIMAL(9, 6)
);

CREATE INDEX idx_stations_municipality ON stations(municipality_code);
CREATE INDEX idx_stations_name ON stations USING gin(name_ja gin_trgm_ops);

-- Station-Line junction
CREATE TABLE station_lines (
    station_code VARCHAR(10) REFERENCES stations(code),
    line_id     INTEGER REFERENCES train_lines(id),
    PRIMARY KEY (station_code, line_id)
);

-- Property Types
CREATE TABLE property_types (
    id          SERIAL PRIMARY KEY,
    name_ja     VARCHAR(100) NOT NULL,
    name_en     VARCHAR(100) NOT NULL,
    category    VARCHAR(20),
    UNIQUE(name_en)
);

INSERT INTO property_types (name_ja, name_en, category) VALUES
    ('中古マンション等', 'Pre-owned Condominiums', 'residential'),
    ('宅地(土地)', 'Residential Land', 'land'),
    ('宅地(土地と建物)', 'Residential Land and Building', 'residential'),
    ('農地', 'Agricultural Land', 'land'),
    ('林地', 'Forest Land', 'land'),
    ('中古住宅', 'Pre-owned House', 'residential'),
    ('事務所', 'Office', 'commercial'),
    ('店舗', 'Shop', 'commercial'),
    ('倉庫', 'Warehouse', 'commercial'),
    ('工場', 'Factory', 'commercial')
ON CONFLICT (name_en) DO NOTHING;

-- FX Rates for currency conversion
CREATE TABLE fx_rates (
    currency    VARCHAR(3) NOT NULL,
    year        SMALLINT NOT NULL,
    quarter     SMALLINT NOT NULL,
    rate        DECIMAL(12, 8) NOT NULL,
    rate_date   DATE,
    updated_at  TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (currency, year, quarter)
);

CREATE INDEX idx_fx_rates_lookup ON fx_rates(year, quarter);

-- =============================================================================
-- MAIN TRANSACTION TABLE
-- =============================================================================

CREATE TABLE transactions (
    id                  BIGSERIAL PRIMARY KEY,

    -- Source tracking
    source_hash         VARCHAR(64) UNIQUE,
    price_classification CHAR(2),

    -- Location
    prefecture_code     CHAR(2) NOT NULL REFERENCES prefectures(code),
    municipality_code   CHAR(5) REFERENCES municipalities(code),
    district_name       TEXT,
    nearest_station_code VARCHAR(10) REFERENCES stations(code),

    -- Property details
    property_type_id    INTEGER REFERENCES property_types(id),
    property_type_raw   TEXT,

    -- Pricing (unit_price computed from trade_price / area_m2)
    trade_price         BIGINT,
    unit_price          INTEGER,
    price_per_tsubo     INTEGER,

    -- Size
    area_m2             DECIMAL(10, 2),
    total_floor_area_m2 DECIMAL(10, 2),
    balcony_area_m2     DECIMAL(8, 2),
    floor_plan          TEXT,

    -- Building info
    building_year       SMALLINT,
    building_age        SMALLINT,
    structure           TEXT,
    floor_number        SMALLINT,

    -- Land details
    land_shape          TEXT,
    frontage_m          DECIMAL(6, 2),
    road_direction      TEXT,
    road_type           TEXT,
    road_width_m        DECIMAL(5, 2),

    -- Zoning
    city_planning       TEXT,
    coverage_ratio      SMALLINT,
    floor_area_ratio    SMALLINT,

    -- Transaction info
    transaction_year    SMALLINT NOT NULL,
    transaction_quarter SMALLINT,
    transaction_period  TEXT,
    renovation          TEXT,
    remarks             TEXT,

    -- Computed fields
    price_category      VARCHAR(20),
    size_category       VARCHAR(20),
    age_category        VARCHAR(20),

    -- Timestamps
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

-- =============================================================================
-- OPTIMIZED INDEXES
-- =============================================================================

-- Primary query pattern: Filter by prefecture + municipality + year + unit_price valid
-- This covers: main charts, cohorts, ward comparison, valuation
CREATE INDEX idx_tx_main_filter ON transactions(
    prefecture_code,
    municipality_code,
    transaction_year
) WHERE unit_price IS NOT NULL AND unit_price > 0 AND unit_price < 50000000;

-- For aggregation queries (price trends) - include columns for covering index
CREATE INDEX idx_tx_trends ON transactions(
    prefecture_code,
    municipality_code,
    transaction_year,
    transaction_quarter
) INCLUDE (unit_price, trade_price)
WHERE unit_price IS NOT NULL AND unit_price > 0 AND unit_price < 50000000;

-- For property type filtering (common in valuation)
CREATE INDEX idx_tx_property_type_raw ON transactions(property_type_raw);

-- For property type + location (valuation queries)
CREATE INDEX idx_tx_valuation ON transactions(
    municipality_code,
    property_type_raw,
    transaction_year
) INCLUDE (unit_price, trade_price, area_m2, building_year, floor_plan, district_name)
WHERE unit_price IS NOT NULL AND unit_price > 0;

-- For building age cohort analysis
CREATE INDEX idx_tx_cohort ON transactions(
    prefecture_code,
    municipality_code,
    transaction_year,
    building_year
) INCLUDE (unit_price, transaction_quarter)
WHERE unit_price IS NOT NULL
  AND unit_price > 0
  AND unit_price < 50000000
  AND building_year IS NOT NULL;

-- For scatter plot queries (building_age calculations)
CREATE INDEX idx_tx_scatter ON transactions(
    prefecture_code,
    municipality_code
) INCLUDE (transaction_year, unit_price, trade_price, area_m2, building_year, structure, floor_plan)
WHERE unit_price IS NOT NULL AND unit_price > 0;

-- For dropdown population (distinct values)
CREATE INDEX idx_tx_structure ON transactions(structure) WHERE structure IS NOT NULL;
CREATE INDEX idx_tx_floor_plan ON transactions(floor_plan) WHERE floor_plan IS NOT NULL;
CREATE INDEX idx_tx_district ON transactions(municipality_code, district_name) WHERE district_name IS NOT NULL;

-- For year range queries
CREATE INDEX idx_tx_year ON transactions(transaction_year);
CREATE INDEX idx_tx_building_year ON transactions(building_year) WHERE building_year IS NOT NULL AND building_year > 1900;

-- For price/area range filters
CREATE INDEX idx_tx_trade_price ON transactions(trade_price) WHERE trade_price IS NOT NULL;
CREATE INDEX idx_tx_area ON transactions(area_m2) WHERE area_m2 IS NOT NULL;
CREATE INDEX idx_tx_unit_price ON transactions(unit_price) WHERE unit_price IS NOT NULL AND unit_price > 0;

-- =============================================================================
-- HELPER FUNCTIONS
-- =============================================================================

CREATE OR REPLACE FUNCTION categorize_price(price BIGINT)
RETURNS VARCHAR(20) AS $$
BEGIN
    RETURN CASE
        WHEN price IS NULL THEN NULL
        WHEN price < 10000000 THEN '<10M'
        WHEN price < 20000000 THEN '10-20M'
        WHEN price < 30000000 THEN '20-30M'
        WHEN price < 50000000 THEN '30-50M'
        WHEN price < 80000000 THEN '50-80M'
        WHEN price < 100000000 THEN '80-100M'
        WHEN price < 200000000 THEN '100-200M'
        ELSE '200M+'
    END;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION categorize_area(area DECIMAL)
RETURNS VARCHAR(20) AS $$
BEGIN
    RETURN CASE
        WHEN area IS NULL THEN NULL
        WHEN area < 30 THEN '<30m2'
        WHEN area < 50 THEN '30-50m2'
        WHEN area < 70 THEN '50-70m2'
        WHEN area < 100 THEN '70-100m2'
        WHEN area < 150 THEN '100-150m2'
        ELSE '150m2+'
    END;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION categorize_age(age SMALLINT)
RETURNS VARCHAR(20) AS $$
BEGIN
    RETURN CASE
        WHEN age IS NULL THEN NULL
        WHEN age < 5 THEN '<5yr'
        WHEN age < 10 THEN '5-10yr'
        WHEN age < 20 THEN '10-20yr'
        WHEN age < 30 THEN '20-30yr'
        ELSE '30yr+'
    END;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- =============================================================================
-- TRIGGER FOR COMPUTED FIELDS
-- =============================================================================

CREATE OR REPLACE FUNCTION update_computed_fields()
RETURNS TRIGGER AS $$
BEGIN
    -- Compute unit_price if missing
    IF NEW.unit_price IS NULL AND NEW.trade_price IS NOT NULL AND NEW.area_m2 IS NOT NULL AND NEW.area_m2 > 0 THEN
        NEW.unit_price := (NEW.trade_price / NEW.area_m2)::INTEGER;
    END IF;

    NEW.price_category := categorize_price(NEW.trade_price);
    NEW.size_category := categorize_area(NEW.area_m2);

    IF NEW.building_year IS NOT NULL AND NEW.transaction_year IS NOT NULL THEN
        NEW.building_age := NEW.transaction_year - NEW.building_year;
    END IF;

    NEW.age_category := categorize_age(NEW.building_age);
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_update_computed_fields
    BEFORE INSERT OR UPDATE ON transactions
    FOR EACH ROW
    EXECUTE FUNCTION update_computed_fields();

-- =============================================================================
-- MATERIALIZED VIEWS FOR DASHBOARD
-- =============================================================================

-- Price trends by quarter (used by main chart)
CREATE MATERIALIZED VIEW mv_price_trends AS
SELECT
    prefecture_code,
    municipality_code,
    property_type_raw,
    transaction_year,
    transaction_quarter,
    COUNT(*) as transaction_count,
    ROUND(AVG(unit_price)) as avg_price_m2,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY unit_price)::INTEGER as median_price_m2,
    ROUND(AVG(trade_price)) as avg_price
FROM transactions
WHERE unit_price IS NOT NULL
  AND unit_price > 0
  AND unit_price < 50000000
GROUP BY prefecture_code, municipality_code, property_type_raw, transaction_year, transaction_quarter;

CREATE UNIQUE INDEX idx_mv_price_trends ON mv_price_trends(
    prefecture_code, municipality_code, property_type_raw, transaction_year, transaction_quarter
);

-- Ward comparison stats (used by ward comparison tab)
CREATE MATERIALIZED VIEW mv_ward_stats AS
SELECT
    m.code as municipality_code,
    m.name_en as ward,
    t.prefecture_code,
    t.property_type_raw,
    t.transaction_year,
    COUNT(*) as transactions,
    ROUND(AVG(t.unit_price)) as avg_price_m2,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.unit_price)::INTEGER as median_price_m2
FROM transactions t
JOIN municipalities m ON t.municipality_code = m.code
WHERE t.unit_price IS NOT NULL
  AND t.unit_price > 0
  AND t.unit_price < 50000000
GROUP BY m.code, m.name_en, t.prefecture_code, t.property_type_raw, t.transaction_year;

CREATE UNIQUE INDEX idx_mv_ward_stats ON mv_ward_stats(
    municipality_code, prefecture_code, property_type_raw, transaction_year
);

-- Dropdown options cache (for faster initial load)
CREATE MATERIALIZED VIEW mv_filter_options AS
SELECT
    'prefecture' as filter_type,
    p.code as code,
    p.name_en as name
FROM prefectures p
WHERE EXISTS (SELECT 1 FROM transactions t WHERE t.prefecture_code = p.code)

UNION ALL

SELECT
    'property_type' as filter_type,
    NULL as code,
    property_type_raw as name
FROM transactions
WHERE property_type_raw IS NOT NULL
GROUP BY property_type_raw

UNION ALL

SELECT
    'structure' as filter_type,
    NULL as code,
    structure as name
FROM transactions
WHERE structure IS NOT NULL AND structure != ''
GROUP BY structure

UNION ALL

SELECT
    'floor_plan' as filter_type,
    NULL as code,
    floor_plan as name
FROM transactions
WHERE floor_plan IS NOT NULL AND floor_plan != ''
GROUP BY floor_plan;

CREATE INDEX idx_mv_filter_options ON mv_filter_options(filter_type);

-- Year ranges (for slider initialization)
CREATE MATERIALIZED VIEW mv_year_ranges AS
SELECT
    MIN(transaction_year) as min_transaction_year,
    MAX(transaction_year) as max_transaction_year,
    MIN(building_year) FILTER (WHERE building_year > 1900) as min_building_year,
    MAX(building_year) FILTER (WHERE building_year > 1900) as max_building_year
FROM transactions;

-- =============================================================================
-- REFRESH FUNCTION
-- =============================================================================

CREATE OR REPLACE FUNCTION refresh_materialized_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_price_trends;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_ward_stats;
    REFRESH MATERIALIZED VIEW mv_filter_options;
    REFRESH MATERIALIZED VIEW mv_year_ranges;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- STATISTICS AND MAINTENANCE
-- =============================================================================

-- Increase statistics target for frequently filtered columns
ALTER TABLE transactions ALTER COLUMN prefecture_code SET STATISTICS 1000;
ALTER TABLE transactions ALTER COLUMN municipality_code SET STATISTICS 1000;
ALTER TABLE transactions ALTER COLUMN property_type_raw SET STATISTICS 500;
ALTER TABLE transactions ALTER COLUMN transaction_year SET STATISTICS 500;
ALTER TABLE transactions ALTER COLUMN unit_price SET STATISTICS 1000;

-- After loading data, run:
-- ANALYZE transactions;
-- VACUUM ANALYZE transactions;
