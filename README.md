#  Data Platform for Analyzing Kenya’s Food Prices and Inflation Trends.

Fact Table = Contains measurable data (metrics/numbers)
Dimension Table = Contains descriptive attributes (who, what, where, when)


### schema
-- Date dimension
CREATE TABLE dim_date (
    date_id SERIAL PRIMARY KEY,
    date_value DATE UNIQUE,
    year INTEGER,
    month INTEGER,
    day INTEGER
);

-- Location dimension
CREATE TABLE dim_location (
    location_id SERIAL PRIMARY KEY,
    region TEXT,
    admin1 TEXT,
    admin2 TEXT,
    market TEXT,
    UNIQUE(region, admin1, admin2, market)
);

-- Commodity dimension
CREATE TABLE dim_commodity (
    commodity_id SERIAL PRIMARY KEY,
    category TEXT,
    commodity_name TEXT,
    unit TEXT,
    UNIQUE(category, commodity_name, unit)
);

-- Market type dimension
CREATE TABLE dim_market_type (
    market_type_id SERIAL PRIMARY KEY,
    market_type TEXT UNIQUE
);

-- 2. Create fact table (main data table)
CREATE TABLE fact_food_prices (
    fact_id SERIAL PRIMARY KEY,
    date_id INTEGER REFERENCES dim_date(date_id),
    location_id INTEGER REFERENCES dim_location(location_id),
    commodity_id INTEGER REFERENCES dim_commodity(commodity_id),
    market_type_id INTEGER REFERENCES dim_market_type(market_type_id),
    price_kes DECIMAL(10,2),
    price_usd DECIMAL(10,4)
);