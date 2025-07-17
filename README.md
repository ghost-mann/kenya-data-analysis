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

What is a VIEW?
A view is like a "virtual table" - it's a saved query that looks like a table but doesn't store data itself.
Your food_prices_view does this:
sqlCREATE VIEW food_prices_view AS
SELECT 
    d.date_value,      -- Gets actual date from dim_date table
    l.region,          -- Gets region name from dim_location table
    l.admin1,          -- Gets admin1 from dim_location table
    l.admin2,          -- Gets admin2 from dim_location table
    l.market,          -- Gets market name from dim_location table
    c.category,        -- Gets category from dim_commodity table
    c.commodity_name,  -- Gets commodity name from dim_commodity table
    c.unit,            -- Gets unit from dim_commodity table
    mt.market_type,    -- Gets market type from dim_market_type table
    f.price_kes,       -- Gets price in KES from fact table
    f.price_usd        -- Gets price in USD from fact table
FROM fact_food_prices f
JOIN dim_date d ON f.date_id = d.date_id
JOIN dim_location l ON f.location_id = l.location_id
JOIN dim_commodity c ON f.commodity_id = c.commodity_id
JOIN dim_market_type mt ON f.market_type_id = mt.market_type_id;
What it does:

Joins all your tables together automatically
Converts ID numbers back to readable names (e.g., location_id → actual region name)
Creates a "flat" table that looks like your original CSV but is stored efficiently

Now instead of writing complex JOINs, you can simply:
sqlSELECT * FROM food_prices_view WHERE region = 'North Eastern';
SELECT commodity_name, AVG(price_kes) FROM food_prices_view GROUP BY commodity_name;

https://claude.ai/public/artifacts/4694eccb-582a-408e-8f21-410b827d6202