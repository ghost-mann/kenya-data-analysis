import pandas as pd
import knoema
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

# load .env file
load_dotenv()

# import .env values
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_port = os.getenv("DB_PORT")

# db connection string 
conn_string = f'postgresql://{db_user}:{db_password}@{db_host}/{db_name}'
engine = create_engine(conn_string)

def extraction_and_transformation():
    # extraction
    df = pd.read_csv('wfp_food_prices_ken.csv', skiprows=[1])

    # drop rows with null values
    df = df.dropna()

    # remove whitespace from column names
    df.columns = df.columns.str.strip()

    # remove " Kenya" from commodity column if present
    if 'commodity' in df.columns:
        df['commodity'] = df['commodity'].str.replace(r'\s*Kenya\s*', '', regex=True).str.strip()

    # remove duplicate rows
    df.drop_duplicates(inplace=True)

    # reset index
    df.reset_index(drop=True, inplace=True)

    print(df.head())
    return df


# loading the dimension tables

def load_dimensions(df):
    
    # date dimensions
    print("Loading the date dimension...")
    df['date'] = pd.to_datetime(df['date'])
    dates = df[['date']].drop_duplicates()
    dates['date_value'] = dates['date'].dt.date
    dates['year'] = dates['date'].dt.year
    dates['month'] = dates['date'].dt.month
    dates['day'] = dates['date'].dt.day

    # Get existing dates from database
    existing_dates_query = "SELECT date_value FROM dim_date"
    existing_dates = pd.read_sql(existing_dates_query, engine)

    # Filter out already existing dates
    new_dates = dates[~dates['date_value'].isin(existing_dates['date_value'])]

    if not new_dates.empty:
        try:
            new_dates[['date_value', 'year', 'month', 'day']].to_sql(
                'dim_date', engine, if_exists='append', index=False
            )
            print(f"Inserted {len(new_dates)} new date records")
        except Exception as e:
            print(f"Error inserting new dates: {e}")
    else:
        print("No new dates to insert!")

def load_location_dimension(df):
    print("Loading location dimension...")
    
    df['region'] = df['admin1'].str.strip()
    
    location_cols = ['region', 'admin1', 'admin2', 'market']
    locations = df[location_cols].drop_duplicates()

    existing = pd.read_sql("SELECT region, admin1, admin2, market FROM dim_location", engine)
    new_locations = locations.merge(existing, on=location_cols, how='left', indicator=True)
    new_locations = new_locations[new_locations['_merge'] == 'left_only'].drop('_merge', axis=1)

    if not new_locations.empty:
        new_locations.to_sql('dim_location', engine, if_exists='append', index=False)
        print(f"Inserted {len(new_locations)} new location records!")
    else:
        print("No new location records needed!")


def load_commodity_dimension(df):
    print("Loading commodity dimension...")
    commodity_cols = ['category', 'commodity', 'unit']
    commodities = df[commodity_cols].drop_duplicates()
    commodities.rename(columns={'commodity': 'commodity_name'}, inplace=True)

    existing = pd.read_sql("SELECT category, commodity_name, unit FROM dim_commodity", engine)
    new_commodities = commodities.merge(existing, on=['category', 'commodity_name', 'unit'], how='left', indicator=True)
    new_commodities = new_commodities[new_commodities['_merge'] == 'left_only'].drop('_merge', axis=1)

    if not new_commodities.empty:
        new_commodities.to_sql('dim_commodity', engine, if_exists='append', index=False)
        print(f"Inserted {len(new_commodities)} new commodity records")
    else:
        print("No new commodity records needed")

def load_market_type_dimension(df):
    print("Loading market type dimension...")
    types = df[['pricetype']].drop_duplicates().rename(columns={'pricetype': 'market_type'})

    existing = pd.read_sql("SELECT market_type FROM dim_market_type", engine)
    new_types = types[~types['market_type'].isin(existing['market_type'])]

    if not new_types.empty:
        new_types.to_sql('dim_market_type', engine, if_exists='append', index=False)
        print(f"Inserted {len(new_types)} new market types")
    else:
        print("No new market types needed")

def load_fact_table(df):
    print("Loading fact table...")
    
    # Prepare the fact table data
    fact_df = df.copy()
    
    # Convert date to date format for joining
    fact_df['date'] = pd.to_datetime(fact_df['date'])
    fact_df['date_value'] = fact_df['date'].dt.date
    
    # Add region column for location dimension join
    fact_df['region'] = fact_df['admin1'].str.strip()

    
    # Rename columns to match schema
    fact_df = fact_df.rename(columns={
        'commodity': 'commodity_name',
        'price': 'price_kes',
        'usdprice': 'price_usd'
    })
    
    # Get dimension table IDs
    print("Fetching dimension table IDs...")
    
    # Get date dimension IDs
    date_dim = pd.read_sql("SELECT date_id, date_value FROM dim_date", engine)
    
    # Get location dimension IDs
    location_dim = pd.read_sql("""
        SELECT location_id, region, admin1, admin2, market 
        FROM dim_location
    """, engine)
    
    # Get commodity dimension IDs
    commodity_dim = pd.read_sql("""
        SELECT commodity_id, category, commodity_name, unit 
        FROM dim_commodity
    """, engine)
    
    # Get market type dimension IDs
    market_type_dim = pd.read_sql("""
        SELECT market_type_id, market_type 
        FROM dim_market_type
    """, engine)
    
    # Join with dimension tables to get foreign keys
    print("Joining with dimension tables...")
    
    # Debug: Check data before joins
    print(f"Fact table shape before joins: {fact_df.shape}")
    print(f"Sample commodity data: {fact_df[['category', 'commodity_name', 'unit']].head()}")
    print(f"Commodity dimension shape: {commodity_dim.shape}")
    print(f"Sample commodity dimension: {commodity_dim.head()}")
    
    # Join with date dimension
    fact_df = fact_df.merge(
        date_dim, 
        on='date_value', 
        how='left'
    )
    print(f"After date join: {fact_df.shape}")
    
    # Join with location dimension
    fact_df = fact_df.merge(
        location_dim, 
        on=['region', 'admin1', 'admin2', 'market'], 
        how='left'
    )
    print(f"After location join: {fact_df.shape}")
    
    # Join with commodity dimension - add debugging
    print("Checking commodity join keys...")
    commodity_join_keys = ['category', 'commodity_name', 'unit']
    
    # Check if all join keys exist in both dataframes
    for key in commodity_join_keys:
        if key not in fact_df.columns:
            print(f"ERROR: {key} not found in fact_df columns: {fact_df.columns.tolist()}")
        if key not in commodity_dim.columns:
            print(f"ERROR: {key} not found in commodity_dim columns: {commodity_dim.columns.tolist()}")
    
    # Check for exact matches
    fact_commodity_combinations = fact_df[commodity_join_keys].drop_duplicates()
    print(f"Unique commodity combinations in fact data: {len(fact_commodity_combinations)}")
    
    # Check if combinations exist in dimension table
    merged_check = fact_commodity_combinations.merge(
        commodity_dim[commodity_join_keys], 
        on=commodity_join_keys, 
        how='inner'
    )
    print(f"Matching combinations found: {len(merged_check)}")
    
    if len(merged_check) == 0:
        print("No matching commodity combinations found!")
        print("Sample fact combinations:")
        print(fact_commodity_combinations.head())
        print("Sample dimension combinations:")
        print(commodity_dim[commodity_join_keys].head())
    
    fact_df = fact_df.merge(
        commodity_dim, 
        on=commodity_join_keys, 
        how='left'
    )
    print(f"After commodity join: {fact_df.shape}")
    
    # Join with market type dimension
    fact_df = fact_df.merge(
        market_type_dim, 
        left_on='pricetype', 
        right_on='market_type', 
        how='left'
    )
    print(f"After market type join: {fact_df.shape}")
    
    # Select only the columns needed for the fact table
    print(f"Available columns after all joins: {fact_df.columns.tolist()}")
    
    # Handle the commodity_id column name issue
    if 'commodity_id' in fact_df.columns:
        commodity_id_col = 'commodity_id'
    elif 'commodity_id_y' in fact_df.columns:
        commodity_id_col = 'commodity_id_y'
    elif 'commodity_id_x' in fact_df.columns:
        commodity_id_col = 'commodity_id_x'
    else:
        raise ValueError("No commodity_id column found in any expected format")
    
    fact_columns = [
        'date_id', 
        'location_id', 
        commodity_id_col,  # Use the actual column name
        'market_type_id', 
        'price_kes', 
        'price_usd'
    ]
    
    # Check which columns are missing
    missing_columns = [col for col in fact_columns if col not in fact_df.columns]
    if missing_columns:
        print(f"Missing columns: {missing_columns}")
        # Show what columns are actually available
        available_fact_columns = [col for col in fact_columns if col in fact_df.columns]
        print(f"Available fact columns: {available_fact_columns}")
    
    fact_table = fact_df[fact_columns].copy()
    
    # Rename the commodity_id column to the standard name if needed
    if commodity_id_col != 'commodity_id':
        fact_table = fact_table.rename(columns={commodity_id_col: 'commodity_id'})
    
    # Check for any null foreign keys
    null_checks = {
        'date_id': fact_table['date_id'].isnull().sum(),
        'location_id': fact_table['location_id'].isnull().sum(),
        'commodity_id': fact_table['commodity_id'].isnull().sum(),
        'market_type_id': fact_table['market_type_id'].isnull().sum()
    }
    
    print("Null foreign key counts:")
    for key, count in null_checks.items():
        print(f"  {key}: {count}")
    
    # Remove rows with null foreign keys
    initial_count = len(fact_table)
    fact_table = fact_table.dropna(subset=['date_id', 'location_id', 'commodity_id', 'market_type_id'])
    final_count = len(fact_table)
    
    if initial_count != final_count:
        print(f"Removed {initial_count - final_count} rows with null foreign keys")
    
    # Check for existing records to avoid duplicates
    if not fact_table.empty:
        # Create a composite key for duplicate checking
        fact_table['composite_key'] = (
            fact_table['date_id'].astype(str) + '_' +
            fact_table['location_id'].astype(str) + '_' +
            fact_table['commodity_id'].astype(str) + '_' +
            fact_table['market_type_id'].astype(str)
        )
        
        # Get existing composite keys from database
        existing_query = """
            SELECT CONCAT(date_id, '_', location_id, '_', commodity_id, '_', market_type_id) as composite_key
            FROM fact_food_prices
        """
        
        try:
            existing_keys = pd.read_sql(existing_query, engine)
            
            # Filter out existing records
            new_facts = fact_table[~fact_table['composite_key'].isin(existing_keys['composite_key'])]
            
            # Drop the composite key column before inserting
            new_facts = new_facts.drop('composite_key', axis=1)
            
            if not new_facts.empty:
                # Insert new records
                new_facts.to_sql('fact_food_prices', engine, if_exists='append', index=False)
                print(f"Inserted {len(new_facts)} new fact records")
            else:
                print("No new fact records to insert")
                
        except Exception as e:
            print(f"Error checking existing records: {e}")
            # If checking fails, just insert all records
            fact_table_clean = fact_table.drop('composite_key', axis=1)
            fact_table_clean.to_sql('fact_food_prices', engine, if_exists='append', index=False)
            print(f"Inserted {len(fact_table_clean)} fact records (without duplicate check)")
    
    else:
        print("No fact records to insert")
    
    print("Fact table loading completed!")

# Run the process
if __name__ == "__main__":
    try:
        df = extraction_and_transformation() # captures df returned by function
        load_dimensions(df)
        load_location_dimension(df)
        load_commodity_dimension(df)
        load_market_type_dimension(df)
        load_fact_table(df)
        print("\n🎉 Data loaded successfully into PostgreSQL!")
        
    except Exception as e:
        print(f"\n Error: {e}")
