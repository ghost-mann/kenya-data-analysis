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

    # knoema_data = knoema.get('zpkquzb', **{'Product': 'P14;P15',
    # 	'Product Class': 'C4'})
    # df_knoema = pd.DataFrame(knoema_data)

    # transformation
    # drop rows with null values
    df = df.dropna()

    # remove whitespace from column names
    df.columns = df.columns.str.strip()

    # remove duplicate rows
    df.drop_duplicates(inplace=True)

    # reset index
    df.reset_index(drop=True, inplace=True)

    print(df)
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
            print(f"  ⚠ Error inserting new dates: {e}")
    else:
        print("No new dates to insert")

def load_location_dimension(df):
    print("Loading location dimension...")
    df['region'] = 'Kenya'  # Assuming your dataset is Kenya-only
    location_cols = ['region', 'admin1', 'admin2', 'market']
    locations = df[location_cols].drop_duplicates()

    existing = pd.read_sql("SELECT region, admin1, admin2, market FROM dim_location", engine)
    new_locations = locations.merge(existing, on=location_cols, how='left', indicator=True)
    new_locations = new_locations[new_locations['_merge'] == 'left_only'].drop('_merge', axis=1)

    if not new_locations.empty:
        new_locations.to_sql('dim_location', engine, if_exists='append', index=False)
        print(f"✓ Inserted {len(new_locations)} new location records")
    else:
        print("✓ No new location records needed")

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
        print(f"✓ Inserted {len(new_commodities)} new commodity records")
    else:
        print("✓ No new commodity records needed")

def load_market_type_dimension(df):
    print("Loading market type dimension...")
    types = df[['pricetype']].drop_duplicates().rename(columns={'pricetype': 'market_type'})

    existing = pd.read_sql("SELECT market_type FROM dim_market_type", engine)
    new_types = types[~types['market_type'].isin(existing['market_type'])]

    if not new_types.empty:
        new_types.to_sql('dim_market_type', engine, if_exists='append', index=False)
        print(f"✓ Inserted {len(new_types)} new market types")
    else:
        print("✓ No new market types needed")

# Run the process
if __name__ == "__main__":
    try:
        df = extraction_and_transformation() # captures df returned by function
        load_dimensions(df)
        load_location_dimension(df)
        load_commodity_dimension(df)
        print("\n🎉 Data loaded successfully into PostgreSQL!")
        
    except Exception as e:
        print(f"\n Error: {e}")
