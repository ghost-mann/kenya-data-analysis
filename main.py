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
            print(f"  ✓ Inserted {len(new_dates)} new date records")
        except Exception as e:
            print(f"  ⚠ Error inserting new dates: {e}")
    else:
        print("  ✓ No new dates to insert")


# Run the process
if __name__ == "__main__":
    try:
        df = extraction_and_transformation() # captures df returned by function
        load_dimensions(df)
        # load_fact_table()
        # test_data()
        print("\n🎉 Data loaded successfully into PostgreSQL!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nMake sure to:")
        print("1. Update database credentials")
        print("2. Check that tables exist")
        print("3. Verify column names match your CSV")
        print("4. Install required packages: pip install pandas psycopg2-binary sqlalchemy")