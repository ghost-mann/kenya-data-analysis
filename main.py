import pandas as pd
import knoema
import os
from dotenv import load_dotenv

# load .env file
load_dotenv()

# import .env values
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_port = os.getenv("DB_PORT")

# extraction
df = pd.read_csv('wfp_food_prices_ken.csv')

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

print(df.head())