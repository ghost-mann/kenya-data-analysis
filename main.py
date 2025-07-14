import pandas as pd

kenya_df = pd.read_csv('/home/austin/vscodeProjects/kenya-data-analysis/API_KEN_DS2_en_csv_v2_30096/API_KEN_DS2_en_csv_v2_30096.csv',skiprows=4
                       )

keywords = ['food', 'inflation', 'consumer price', 'cpi', 'price index']

# filtering step - by indicator 
filtered_df = kenya_df[kenya_df['Indicator Name'].str.contains('|'.join(keywords), case=False, na=False)]

# column selection
filtered_df = filtered_df[['Indicator Name', '2000', '2023']]

print(filtered_df)

print(f"Filtered indicators count: {len(filtered_df)}")