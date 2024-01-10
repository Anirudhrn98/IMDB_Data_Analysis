import pandas as pd
import json
import numpy as np

# Create a DataFrame for the json file
df = pd.read_json('/Users/anirudhramesh/Desktop/INTRO_TO_BIGDATA/ASSN4/extra-data.json', lines=True)

# Select specific columns that we require and
columns = ['box_office_currencyLabel', 'MPAA_film_ratingLabel', 'IMDb_ID', 'cost', 'distributorLabel',
             'box_office', 'titleLabel']
df = df[columns]
# Rename box_office to revenue
df = df.rename(columns={'box_office': 'revenue'})
columns[5] = 'revenue'

# Choose only the data in the 'value' field for each column
for col in columns:
    df[col] = df[col].apply(lambda x: x['value'] if isinstance(x, dict) else x)

# Process data before uploading to mongodb
# Remove tt from imdb_id and set cost, revenue and imdb_id to int, float as needed.
df['IMDb_ID'] = df['IMDb_ID'].str.replace('tt', '')
df['IMDb_ID'] = df['IMDb_ID'].str.replace('nm', '')
#  Replace NaN values with empty string.
df['IMDb_ID'] = df['IMDb_ID'].fillna('')
df['cost'] = df['cost'].fillna('')
df['revenue'] = df['revenue'].fillna('')
df['IMDb_ID'] = df['IMDb_ID'].fillna('')

# Check if data exists and convert each value in required columns to required datatype
try:
    for i, val in enumerate(df['cost']):
        if val != '':
            df.loc[i, 'cost'] = float(val)
    for j, val2 in enumerate(df['revenue']):
        if val2 != '':
            df.loc[j, 'revenue'] = float(val2)
    for k, val3 in enumerate(df['IMDb_ID']):
        if val3 != '':
            df.loc[k, 'IMDb_ID'] = int(val3)
except ValueError:
    pass

# Dropping rows where 'IMDB_ID' not present,
df = df.dropna(subset=['IMDb_ID'])
# Set revenue and cost to null where currency is not USD
df.loc[df['box_office_currencyLabel'] != 'United States dollar', ['revenue']] = np.nan

# Store dataframe as Json File
df.to_json('/Users/anirudhramesh/Desktop/INTRO_TO_BIGDATA/ASSN4/json_data_cleaned_mid.json', orient='records',
           lines=True)

# Read above json file and delete null values and store a new json file.
with open('/Users/anirudhramesh/Desktop/INTRO_TO_BIGDATA/ASSN4/json_data_cleaned_mid.json') as f:
    datax = f.readlines()
updated_data = []
# Read through each line of json file and remove anywhere key-value pair where value is empty string.
for line in datax:
    data_dict = json.loads(line)
    for key, value in list(data_dict.items()):
        if value is None or value == '':
            del data_dict[key]
    updated_data.append(data_dict)

# Write the updated json data to a new file
with open('/Users/anirudhramesh/Desktop/INTRO_TO_BIGDATA/ASSN4/json_data_cleaned_final.json', 'w') as f:
    for data_dict in updated_data:
        json_str = json.dumps(data_dict)
        f.write(json_str + ',' + '\n')
