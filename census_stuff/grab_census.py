from census import Census
import us
import requests
import re
import pandas as pd
from dotenv import load_dotenv
import os
import sqlite3
import re
from contextlib import closing
import pandas as pd

def get_endpoint(variable_code):
    endpoint_map = {
        'B': '/acs/acs5',
        'C': '/acs/acs5',
        'D': '/acs/acs5',
        'S': '/acs/acs5/subject',
        'DP': '/acs/acs5/profile'
    }
    prefix = variable_code[:2] if variable_code.startswith("DP") else variable_code[0]
    endpoint= endpoint_map.get(prefix, '/acs/acs5')
    return endpoint

def fetch_subject_county(var, state_fips, year, key, endpoint, county="*"):
    url = f"https://api.census.gov/data/{year}{endpoint}"
    params = {
        "get": f"NAME,{var}",
        "for": f"county:{county}",
        "in": f"state:{state_fips}",
        "key": key
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    data = r.json()
    # Convert to list of dicts
    cols = data[0]
    return [dict(zip(cols, row)) for row in data[1:]]

def get_variable_id(variable_code):
    with closing(sqlite3.connect('MoVE.db')) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT variable_ID FROM census_variables WHERE variable_code = ?", (variable_code,))
        result = cursor.fetchone()
        print(result[0])
        return result[0] if result else None

def clean_dataframe(df):
    df = df.rename(columns={variable_code: 'data'})
    df['variable_ID'] = var_id
    df['state'] = df['state'].astype(str).str.lstrip("0")
    df['County_id'] = df['state'].astype(str) + df['county'].astype(str)
    df = df.drop(columns=['state', 'county', 'NAME'])
    return df

# Load Census API key from .env file
load_dotenv() 
key = os.getenv("CENSUS_API_KEY")

# hard-clean: strip whitespace, quotes, and any non-ASCII, clean census key
clean = key.strip().strip('"').strip("'")
clean = re.sub(r"[^\x20-\x7E]", "", clean)
print("clean repr:", repr(clean), "len:", len(clean))

conn = sqlite3.connect('MoVE.db')
cursor = conn.cursor()
df_census_codes = pd.read_sql("SELECT variable_ID, variable_code, name, multi FROM census_variables;", conn)

df_census_codes = df_census_codes[(df_census_codes['variable_ID'].astype(int) >= 7) & (df_census_codes['variable_ID'].astype(int) != 13)]
failures = pd.DataFrame(columns=['variable_code', 'name', 'error', 'state'])
all_data = []
for index, row in df_census_codes.iterrows():
    try:
        variable_code = row['variable_code']
        var_id = row['variable_ID']
        name = row['name']
        for state in us.states.STATES:
            print(f"Fetched data for state: {state}")
            endpoint = get_endpoint(variable_code)
            rows = fetch_subject_county(variable_code, state.fips, 2023, clean, endpoint)
            all_data.extend(rows)
        df = pd.DataFrame(all_data)
        df = clean_dataframe(df)
        all_data= []
        df.to_sql('census_facts', conn, if_exists='append', index=False)
        print(f"Inserted data for variable: {variable_code} - {name}")
        conn.commit()
    except Exception as e:
        print(f"Error processing variable {variable_code} - {name}: {e}")
    
df_census_codes.to_csv("failures.csv", index=False)
conn.close()
    


            

