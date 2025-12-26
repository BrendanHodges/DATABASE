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

# Load Census API key from .env file
load_dotenv() 
key = os.getenv("CENSUS_API_KEY")

# hard-clean: strip whitespace, quotes, and any non-ASCII, clean census key
clean = key.strip().strip('"').strip("'")
clean = re.sub(r"[^\x20-\x7E]", "", clean)
print("clean repr:", repr(clean), "len:", len(clean))


def fetch_subject_county(var, state_fips, year, key, county="*"):
    url = f"https://api.census.gov/data/{year}/acs/acs5/subject"
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


def fetch_county_sqmi(state_fips="*", year=2023, key=None):
    base = f"https://api.census.gov/data/{year}/geoinfo"
    get_vars = ["NAME", "AREALAND_SQMI"]

    params = {
        "get": ",".join(get_vars),
        "for": "county:*",
    }
    # filter to a single state if provided
    if state_fips != "*":
        params["in"] = f"state:{state_fips}"
    if key:
        params["key"] = key

    r = requests.get(base, params=params)
    r.raise_for_status()
    data = r.json()
    cols = data[0]
    rows = [dict(zip(cols, row)) for row in data[1:]]

    # make numeric where appropriate
    for d in rows:
        d["AREALAND_SQMI"] = float(d["AREALAND_SQMI"])
    return rows

rows = fetch_county_sqmi(year=2023, key=clean)
df_sqmi = pd.DataFrame(rows)
df_sqmi.to_csv("county_sqmi_2023.csv", index=False)

