import sqlite3
import re
from contextlib import closing
import pandas as pd

conn = sqlite3.connect('MoVE.db')
cursor = conn.cursor()
# cursor.execute("DROP TABLE IF EXISTS census_datasets;")
# query = """ CREATE TABLE IF NOT EXISTS census_datasets (
#   dataset_ID   TEXT PRIMARY KEY,
#   table_ID     TEXT,
#   title        TEXT,
#   survey       TEXT,
#   vintage      TEXT,
#   description  TEXT
# );
# """
# cursor.execute(query)
# conn.commit()

# query = """ CREATE TABLE IF NOT EXISTS census_variables (
#   variable_ID  TEXT PRIMARY KEY,
#   dataset_ID   TEXT,
#   variable_code  TEXT,
#   name TEXT,
#   multi BOOLEAN,
#   FOREIGN KEY (dataset_ID) REFERENCES census_datasets(dataset_ID)
# );
# """
# cursor.execute(query)
# conn.commit()
cursor.execute("DROP TABLE IF EXISTS census_facts;")
query = """ CREATE TABLE IF NOT EXISTS census_facts (
  variable_ID     TEXT,
  County_id  TEXT,
  data TEXT,
  date_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (variable_ID, County_ID),
    FOREIGN KEY (County_ID) REFERENCES counties(County_ID),
    FOREIGN KEY (variable_ID) REFERENCES census_variables(variable_ID)
);
"""

# df = pd.read_excel(r"C:\Users\hodge\Downloads\Voting_DB\Census_Variables.xlsx")
# print(df)
# df['dataset_ID'] = df['dataset_ID'].astype(int).astype(str)
# df['variable_ID'] = df['variable_ID'].astype(int).astype(str)
# df.to_sql('census_variables', conn, if_exists='append', index=False)
cursor.execute(query)
conn.commit()

