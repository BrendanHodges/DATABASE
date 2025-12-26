import sqlite3
import re
from contextlib import closing
import pandas as pd

conn = sqlite3.connect("MoVE.db")
cursor = conn.cursor()
#
cursor.execute("DROP TABLE IF EXISTS county_stats")
cursor.execute("DROP TABLE IF EXISTS column_metadata")
query = """ CREATE TABLE IF NOT EXISTS column_metadata (
  table_name        TEXT,
  column_name       TEXT,
  data_type         TEXT,
  is_primary_key    INTEGER DEFAULT 0,
  is_foreign_key    INTEGER DEFAULT 0,
  references_table  TEXT,
  definition        TEXT,
  source            TEXT,
  unit              TEXT,
  notes             TEXT,
  created_at        TEXT DEFAULT (datetime('now')),
  updated_at        TEXT,
  PRIMARY KEY (table_name, column_name)
);
""" 

Tables = {
    'counties': [
        {'title': 'County_id', 'description': 'Unique GEOID for county', 'data_type': 'Text',
         'is_primary_key': '1', 'is_foreign_key': '0', 'unit': 'id', 'notes': '', 'created_at': '9/1/2025', 'source': ''},
        {'title': 'county', 'description': 'Name of county', 'data_type': 'Text',
         'is_primary_key': '0', 'is_foreign_key': '0', 'unit': '', 'notes': '', 'created_at': '9/1/2025', 'source': ''},
        {'title': 'state', 'description': 'Name of State county resides in', 'data_type': 'Text',
         'is_primary_key': '0', 'is_foreign_key': '0', 'unit': '', 'notes': '', 'created_at': '9/1/2025', 'source': ''},
        {'title': 'name', 'description': 'Full name of county', 'data_type': 'Text',
         'is_primary_key': '0', 'is_foreign_key': '0', 'unit': '', 'notes': '', 'created_at': '9/1/2025', 'source': ''},
        {'title': 'state_abbrev', 'description': 'State abbreviation', 'data_type': 'Text',
         'is_primary_key': '0', 'is_foreign_key': '0', 'unit': '', 'notes': '', 'created_at': '9/1/2025', 'source': ''},
    ], 
     'county_economics': [
        {'title': 'county_id', 'description': 'Unique GEOID for county', 'data_type': 'Text',
         'is_primary_key': '0', 'is_foreign_key': '1', 'references_table': 'counties(county_id)', 'unit': 'id', 'notes': '', 'created_at': '9/1/2025'},
        {'title': 'median_household_income', 'description': 'Median household income of county', 'data_type': 'Integer',
         'is_primary_key': '0', 'is_foreign_key': '0', 'references_table': '', 'unit': 'USD', 'notes': '', 'created_at': '9/1/2025', 'source': 'https://data.census.gov/table/ACSST5Y2023.S1903?t=Income+(Households,+Families,+Individuals)&g=010XX00US,$0500000'},
    ],
    'county_voting_info': [
        {'title': 'county_id', 'description': 'Unique GEOID for county', 'data_type': 'Text',
         'is_primary_key': '0', 'is_foreign_key': '1', 'references_table': 'counties(county_id)', 'unit': 'id', 'notes': '', 'created_at': '9/1/2025', 'source': ''},
        {'title': 'registered_voters_2024', 'description': 'Number of registered voters in county as of November 2024', 'data_type': 'Integer',
         'is_primary_key': '0', 'is_foreign_key': '0', 'references_table': '', 'unit': 'people', 'notes': '', 'created_at': '9/1/2025', 'source': 'State-website'},
        {'title': 'total_voter_turnout_2024_general', 'description': 'Total voter turnout in 2024 general election', 'data_type': 'Integer',
         'is_primary_key': '0', 'is_foreign_key': '0', 'references_table': '', 'unit': 'people', 'notes': '', 'created_at': '9/1/2025', 'source': 'State-website'},
        {'title': 'turnout_percentage_2024_general', 'description': 'Percentage of registred voters who casts ballots in 2024 general election', 'data_type': 'Decimal',
         'is_primary_key': '0', 'is_foreign_key': '0', 'references_table': '', 'unit': 'people', 'notes': '', 'created_at': '9/1/2025'},
        {'title': 'population_over_18', 'description': 'Number of residents in county over the age of 18', 'data_type': 'Integer',
         'is_primary_key': '0', 'is_foreign_key': '0', 'references_table': '', 'unit': 'people', 'notes': '', 'created_at': '9/1/2025', 'source': 'https://www.census.gov/programs-surveys/decennial-census/about/voting-rights/cvap.html?utm_source=chatgpt.com'},
        {'title': 'registered_voters_percentage', 'description': 'Percentage of residents over 18 who registered to vote', 'data_type': 'Decimal',
         'is_primary_key': '0', 'is_foreign_key': '0', 'references_table': '', 'unit': 'people', 'notes': '', 'created_at': '9/1/2025'}
    ],
     'county_population_size': [
        {'title': 'county_id', 'description': 'Unique GEOID for county', 'data_type': 'Text',
         'is_primary_key': '0', 'is_foreign_key': '1', 'references_table': 'counties(county_id)', 'unit': 'id', 'notes': '', 'created_at': '9/1/2025'},
        {'title': 'population', 'description': 'Population of county', 'data_type': 'Integer',
         'is_primary_key': '0', 'is_foreign_key': '0', 'references_table': '', 'unit': 'people', 'notes': '', 'created_at': '9/1/2025', 'source': 'https://www.census.gov/data/tables/time-series/demo/popest/2020s-counties-total.html'},
        {'title': 'land_area_sqmi', 'description': 'Land area of county', 'data_type': 'Real',
         'is_primary_key': '0', 'is_foreign_key': '0', 'references_table': '', 'unit': 'square miles', 'notes': '', 'created_at': '9/1/2025', 'source': 'https://www.census.gov/cgi-bin/geo/shapefiles/index.php?year=2024&layergroup=Counties+%28and+equivalent%29'},
        {'title': 'population_density', 'description': 'Population density of county', 'data_type': 'Real',
         'is_primary_key': '0', 'is_foreign_key': '0', 'references_table': '', 'unit': 'people per square mile', 'notes': '', 'created_at': '9/1/2025'}
    ],
    'questions': [
        {'title': 'question_id', 'description': 'Unique ID for county specific MoVE question', 'data_type': 'Integer',
         'is_primary_key': '1', 'is_foreign_key': '0', 'references_table': '', 'unit': 'id', 'notes': '', 'created_at': '9/1/2025'},
        {'title': 'question', 'description': 'County specific MoVE question', 'data_type': 'Text',
         'is_primary_key': '0', 'is_foreign_key': '0', 'references_table': '', 'unit': 'answer', 'notes': '', 'created_at': '9/1/2025'},
        {'title': 'category', 'description': 'Category of question/ each question will relate to a specific score', 'data_type': 'Text',
         'is_primary_key': '0', 'is_foreign_key': '0', 'references_table': '', 'unit': '', 'notes': 'Question will relate to abuse score, voting provision score, or registration provisions score', 'created_at': '9/1/2025'},
        {'title': 'spreadsheet_idx', 'description': 'Location on MoVE spreadsheet response to question can be found', 'data_type': 'Text',
         'is_primary_key': '0', 'is_foreign_key': '0', 'references_table': '', 'unit': '', 'notes': '', 'created_at': '9/1/2025'},
    ],
    'responses': [
        {'title': 'county_id', 'description': 'Unique ID for county response relates to', 'data_type': 'Text',
         'is_primary_key': '0', 'is_foreign_key': '1', 'references_table': 'counties(county_id)', 'unit': 'id', 'notes': '', 'created_at': '9/1/2025'},
        {'title': 'question_id', 'description': 'Unique ID for question response relates to', 'data_type': 'Integer',
         'is_primary_key': '0', 'is_foreign_key': '1', 'references_table': 'questions(question_id)', 'unit': 'id', 'notes': '', 'created_at': '9/1/2025'},
        {'title': 'definition', 'description': 'Information Supporting response to Question', 'data_type': 'Text',
         'is_primary_key': '0', 'is_foreign_key': '0', 'references_table': '', 'unit': '', 'notes': '', 'created_at': '9/1/2025'},
        {'title': 'link', 'description': 'Link where information to answer question was found', 'data_type': 'Text',
         'is_primary_key': '0', 'is_foreign_key': '0', 'references_table': '', 'unit': 'Hyper Link', 'notes': '', 'created_at': '9/1/2025'},
        {'title': 'value', 'description': 'Answer to question 1(yes), 0(No), =<2(specific score)', 'data_type': 'Integer',
         'is_primary_key': '0', 'is_foreign_key': '0', 'references_table': '', 'unit': '', 'notes': '', 'created_at': '9/1/2025'},
    ],
    'states': [
        {'title': 'state_id', 'description': 'Unique ID for state, from census FIPs', 'data_type': 'Text',
         'is_primary_key': '1', 'is_foreign_key': '0', 'references_table': '', 'unit': 'id', 'notes': '', 'created_at': '9/1/2025'},
        {'title': 'abbrev', 'description': 'State abbrev', 'data_type': 'Text',
         'is_primary_key': '0', 'is_foreign_key': '0', 'references_table': '', 'unit': '', 'notes': '', 'created_at': '9/1/2025'},
        {'title': 'name', 'description': 'Full state name', 'data_type': 'Text',
         'is_primary_key': '0', 'is_foreign_key': '0', 'references_table': '', 'unit': 'id', 'notes': '', 'created_at': '9/1/2025'},
    ],
    'state_questions': [
        {'title': 'state_question_id', 'description': 'Unique ID identifying state specific questions', 'data_type': 'Text',
         'is_primary_key': '1', 'is_foreign_key': '0', 'references_table': '', 'unit': 'id', 'notes': '', 'created_at': '9/1/2025'},
        {'title': 'state_question', 'description': 'State specific MoVE question', 'data_type': 'Text',
         'is_primary_key': '0', 'is_foreign_key': '0', 'references_table': '', 'unit': '', 'notes': '', 'created_at': '9/1/2025'},
    ],
    'state_responses': [
        {'title': 'state_id', 'description': 'Unique ID for state response relates to', 'data_type': 'Text',
         'is_primary_key': '0', 'is_foreign_key': '1', 'references_table': 'states(state_id)', 'unit': 'id', 'notes': '', 'created_at': '9/1/2025'},
        {'title': 'state_question_id', 'description': 'Unique ID for state specific question response relates to', 'data_type': 'Text',
         'is_primary_key': '0', 'is_foreign_key': '1', 'references_table': 'state_questions(state_question_id)', 'unit': 'id', 'notes': '', 'created_at': '9/1/2025'},
        {'title': 'response', 'description': 'Response to state specific question', 'data_type': 'Text',
         'is_primary_key': '0', 'is_foreign_key': '0', 'references_table': '', 'unit': '', 'notes': 'Answers will be Yes, No or a percentage', 'created_at': '9/1/2025'},
    ] 
}

cursor.execute(query)
for table_name, columns in Tables.items():
    print(f"Table: {table_name}")
    for col in columns:
        print(f"  Column: {col['title']}")
        cursor.execute("""
            INSERT INTO column_metadata (table_name, column_name, data_type, is_primary_key, is_foreign_key, references_table, definition, unit, notes, created_at, updated_at, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """, (table_name, col['title'], col['data_type'], col['is_primary_key'], col['is_foreign_key'], col.get('references_table', None), col['description'], col['unit'], col['notes'], col['created_at'], None, col.get('source', None)))
conn.commit()
conn.close()