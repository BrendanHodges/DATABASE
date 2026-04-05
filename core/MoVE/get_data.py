import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from Data.db import get_session
from config.settings import Settings
from sqlalchemy import text
import re
import time
import streamlit as st
from sqlalchemy import text, bindparam
from config.settings import settings
from Data.sheets import Sheets
from Data import pipelines as p
from core.cleaners import cleaners as c 

#Set up credentials for Google Account
sheets = Sheets(
    credentials_file=settings.GOOGLE_CREDENTIALS_FILE,
    scopes=settings.GOOGLE_SCOPES,
    retries=settings.SHEETS_RETRIES,
    base_delay=settings.SHEETS_BACKOFF_BASE_SEC,
)

def state_setup(Sheet_id, question_map, state):
    sheet = sheets.open_by_key(Sheet_id)
    worksheets = sheet.worksheets()
    MoVE_data = pd.DataFrame(columns=['score', 'definition', 'link'])
    progress = st.progress(0)
    status = st.empty()
    total = len(worksheets)
    for i, worksheet in enumerate(worksheets):
        county_name = worksheet.title  # Assuming sheet name = county name
        if county_name == "State Sources" or county_name == "State Resources" or county_name == "State-Wide" or county_name == "GUIDE" or county_name == "Sheet77" or county_name == "Assignment List":
            continue  # Skip the sources sheet
        county_name = c.strip_trailing_parenthetical(county_name)

        county_id = p.get_county_id(county_name, state)
        if isinstance(county_id, list):  # If no good match found
            st.warning(county_id[0])
            continue

        status.text(f"Collecting data for: {county_name} ({i+1}/{total})")
        MoVE_data_temp = get_MoVE_data_all_county(worksheet, county_name, question_map, county_id)
        MoVE_data = pd.concat([MoVE_data, MoVE_data_temp], ignore_index=True)
        progress.progress((i+1)/total)
        time.sleep(0.85) # Pause to avoid hitting API limits
    return MoVE_data


def county_setup(Sheet_id, result, question_map):
    key_word = result.get("county_name")# e.g., "Baltimore County"
    state = result.get("state")  # e.g., "MD"
    county_name = key_word.replace(" County", "").strip()
    county_name = c.strip_trailing_parenthetical(county_name)
    county_id = p.get_county_id(county_name, state)
    # Open the Google Sheets
    sheet = sheets.open_by_key(Sheet_id)
    worksheets = sheet.worksheets()
    matched_to_county = next((ws for ws in worksheets if county_name.lower() in ws.title.lower()), None)
    MoVE_data = get_MoVE_data_all_county(matched_to_county, county_name, question_map, county_id)
    return MoVE_data

def match_to_question(df, question_map):
    # Invert it
    inverted = {v: k for k, v in question_map.items()}
    # inverted: either {"(24, 25)": "Question"} or {"Question": "(24, 25)"}
    is_blob = lambda x: re.fullmatch(r"[()\d,\s,]+", str(x) or "") is not None
    idx2q = {
        i: (v if is_blob(k) else k)
        for k, v in inverted.items()
        for i in map(int, re.findall(r"\d+", str(k if is_blob(k) else v)))
    }
    # If spreadsheet_idx is a column:
    df["question_name"] = df["spreadsheet_idx"].astype(int).map(idx2q)
    return df

def get_data_all_county(result):
    if result.get("mode") == "specific":
        returned = p.grab_specific_questions(result)
        question_map = returned.set_index("question")["spreadsheet_idx"].to_dict()
    else:    
        with get_session() as s:
            returned = s.execute(text("SELECT question, spreadsheet_idx FROM questions"))
            question_map = {name: idx for name, idx in returned}
    state = result.get("state")
    if not state:
        raise ValueError("State abbreviation is required.")
    # Fetch the sheet_id from the database
    Sheet_id = p.get_sheet_id(state)
    print(f"Using Sheet ID: {Sheet_id} for state: {state}")
    if result.get("entity") == "county":
        MoVE_data = county_setup(Sheet_id, result, question_map)
        final = match_to_question(MoVE_data, question_map)
        return final
    if result.get("entity") == "state":
        MoVE_data = state_setup(Sheet_id, question_map, state)
        print(MoVE_data)
        final = match_to_question(MoVE_data, question_map)
        return final

def get_MoVE_data_all_county(matched, key_word, question_map, county_id):
    idx = c.normalize_idx(question_map.values())
    print(f"Fetching data for indexes: {idx} in county: {key_word}")
    if matched:
        ranges = [f"B{r}:D{r}" for r in idx]
        # Batch get all ranges at once
        results = matched.batch_get(ranges)
        # Pair each row number with its B–D values
        row_data = {r: vals[0] if vals else [] for r, vals in zip(idx, results)}
    else:
        raise ValueError(f"No worksheet found for county: {key_word}")
    #Indexes where data needs cleaning
    remove = [122, 123, 124, 24, 25, 54, 55, 63, 64, 72, 73, 80, 81, 82, 83, 84, 85, 100, 101, 102, 103, 106, 107]
    Needs_cleaning = {k: v for k, v in row_data.items() if k in remove}
    Cleaned = {k: v for k, v in row_data.items() if k not in remove}
    if Needs_cleaning:
        print(Needs_cleaning, "HELLLlLLLOOOOOOOOOOOOOO")
        cleaned = c.clean_unatural_indexes(Needs_cleaning)
        final = {**Cleaned, **cleaned}
    else:
        final = Cleaned
    #Data to add including spreadsheet index, value and definition
    final2 = pd.DataFrame.from_dict(final, orient='index', columns=['score', 'definition', 'link'])
    final2['County_id'] = county_id
    final2['county_name'] = key_word
    final2 = final2.reset_index().rename(columns={'index': 'spreadsheet_idx'})

    final2['score'] = pd.to_numeric(final2['score'], errors='coerce')  # convert to numbers, NaN if bad
    final2.loc[~final2['score'].isin([0, 1, 2]) & ~final2['spreadsheet_idx'].isin(remove), 'score'] = 0 
    final2['score'] = final2['score'].fillna(0).astype(int)
    
    return final2

    





