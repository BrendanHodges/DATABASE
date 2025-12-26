import pandas as pd
from Data.db import get_session
from sqlalchemy import text
import re
from sqlalchemy import text, bindparam
import re, unicodedata
from difflib import SequenceMatcher
from sqlalchemy import text
import os
import duckdb

def grab_categories():
    query = text("SELECT DISTINCT category FROM questions;")
    with get_session() as s:
        rows = s.execute(query).mappings().all()
    return [row["category"] for row in rows]


def normalize_state_input(user_text: str):
    user_text = (user_text or "").strip()
    if not user_text or len(user_text) > 3:
        return None
    try:
        query = text("SELECT state_ID FROM states WHERE abbrev = :abbrev")
        with get_session() as s:
            res = s.execute(query, {"abbrev": user_text.upper()}).fetchone()
            if res:
                return res[0]
            return user_text
    except ValueError:
        return None
    except Exception as e:
        print(f"Database error: {str(e)}")
        return None

from sqlalchemy import text, bindparam
import pandas as pd


def run_query(state_ids: list[str] | str | None, selected_variables: list[str], category: str) -> pd.DataFrame:
    """Query MoVE data by one or multiple state abbreviations (TEXT state_IDs)."""
    # --- Guard clauses
    if not selected_variables:
        return pd.DataFrame()
    selected_lc = [s.strip().lower() for s in selected_variables if s.strip()]
    if not selected_lc:
        return pd.DataFrame()

    # --- Normalize state_ids (always text)
    if isinstance(state_ids, str):
        state_ids_list = [s.strip().upper() for s in state_ids.split(",") if s.strip()]
    elif isinstance(state_ids, list):
        state_ids_list = [str(s).strip().upper() for s in state_ids if str(s).strip()]
    else:
        state_ids_list = None

    # --- Base SQL

    sql_base = """
    WITH resp AS (
    SELECT 
        r.County_ID,
        SUM(CAST(r.value AS REAL)) AS category_sum
    FROM responses AS r
    JOIN questions AS q
        ON q.Question_ID = r.Question_ID
    WHERE q.category = :category
    GROUP BY r.County_ID
    )
    SELECT
        c.state_ID            AS state_id,
        s.name                AS state_name,         -- ← grab state name
        c.County_ID           AS county_id,
        c.name                AS county_name,
        v.name                AS variable_name,
        CAST(cf.data AS REAL) AS variable_value,
        resp.category_sum     AS category_sum
    FROM counties AS c
    LEFT JOIN states AS s           ON s.state_ID = c.state_ID   -- ← new join
    LEFT JOIN census_facts AS cf    ON cf.County_ID = c.County_ID
    LEFT JOIN census_variables AS v ON v.variable_ID = cf.variable_ID
    LEFT JOIN resp                  ON resp.County_ID = c.County_ID
    WHERE LOWER(TRIM(v.name)) IN :variable_names_lc
    """


    params = {
        "category": category,
        "variable_names_lc": selected_lc,
    }

    # --- Add state filter if provided
    if state_ids_list:
        sql_base += "  AND c.state_ID IN :state_ids\n"
        params["state_ids"] = state_ids_list
        sql = text(sql_base + "ORDER BY c.name COLLATE NOCASE;").bindparams(
            bindparam("variable_names_lc", expanding=True),
            bindparam("state_ids", expanding=True),
        )
    else:
        sql = text(sql_base + "ORDER BY c.name COLLATE NOCASE;").bindparams(
            bindparam("variable_names_lc", expanding=True),
        )
    

    # --- Execute
    with get_session() as s:
        rows = s.execute(sql, params).mappings().all()

    long_df = pd.DataFrame(rows)
    print(long_df.head())
    print(f"Debug: Retrieved {len(long_df)} rows from the database.")
    if long_df.empty:
        return pd.DataFrame(columns=["state_name", "county_id", "county_name", "category_sum"] + selected_variables)

    # --- Clean up numeric columns
    for col in ("variable_value", "category_sum"):
        if col in long_df.columns:
            long_df[col] = pd.to_numeric(long_df[col], errors="coerce")

    # --- Pivot: one row per county, variables as columns
    wide = long_df.pivot_table(
        index=["state_name", "county_id", "county_name"],
        columns="variable_name",
        values="variable_value",
        aggfunc="mean"
    ).reset_index()

    # --- Add category_sum back (per county)
    wide = wide.merge(
        long_df[["state_name", "county_id", "category_sum"]].drop_duplicates(),
        on=["state_name", "county_id"],
        how="left"
    )

    wide.columns.name = None
    vars_in_result = [c for c in wide.columns if c not in {"state_name", "county_id", "county_name", "category_sum"}]
    wide = wide[["state_name", "county_id", "county_name"] + vars_in_result + ["category_sum"]]

    return wide

import duckdb
import os
def sqlite_to_duckdb_raw(sqlite_path: str, duckdb_path: str):
    if not os.path.exists(sqlite_path):
        raise FileNotFoundError(sqlite_path)

    con = duckdb.connect(duckdb_path)

    # Load sqlite extension
    try:
        con.execute("INSTALL sqlite; LOAD sqlite;")
    except Exception:
        con.execute("INSTALL sqlite_scanner; LOAD sqlite_scanner;")

    # <<< Key line: avoid type conversions on load >>>
    con.execute("SET sqlite_all_varchar=true;")

    con.execute('CREATE SCHEMA IF NOT EXISTS raw;')

    p = sqlite_path.replace("'", "''")
    tables = con.execute(
        f"""
        SELECT name
        FROM sqlite_scan('{p}', 'sqlite_master')
        WHERE type='table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        """
    ).fetchall()

    for (tbl,) in tables:
        t = tbl.replace('"', '""')
        con.execute(
            f'CREATE OR REPLACE TABLE raw."{t}" AS '
            f"SELECT * FROM sqlite_scan('{p}', '{t}');"
        )

    con.close()

# sqlite_to_duckdb_raw(r"C:\Users\hodge\Downloads\Voting_DB\MoVE.db", r"C:\Users\hodge\Downloads\Voting_DB\MoVEDB Snapshot 11-12-2025.duckdb")

#######################################################
def new_questions():
    query = text("INSERT INTO questions (question_ID, question, category, spreadsheet_idx VALUES (:question_ID, :question, :category, :spreadsheet_idx);")
    with get_session() as s:
        s.execute(query, [{"question_ID": 25, "question": "Information about voting is provided at DMV offices", "category": "Voting", "spreadsheet_idx": 11}])
        s.commit()
new_questions()