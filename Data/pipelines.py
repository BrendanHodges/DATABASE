
import pandas as pd
from Data.db import get_session
from sqlalchemy import text
import re
from sqlalchemy import text, bindparam
import re, unicodedata
from difflib import SequenceMatcher
from sqlalchemy import text

def add_dataset():
    query = text("INSERT INTO census_datasets (dataset_ID, table_ID, title, survey, vintage, description) VALUES (:dataset_ID, :table_ID, :title, :survey, :vintage, :description);")
    with get_session() as s:
        s.execute(query, [{"dataset_ID": "20", "table_ID": "NA", "title": "GEOINFO", "survey": "ACS 5-Year", "vintage": "2023", "description": "Geographic Information including area measurements"}])
        s.commit()


def add_variable():
    query = text("INSERT INTO census_variables (Variable_ID, dataset_ID, variable_code, name, multi) VALUEs (:Variable_ID, :dataset_ID, :variable_code, :name, :multi);")
    with get_session() as s:
        s.execute(query, [{"Variable_ID": "43", "dataset_ID": "20", "variable_code": "AREALAND_SQMI", "name": "Land Area in Square Miles", "multi": False}])
        s.commit()

#Return all table-level metadata as a list of dicts.
def get_table_metadata():
    """Return all table-level metadata as a list of dicts."""
    query = text("SELECT * FROM table_metadata;")
    with get_session() as s:
        rows = s.execute(query).mappings().all()
    return [dict(row) for row in rows]

#Return all column-level metadata as a list of dicts.
def get_column_metadata():
    """Return all column-level metadata as a list of dicts."""
    query = text("SELECT * FROM column_metadata;")
    with get_session() as s:
        rows = s.execute(query).mappings().all()
    return [dict(row) for row in rows]

#Grab specific questions from the questions table based on a provided list.

def grab_specific_questions(result):
    questions = result.get("questions")
    if not questions:
        stmt = text("""
            SELECT question, question_id, spreadsheet_idx
            FROM questions
        """)
        with get_session() as s:
            rows = s.execute(stmt).fetchall()
            df = pd.DataFrame(rows, columns=["question", "question_id", "spreadsheet_idx"])
            return df
    if isinstance(questions, str):
        questions = [questions]  
    stmt = (
            text("""
                SELECT question, question_id, spreadsheet_idx
                FROM questions
                WHERE question IN :questions
            """)
            .bindparams(bindparam("questions", expanding=True))
            )
    with get_session() as s:
        rows = s.execute(stmt, {"questions": questions}).fetchall()
        df = pd.DataFrame(rows, columns=["question", "question_id", "spreadsheet_idx"])
        return df

###########################COUNTY FUZZY FIND AND MATHCHING###########################################################################
#Get the County_id for a given county name and state.
def _norm(s: str) -> str:
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn").lower().strip()

    s = re.sub(r"^\s*(city\s+and\s+county|city|county)\s+of\s+", "", s)
    s = re.sub(r"\bst[.\s]\b", "saint ", s)      # St. -> Saint
    s = re.sub(r"\bste[.\s]\b", "sainte ", s)    # Ste. -> Sainte
    s = re.sub(r"\bmt[.\s]\b", "mount ", s)      # Mt. -> Mount

    s = re.sub(r"\b(county|parish|borough|census area|municipality)\b", "", s)
    s = re.sub(r"[^\w\s]", " ", s)               # drop punctuation ("Mary's" -> "mary s")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _score(a, b):  # tiny fuzzy
    return SequenceMatcher(None, a, b).ratio()

def get_county_id(county_name: str, state: str, threshold: float = 0.90):
    # add "County" unless it looks like a city name
    if not re.search(r"\bcounty\b", county_name, re.I) and not re.search(r"\bcity\b", county_name, re.I):
        county_name += " County"
    target = _norm(county_name)

    with get_session() as s:
        rows = s.execute(text("""
            SELECT County_id, county
            FROM counties
            WHERE state = :state
        """), {"state": state}).fetchall()

    if not rows:
        raise ValueError(f"No counties for state {state}")

    # exact normalized match first
    for cid, cname in rows:
        if _norm(cname) == target:
            return cid

    # quick fuzzy within state
    best = max(rows, key=lambda r: _score(target, _norm(r[1])))
    if _score(target, _norm(best[1])) >= threshold:
        return best[0]

    # helpful error
    alts = sorted(((cname, _score(target, _norm(cname))) for _, cname in rows),
                  key=lambda x: x[1], reverse=True)[:5]
    tips = ", ".join(f"{n} ({s:.2f})" for n, s in alts)
    return [f"No good match for county '{county_name}' in state '{state}'. Closest: {tips}", False]

def get_sheet_id(state):
    query = text("SELECT sheet_id FROM sheet_ids WHERE state = :state")
    try:
        with get_session() as s:
            res = s.execute(query, {"state": state}).fetchone()
            if not res:
                raise ValueError(f"No sheet_id found for state: {state}")
            Sheet_id = res[0]
    except Exception as e:
        raise RuntimeError(f"Database error: {str(e)}") from e
    return Sheet_id

##########################################################################################################################################


#INSERT DATA INTO RESPONSES TABLE RETURN DUPLICATE ANSWERS TO QUESTIONS
def bulk_insert_with_dupe_report(df):
    records = df.to_dict(orient="records")
    if not records:
        return []

    with get_session() as conn:
        # 1) Stage rows in a temp table (dedupes within df)
        conn.execute(text("""
            CREATE TEMP TABLE IF NOT EXISTS tmp_new (
                county_id   INTEGER NOT NULL,
                question_id INTEGER NOT NULL,
                definition  TEXT,
                link        TEXT,
                value       INTEGER,
                PRIMARY KEY (county_id, question_id)
            )
        """))
        conn.execute(text("DELETE FROM tmp_new"))
        conn.execute(text("""
            INSERT OR IGNORE INTO tmp_new (county_id, question_id, definition, link, value)
            VALUES (:county_id, :question_id, :definition, :link, :value)
        """), records)

        # 2) Pairs that already exist in responses
        dup_pairs = [
            (c, q) for (c, q) in conn.execute(text("""
                SELECT t.county_id, t.question_id
                FROM tmp_new t
                INNER JOIN responses r
                  ON r.county_id = t.county_id AND r.question_id = t.question_id
            """)).fetchall()
        ]

        # 3) Insert only new rows, with safety check for value
        #    Coerce to int and keep only {0,1,2}; else 0
        conn.execute(text("""
            INSERT INTO responses (county_id, question_id, definition, link, value)
            SELECT
                t.county_id,
                t.question_id,
                t.definition,
                t.link,
                CASE
                  WHEN CAST(t.value AS INTEGER) IN (0,1,2) THEN CAST(t.value AS INTEGER)
                  ELSE 0
                END AS value
            FROM tmp_new t
            LEFT JOIN responses r
              ON r.county_id = t.county_id AND r.question_id = t.question_id
            WHERE r.county_id IS NULL
        """))

    return dup_pairs

###############################################################FINAL DATA FETCHING##########################################################################
def grab_variables():
    query = text("SELECT name FROM census_variables;")
    with get_session() as s:
        rows = s.execute(query).mappings().all()
    return [row["name"] for row in rows]

    
def grab_categories():
    query = text("SELECT DISTINCT category FROM questions;")
    with get_session() as s:
        rows = s.execute(query).mappings().all()
    return [row["category"] for row in rows]

def normalize_state_input(user_text):
    if not user_text:
        return None

    # Handle comma-separated or list input
    if isinstance(user_text, str):
        parts = [p.strip().upper() for p in user_text.split(",") if p.strip()]
    elif isinstance(user_text, list):
        parts = [str(p).strip().upper() for p in user_text if str(p).strip()]
    else:
        return None

    if not parts:
        return None

    sql = text("SELECT abbrev, state_ID FROM states WHERE abbrev IN :abbrevs")\
        .bindparams(bindparam("abbrevs", expanding=True))
    
    try:
        with get_session() as s:
            rows = s.execute(sql, {"abbrevs": parts}).fetchall()
        ids = [r[1] for r in rows]
        return ids[0] if len(ids) == 1 else ids if ids else None
    except Exception as e:
        print(f"DB error: {e}")
        return None

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

def add_new_data():
    df = pd.read_csv(r"C:\Users\hodge\Downloads\Voting_DB\county_sqmi_2023.csv", dtype=str)
    df["state"] = df["state"].astype(str).str.lstrip("0")
    df["County_id"] = df["state"].astype(str) + df["county"].astype(str)
    df_to_insert = df[["County_id", "AREALAND_SQMI"]]
    df_to_insert.rename(columns={"AREALAND_SQMI": "data"}, inplace=True)
    df_to_insert["variable_ID"] = "43"
    df_to_insert = df_to_insert[["variable_ID", "County_id", "data"]]
    with get_session() as engine:
        cur = engine.connection()
        df_to_insert.to_sql("census_facts", cur, if_exists="append", index=False, method="multi")  
