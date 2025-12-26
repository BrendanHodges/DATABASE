import pandas as pd
from Data.db import get_session
from config.settings import Settings
from sqlalchemy import text
import re
import time
import streamlit as st
from Data import pipelines as p
from core.cleaners import cleaners as c 

def add_move_dataframe(df, results):
    if df.empty:
        raise ValueError("The provided DataFrame is empty.")

    # 1) Merge & select
    df_question_id = p.grab_specific_questions(results)  # must contain 'question' and 'question_id'
    if 'question_name' not in df or 'question' not in df_question_id or 'question_id' not in df_question_id:
        raise KeyError("Missing required columns for merge: need df['question_name'], df_question_id['question','question_id'].")

    df = df.merge(df_question_id, how='left', left_on='question_name', right_on='question')

    required = ['County_id', 'question_id', 'definition', 'link', 'score']
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"Missing columns after merge: {missing}")

    df = df[required].rename(columns={
        'County_id': 'county_id',
        'definition': 'definition',
        'link': 'link',
        'score': 'value',
    })

    # Optional: drop rows with null ids early
    df = df.dropna(subset=['county_id', 'question_id'])

    # 2) Insert and get duplicates (must be List[Tuple[int,int]])
    duplicates = p.bulk_insert_with_dupe_report(df)  # ensure this returns [(county_id, question_id), ...]
    print(duplicates)

    # 3) Build dict and DF using the SAME raw pairs list
    pairs_dict = county_to_questions_tmp(duplicates)
    print(pairs_dict)

    total_dups = sum(len(v) for v in pairs_dict.values())
    dup_df = county_to_questions_df(duplicates)  # <<< pass the tuples, not the dict

    if total_dups == 0:
        return {"success": True, "message": "All data was successfully added to the database. No duplicates found", "duplicates": None, "total_duplicates": 0}
    elif total_dups == len(df):
        return {"success": False, "message": "No new data was added. All entries are duplicates.", "duplicates": dup_df, "total_duplicates": total_dups}
    else:
        return {"success": True, "message": f"{len(df) - total_dups} rows added to responses", "duplicates": dup_df, "total_duplicates": total_dups}


def county_to_questions_tmp(pairs):
    if not pairs:
        return {}
    recs = [{"county_id": c, "question_id": q} for c, q in pairs]
    try:
        with get_session() as conn:
            conn.execute(text("DROP TABLE IF EXISTS tmp_pairs"))
            conn.execute(text("CREATE TEMP TABLE tmp_pairs (county_id INT, question_id INT)"))
            conn.execute(text("INSERT INTO tmp_pairs (county_id, question_id) VALUES (:county_id, :question_id)"), recs)

            rows = conn.execute(text("""
                SELECT c.county, q.question   -- use the actual column names
                FROM tmp_pairs p
                JOIN counties  c ON c.county_id  = p.county_id
                JOIN questions q ON q.question_id = p.question_id
                ORDER BY c.county, q.question_id;
            """)).all()
    except Exception as e:
        # fall back cleanly if streamlit isn't present
        try:
            import streamlit as st
            st.error(f"Database error: {e}")
        except Exception:
            print(f"Database error: {e}")
        return {}

    out = {}
    for county, question_text in rows:
        out.setdefault(county, []).append(question_text)
    return out


def county_to_questions_df(pairs):
    if not pairs:
        return pd.DataFrame(columns=["county", "question"])

    recs = [{"county_id": c, "question_id": q} for c, q in pairs]
    try:
        with get_session() as conn:
            conn.execute(text("DROP TABLE IF EXISTS tmp_pairs"))
            conn.execute(text("CREATE TEMP TABLE tmp_pairs (county_id INT, question_id INT)"))
            conn.execute(text("INSERT INTO tmp_pairs (county_id, question_id) VALUES (:county_id, :question_id)"), recs)

            rows = conn.execute(text("""
                SELECT c.county, q.question
                FROM tmp_pairs p
                JOIN counties  c ON c.county_id  = p.county_id
                JOIN questions q ON q.question_id = p.question_id
                ORDER BY c.county, q.question_id;
            """)).all()
    except Exception as e:
        print(e)
        try:
            import streamlit as st
            st.error(f"Database error: {e}")
        except Exception:
            pass
        return pd.DataFrame(columns=["county", "question"])

    return pd.DataFrame(rows, columns=["county", "question"])

    
        
        