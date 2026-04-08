# backend/health.py
from sqlalchemy import text
from Data.db import get_session
import pandas as pd

def check_integrity():
    """Run PRAGMA integrity_check to ensure DB isn't corrupted."""
    with get_session() as s:
        result = s.execute(text("PRAGMA integrity_check;")).scalar()
    return {"integrity_check": result}

def check_foreign_keys():
    """Check if foreign keys enforcement is ON."""
    with get_session() as s:
        result = s.execute(text("PRAGMA foreign_keys;")).scalar()
    return {"foreign_keys_enabled": bool(result)}

def list_tables():
    """Get a list of all tables in the DB."""
    with get_session() as s:
        result = s.execute(text(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
        )).all()
    return {"tables": [row[0] for row in result]}

def analyze_indexes():
    """Run ANALYZE to refresh query planner statistics."""
    with get_session() as s:
        s.execute(text("ANALYZE;"))
    return {"analyze": "Indexes analyzed"}

def quick_check():
    """Run PRAGMA quick_check for a faster integrity test."""
    with get_session() as s:
        result = s.execute(text("PRAGMA quick_check;")).scalar()
    return {"quick_check": result}

def run_all_checks():
    """Run all checks and return results as a dict."""
    results = {}
    results.update(check_integrity())
    results.update(check_foreign_keys())
    results.update(list_tables())
    results.update(analyze_indexes())
    results.update(quick_check())
    return results

def counties_for_states():
    with get_session() as conn:
        query = """
                WITH response_states AS (
                    SELECT DISTINCT c.state_id
                    FROM responses r
                    JOIN counties c
                        ON r.county_id = c.county_id
                ),
                response_counties AS (
                    SELECT DISTINCT county_id
                    FROM responses
                )
                SELECT 
                    c.state_id, c.state,
                    COUNT(*) AS missing_counties
                FROM counties c
                JOIN response_states rs
                    ON c.state_id = rs.state_id
                LEFT JOIN response_counties rc
                    ON c.county_id = rc.county_id
                WHERE rc.county_id IS NULL
                GROUP BY c.state_id
                ORDER BY c.state_id;
            """
        rows = conn.execute(text(query)).fetchall()
    return rows

def counties_missing_questions():
    with get_session() as conn:
        query = """
                WITH expected_questions AS (
                    SELECT question_id FROM questions
                ),
                county_base AS (
                    SELECT DISTINCT county_id FROM responses
                )
                SELECT
                    c.name,
                    c.state_id,
                    c.county_id,
                    GROUP_CONCAT(q.question_id, ', ') AS missing_questions
                FROM county_base cb
                CROSS JOIN expected_questions q
                LEFT JOIN responses r
                    ON r.county_id = cb.county_id
                AND r.question_id = q.question_id
                JOIN counties c
                    ON c.county_id = cb.county_id
                WHERE r.question_id IS NULL AND q.question_id NOT IN (10)
                GROUP BY c.county_id, c.name, c.state_id
                ORDER BY c.state_id, c.name;
            """

        rows = conn.execute(text(query)).fetchall()
    return rows
    
def find_duplicate_county_questions():
    with get_session() as conn:
        query = """
                SELECT
                    r.county_id,
                    c.name AS county_name,
                    c.state_id,
                    r.question_id,
                    COUNT(*) AS response_count
                FROM responses r
                JOIN counties c
                    ON c.county_id = r.county_id
                GROUP BY r.county_id, c.name, c.state_id, r.question_id
                HAVING COUNT(*) > 1
                ORDER BY c.state_id, c.name, r.question_id;
            """

        rows = conn.execute(text(query)).fetchall()

    return rows

def find_high_values():
    with get_session() as conn:
        query = """
            SELECT c.county_id, c.name, c.state_id, q.question_id, q.question, r.value
            FROM responses r, counties c, questions q
            WHERE r.county_id = c.county_id
            AND r.question_id = q.question_id
            AND q.question_id NOT IN (32, 33, 10)
            AND r.value >= 2
            ORDER BY c.state_id, c.name;"""
        
        rows = conn.execute(text(query)).fetchall()

    return rows

def state_sums(state_abbrev):
    def get_move_equation_scores(state) -> pd.DataFrame:
        print(state)
        with get_session() as conn:
            query = """
                SELECT
                    C.county_id,
                    C.name AS county_name,
                    C.state_id AS state_id,
                    Q.question_id,
                    Q.question,
                    Q.category,
                    R.value AS response_value
                FROM Responses R
                JOIN Counties C ON C.county_id = R.county_id
                JOIN Questions Q ON Q.question_id = R.question_id
                WHERE C.state_abbrev = :state AND q.question_id NOT IN (10)
                ORDER BY C.county_id, Q.question_id
                """
            rows = conn.execute(text(query), {"state": state}).fetchall()
            conn.close()
        return pd.DataFrame(rows)
    
    def build_category_scores(df: pd.DataFrame) -> pd.DataFrame:
        df_cat = (
            df.groupby(["county_id", "county_name", "state_id", "category"], as_index=False)["response_value"]
            .sum()
            .rename(columns={"response_value": "category_score"})
        )

        print(df_cat)

        df_wide = (
            df_cat.pivot_table(
                index=["county_id", "county_name", "state_id"],
                columns="category",
                values="category_score",
                aggfunc="first"
            )
            .reset_index()
        )

        df_wide.columns.name = None
        df_wide.drop(columns = ['county_id', 'state_id'], inplace=True)
        return df_wide
    df = get_move_equation_scores(state_abbrev)
    df_wide = build_category_scores(df)
    return df_wide

def advanced_health():
    results = {}
    results['missing_questions'] = counties_missing_questions()
    results['duplicate_questions'] = find_duplicate_county_questions()
    results['high_values'] = find_high_values()
    results['every_county_in_state'] = counties_for_states()

    return results

