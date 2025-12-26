import pandas as pd
from sqlalchemy import text
from Data.db import get_session
from sqlalchemy import create_engine
import sqlite3

def add_column():
    with get_session() as session:
        session.execute(text("ALTER TABLE state_responses ADD COLUMN import_id INTEGER;"))
        session.commit()
    
# add_column()

def populate_state_id():
    with get_session() as session:
        session.execute(text("""
            UPDATE counties
            SET state_id = (
                SELECT state_id
                FROM states
                WHERE states.abbrev = counties.state_abbrev
            );
        """))
        session.commit()
# populate_state_id()

def add_to_import_id():
    with get_session() as session:
        session.execute(text("""
            UPDATE state_responses
            SET import_id = 7
            WHERE import_id IS NULL;
        """))
        session.commit()
# add_to_import_id()

def InsertNewRow():
    with get_session() as session:
        session.execute(text("""
            INSERT INTO imports (import_id, source_type, source_ref, created_at, inserted_rows, duplicate_rows, error_rows, status, notes, state)
            VALUES (11, 'New MoVE questions to question table', 'NA', CURRENT_TIMESTAMP, 5, 0, 0, 'done', 'NA', 'NA');
        """))
        session.commit()



def new_MoVE_questions():
    with get_session() as session:
        session.execute(text("""
            INSERT INTO questions (question, category, spreadsheet_idx)
            VALUES
            ('Alternative Voting: Vote Centers', 'Vote Centers', '(63,64)');
        """))
        session.commit()
new_MoVE_questions()

def delete_question():
    with get_session() as session:
        session.execute(text("""
            DELETE FROM questions
            WHERE question_id >= 22;
        """))
        session.commit()
