# Backend/metadata.py
from sqlalchemy import text
from Data.db import get_session
from datetime import datetime


# Function to add google sheet ID for later data extraction
def add_sheet_metadata(sheet_info):
    sheet_id = sheet_info.get("sheet_id")
    state = sheet_info.get("state")
    state_id = sheet_info.get("fips_code")
    with get_session() as session:
        stmt = text("""
        INSERT INTO sheet_ids (state_id, state, sheet_id)
        VALUES (:state_id, :state, :sheet_id)
        """)
        session.execute(stmt, {
            "state_id": state_id,
            "state": state,
            "sheet_id": sheet_id,
        })
        session.commit()
    with get_session() as session:
        query = text("SELECT * FROM sheet_ids")
        result = session.execute(query).fetchall()
        return result
