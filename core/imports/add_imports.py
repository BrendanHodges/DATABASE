
import Data.pipelines as p
import pandas as pd
from sqlalchemy import text
import streamlit as st
from Data.db import get_session
from sqlalchemy import text

def add_imports_record(
    source_type,
    source_ref,
    inserted_rows=0,
    duplicate_rows=0,
    error_rows=0,
    status='done',
    notes=None
):
    # If caller passed a dict like {"state": "..."} and this specific source_type, map to sheet_id
    if (source_type == "MoVE Data into responses table" or source_type =="MoVE Spreadsheet ID"):
        state = source_ref.get('state')
        if state:
            source_ref = p.get_sheet_id(state)

    # Ensure numeric fields are ints (not strings like "12,789")
    def _to_int(x):
        if x is None: 
            return 0
        if isinstance(x, str):
            return int(x.replace(",", "").strip())
        return int(x)

    params = {
        "source_type": source_type,
        "source_ref": source_ref,
        "inserted_rows": _to_int(inserted_rows),
        "duplicate_rows": _to_int(duplicate_rows),
        "error_rows": _to_int(error_rows),
        "status": status,
        "notes": notes,
        "state": state
    }

    with get_session() as s:
        # Try RETURNING first (SQLite >= 3.35). If not supported, fallback to last_insert_rowid().
        try:
            result = s.execute(text("""
                INSERT INTO imports (source_type, source_ref, inserted_rows, duplicate_rows, error_rows, status, notes, state)
                VALUES (:source_type, :source_ref, :inserted_rows, :duplicate_rows, :error_rows, :status, :notes, :state)
                RETURNING import_id
            """), params)
            import_id = result.scalar_one()
        except Exception:
            # Fallback path for older SQLite builds
            s.execute(text("""
                INSERT INTO imports (source_type, source_ref, inserted_rows, duplicate_rows, error_rows, status, notes, state)
                VALUES (:source_type, :source_ref, :inserted_rows, :duplicate_rows, :error_rows, :status, :notes, :state)
            """), params)
            import_id = s.execute(text("SELECT last_insert_rowid()")).scalar_one()

        # Update responses only where import_id is currently NULL
        s.execute(
            text("""
                UPDATE responses
                SET import_id = :import_id
                WHERE import_id IS NULL
            """),
            {"import_id": import_id}
        )

        s.commit()

    return import_id


def add_error_imports_record(source_type, source_ref, error_message):
    if source_type == "MoVE Data into responses table" and isinstance(source_ref, dict):
        state = source_ref.get('state')
        if state:
            source_ref = p.get_sheet_id(state)


    params = {
        "source_type": source_type,
        "source_ref": source_ref,
        "error_message": error_message
    }

    with get_session() as s:
        s.execute(text("""
            INSERT INTO import_errors (source_type, source_ref, error_message)
            VALUES (:source_type, :source_ref, :error_message)
        """), params)
        s.commit()
    return True

def grab_import_by_offset(offset: int = 0):
    """
    offset=0 -> last import
    offset=1 -> second to last
    offset=2 -> third to last, etc.
    """
    try:
        with get_session() as s:
            row = (
                s.execute(text("""
                    SELECT import_id, source_type, source_ref, inserted_rows,
                           duplicate_rows, error_rows, status, notes, created_at, state
                    FROM imports
                    ORDER BY import_id DESC
                    LIMIT 1 OFFSET :offset
                """), {"offset": offset})
                .mappings()
                .first()
            )
            return dict(row.items()) if row else None
    except Exception as e:
        print(f"Error fetching import with offset {offset}: {e}")
        return None
        
def delete_last_import(import_id):
    with get_session() as s:
        try:
            # First, nullify import_id in responses linked to this import
            s.execute(text("""
                DELETE FROM responses
                WHERE import_id = :import_id
            """), {"import_id": import_id})

            # Then delete the import record itself
            s.execute(text("""
                DELETE FROM imports
                WHERE import_id = :import_id
            """), {"import_id": import_id})
        except Exception as e:
            return {"success": False, "message": str(e)}

        s.commit()
        return {"success": True, "message": f"Import {import_id} deleted successfully."}
    return True


def delete_this():
    with get_session() as s:
        s.execute(text("DELETE FROM sheet_ids WHERE state = :state"), {"state": "Lousiana"})
        s.commit()
    return True

delete_this()