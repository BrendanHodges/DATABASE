# Backend/metadata.py
from sqlalchemy import text
from Data.db import get_session
from datetime import datetime

def delete_table_metadata(table_name: str):
    """
    Delete a record from table_metadata by table_name.
    Args:
        table_name (str): Name of the table to delete (PRIMARY KEY).
    Returns:
        dict: Status message.
    """
    query = text("DELETE FROM table_metadata WHERE table_name = :table_name")
    params = {"table_name": table_name}

    try:
        with get_session() as s:
            result = s.execute(query, params)
            if result.rowcount == 0:
                return {"success": False, "error": f"No metadata found for table '{table_name}'"}
        return {"success": True, "message": f"Deleted metadata for table '{table_name}'"}
    except Exception as e:
        return {"success": False, "error": str(e)}

def delete_column_metadata(table_name: str, column_name: str):
    """
    Delete a record from column_metadata by table_name and column_name.
    Args:
        table_name (str): Name of the table (part of PRIMARY KEY).
        column_name (str): Name of the column to delete (part of PRIMARY KEY).
    Returns:
        dict: Status message.
    """
    query = text("DELETE FROM column_metadata WHERE table_name = :table_name AND column_name = :column_name")
    params = {"table_name": table_name, "column_name": column_name}

    try:
        with get_session() as s:
            result = s.execute(query, params)
            if result.rowcount == 0:
                return {"success": False, "error": f"No metadata found for column '{column_name}' in table '{table_name}'"}
        return {"success": True, "message": f"Deleted metadata for column '{column_name}' in table '{table_name}'"}
    except Exception as e:
        return {"success": False, "error": str(e)}