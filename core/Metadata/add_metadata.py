# Backend/metadata.py
from sqlalchemy import text
from Data.db import get_session
from datetime import datetime

def add_table_metadata(
    table_name: str,
    title: str = None,
    description: str = None,
    owner: str = None,
    source_system: str = None,
    source_link: str = None
):
    
    # Validate input
    if not table_name or not table_name.strip():
        return {"success": False, "error": "Table name cannot be empty."}
    
    """
    Add a new record to table_metadata.
    Args:
        table_name (str): Name of the table (PRIMARY KEY).
        title (str, optional): Human-readable name of the table.
        description (str, optional): Explanation of the table's purpose.
        owner (str, optional): Person or team responsible.
        source_system (str, optional): Origin of the data.
        source_link (str, optional): URL or file path to source.
    Returns:
        dict: Status message.
    """
    query = text("""
        INSERT INTO table_metadata (
            table_name, title, description, owner, source_system, source_link, created_at, updated_at
        ) VALUES (
            :table_name, :title, :description, :owner, :source_system, :source_link, :created_at, :updated_at
        )
    """)
    params = {
        "table_name": table_name,
        "title": title,
        "description": description,
        "owner": owner,
        "source_system": source_system,
        "source_link": source_link,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    try:
        with get_session() as s:
            s.execute(query, params)
        return {"success": True, "message": f"Added metadata for table '{table_name}'"}
    except Exception as e:
        return {"success": False, "error": str(e)}

def add_column_metadata(
    table_name: str,
    column_name: str,
    data_type: str = None,
    is_primary_key: bool = False,
    is_foreign_key: bool = False,
    references_table: str = None,
    definition: str = None,
    source: str = None,
    unit: str = None,
    notes: str = None
):
    """
    Add a new record to column_metadata.
    Args:
        table_name (str): Name of the table containing the column (part of PRIMARY KEY).
        column_name (str): Name of the column (part of PRIMARY KEY).
        data_type (str, optional): Data type of the column.
        is_primary_key (bool, optional): Whether it's a primary key.
        is_foreign_key (bool, optional): Whether it's a foreign key.
        references_table (str, optional): Referenced table and column if foreign key.
        definition (str, optional): Explanation of the column's purpose.
        source (str, optional): Origin of the data.
        unit (str, optional): Measurement unit if applicable.
        notes (str, optional): Additional information.
        created_at (str, optional): Timestamp of creation.
        updated_at (str, optional): Timestamp of last update.
    Returns:
        dict: Status message.
    """

    if not table_name or not table_name.strip():
        return {"success": False, "error": "Table name cannot be empty."}
    if not column_name or not column_name.strip():
        return {"success": False, "error": "Column name cannot be empty."}
    
    query = text("""
        INSERT INTO column_metadata (
            table_name, column_name, data_type, is_primary_key, is_foreign_key, references_table,
            definition, source, unit, notes,
            created_at, updated_at
        ) VALUES (
            :table_name, :column_name, :data_type, :is_primary_key, :is_foreign_key, :references_table,
            :definition, :source, :unit, :notes,
            :created_at, :updated_at
        )
    """)
    params = {
        "table_name": table_name,
        "column_name": column_name,
        "data_type": data_type,
        "is_primary_key": 1 if is_primary_key else 0,
        "is_foreign_key": 1 if is_foreign_key else 0,
        "references_table": references_table,
        "definition": definition,
        "source": source,
        "unit": unit,
        "notes": notes,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    try:
        with get_session() as s:
            s.execute(query, params)
        return {"success": True, "message": f"Added metadata for table '{table_name}'"}
    except Exception as e:
        return {"success": False, "error": str(e)}
