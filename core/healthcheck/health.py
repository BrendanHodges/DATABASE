# backend/health.py
from sqlalchemy import text
from Data.db import get_session

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
