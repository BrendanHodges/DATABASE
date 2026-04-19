from Data.db import get_session
from sqlalchemy import text

with get_session() as conn:
    query = """
        DELETE FROM responses WHERE county_id = 24001;
    """
    conn.execute(text(query))
    conn.commit()