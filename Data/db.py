import os
from contextlib import contextmanager
from pathlib import Path
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

# First try the server environment variable
DB_PATH = os.getenv("MOVE_DB_PATH")

# Fallback for local development if env var is not set
if not DB_PATH:
    DB_PATH = r"C:\Users\Brend\Documents\DATABASE\MoVE - Safe.db"

engine = create_engine(f"sqlite:///{DB_PATH}", future=True)

# Ensure SQLite enforces foreign keys
@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_conn, _):
    cur = dbapi_conn.cursor()
    cur.execute("PRAGMA foreign_keys = ON;")
    cur.close()

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

@contextmanager
def get_session():
    s = SessionLocal()
    try:
        yield s
        s.commit()
    except:
        s.rollback()
        raise
    finally:
        s.close()