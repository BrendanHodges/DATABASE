# db.py
from contextlib import contextmanager
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker

DB_PATH = r"C:\Users\hodge\Downloads\Voting_DB\MoVE.db"  # put the file next to this script or use an absolute path
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