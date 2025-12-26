import pandas as pd
from Data.db import get_session
from sqlalchemy import text
import re
from sqlalchemy import text, bindparam
import re, unicodedata
from difflib import SequenceMatcher
from sqlalchemy import text
import os
import duckdb


def add_questions(question_list):
    query = text("""
        INSERT INTO questions (question_ID, question, category, spreadsheet_idx)
        VALUES (:question_ID, :question, :category, :spreadsheet_idx);
    """)

    with get_session() as s:
        s.execute(query, question_list)   # executemany-style
        s.commit()    

add_questions([
    {
        "question_ID": 26,
        "question": "Voters are informed about polling place closures and re-location",
        "category": "Informed_Voters",
        "spreadsheet_idx": 18
    },
    {
        "question_ID": 27,
        "question": "Voters informed about changes in drop box locations and times",
        "category": "Informed_Voters",
        "spreadsheet_idx": 19
    },
    {
        "question_ID": 28,
        "question": "Voters informed about changes in early voting locations and times",
        "category": "Informed_Voters",
        "spreadsheet_idx": 20
    },
    {
        "question_ID": 29,
        "question": "Voters informed about vote center locations and times",
        "category": "Informed_Voters",
        "spreadsheet_idx": 21
    },
    {
        "question_ID": 30,
        "question": "Voters informed about changes in early voting locations and times",
        "category": "Informed_Voters",
        "spreadsheet_idx": 20
    },
    {
        "question_ID": 31,
        "question": "Residential Mail Delivery",
        "category": "Mail_delivery",
        "spreadsheet_idx": '(72, 73)'
    },
    {
        "question_ID": 32,
        "question": "Location Of Post Office",
        "category": "Post Office Location",
        "spreadsheet_idx": '(80, 81, 82, 83, 84, 85)'
    },
    {
        "question_ID": 33,
        "question": "State Support For Running Elections",
        "category": "State Support",
        "spreadsheet_idx": '(100, 101, 102, 103)'
    },
    {
        "question_ID": 34,
        "question": "State Technology Funding",
        "category": "Technology State Funding",
        "spreadsheet_idx": '(106, 107)'
    }
])
