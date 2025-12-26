# ui/app.py
import streamlit as st
import pandas as pd
from core.healthcheck.health import run_all_checks
from app_helpers.Metadata_streamlit import render_metadata_page
from app_helpers.add_MoVE import admin_add_entity
from pathlib import Path
from app_helpers.adjust_db import render_adjust_database_page
from app_helpers.grab_data import build_query_interface
import os

css_path = Path(r"C:\Users\hodge\Downloads\Voting_DB\app_helpers\Style.css")

st.set_page_config(page_title="MoVE Admin", layout="wide")
st.title("MoVE Internal Admin Tool")

# Sidebar
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Choose a section:",
    ["Grab Data", "Adjust Database", "Extract and Insert MoVE Info", "Insert Census/Other Info", "Health Check", "Metadata Viewer"]
)

if page == "Grab Data":
    build_query_interface()

if page == "Adjust Database":
    render_adjust_database_page()
# Health Check tab
if page == "Health Check":
    st.header("Database Health Check")
    st.write("Click below to run all integrity and performance checks on MoVE.db.")
    if st.button("Run Health Check"):
        results = run_all_checks()
        st.success("Health check complete!")
        # Display results nicely
        st.json(results)

# Page routing
if page == "Metadata Viewer":
    render_metadata_page()

if page == "Extract and Insert MoVE Info":
    result = admin_add_entity()
    if result:
        st.write("Submitted Data:")
        st.json(result)
    
        