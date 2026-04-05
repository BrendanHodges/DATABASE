# ui/app.py
import streamlit as st
import pandas as pd
from core.healthcheck.health import run_all_checks, advanced_health, state_sums
from app_helpers.Metadata_streamlit import render_metadata_page
from app_helpers.add_MoVE import admin_add_entity
from pathlib import Path
from app_helpers.adjust_db import render_adjust_database_page
from Data.pipelines import grab_states_with_responses, response_stats
import os

css_path = Path(r"C:\Users\hodge\Downloads\Voting_DB\app_helpers\Style.css")

st.set_page_config(page_title="MoVE Admin", layout="wide")
st.title("MoVE Internal Admin Tool")

# Sidebar
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Choose a section:",
    ["Home", "Adjust Database", "Extract and Insert MoVE Info", "Health and Data Summary's", "Metadata Viewer"]
)

if page == "Home":
    st.write("Welcome to the MoVE Admin Tool!")

    st.divider()
    st.subheader("Database Summary")
    db_summary = {}
    try:
        all_states = grab_states_with_responses()
        all_states_count = len(all_states)
        response_data = response_stats()
        all_counties = response_data.get("total_counties", "—")
        total_responses = response_data.get("total_responses", "—")
        total_questions = response_data.get("total_questions", "—")
        total_categories = response_data.get("total_categories", "—")
        categories = response_data.get("categories", "—")
        db_summary = {
            "total_states": all_states_count,
            "total_counties": all_counties,
            "total_questions": total_questions,
            "total_MoVE_categories": total_categories,
            "total_responses": total_responses,
            "states": ", ".join([s["state_name"] for s in all_states]),
            "MoVE categories (as named in database)": categories
        }


        st.json(db_summary)
    except Exception as e:
        st.warning(f"Could not fetch database summary: {e}")


if page == "Adjust Database":
    render_adjust_database_page()

# Health Check tab
if page == "Health and Data Summary's":
    st.header("Database Health Check")
    st.write("Click below to run all integrity and performance checks on MoVE.db.")

    if st.button("Run Database Integrity Health Check"):
        results = run_all_checks()
        st.success("Health check complete!")
        st.json(results)

    if st.button("Check Data and County Correctness"):
        results = advanced_health()
        st.success("Advanced health check complete!")

        # Missing questions
        st.subheader("Missing Questions")
        if not results["missing_questions"]:
            st.info("No counties with missing questions were found. All counties in response table had an answer for every question in the question table")
        else:
            st.warning(f"{len(results['missing_questions'])} counties with missing questions found.")
            for row in results["missing_questions"]:
                st.write(
                    f"{row._mapping['name']} ({row._mapping['state_id']}, {row._mapping['county_id']}) "
                    f"-> Missing questions: {row._mapping['missing_questions']}"
                )


        # Duplicate county-question pairs
        st.subheader("Duplicate County-Question Pairs")
        if not results["duplicate_questions"]:
            st.info("No duplicate county-question pairs were found. No counties answered the same question twice.")
        else:
            st.warning(f"{len(results['duplicate_questions'])} duplicate county-question rows found.")
            for row in results["duplicate_questions"]:
                st.write(
                    f"{row._mapping['county_name']} ({row._mapping['state_id']}, {row._mapping['county_id']}) "
                    f"-> Question {row._mapping['question_id']} appears "
                    f"{row._mapping['response_count']} times"
                )


        # High values
        st.subheader("Unexpected High Values")
        if not results["high_values"]:
            st.info("No unexpected high values were found. All questions either answered 0 (No) or 1 (Yes)")
        else:
            st.warning(f"{len(results['high_values'])} rows with unexpected high values found.")
            for row in results["high_values"]:
                st.write(
                    f"{row._mapping['name']} ({row._mapping['state_id']}, {row._mapping['county_id']}) "
                    f"-> Question {row._mapping['question_id']}: {row._mapping['question']} "
                    f"(Value = {row._mapping['value']})"
                )

        st.subheader("States with Missing Counties in Response Table")
        if not results["every_county_in_state"]:
            st.info("No State in the response table has missing counties.")
        else:
            st.warning(f"{len(results['every_county_in_state'])} states with missing counties in response table found.")
            for row in results["every_county_in_state"]:
                st.write(
                    f"State {row._mapping['state_id']} ({row._mapping['state_abbrev']}) "
                    f"-> Missing counties: {row._mapping['missing_counties']}"
                )
    st.divider()
    st.subheader("State-Level Sums of Categories")
    state_abbr = st.text_input("Enter State Abbrer (Ex: CA, NY, etc.):")
    if state_abbr:
        summary = state_sums(state_abbr)
        st.dataframe(summary)



# Page routing
if page == "Metadata Viewer":
    render_metadata_page()
if page == "Extract and Insert MoVE Info":
    result = admin_add_entity()
    if result:
        st.write("Submitted Data:")
        st.json(result)
    
        