import streamlit as st
from core.MoVE.get_data import get_data_all_county
from core.MoVE.add_sheet import add_sheet_metadata
from Data.db import get_session
import pandas as pd
from sqlalchemy import text
from core.MoVE.add_data import add_move_dataframe
from core.imports.add_imports import add_imports_record, add_error_imports_record

def admin_add_entity():
    st.header("Extract and Upload MoVE Data")
    col1, col2 = st.columns(2)
    with col1:
        entity = st.radio(
            "What would you like to add?",
            ("County", "State", "New Spreadsheet"),
            horizontal=True,
            key="entity_choice",
        )
    with col2:
        mode = st.radio(
            "Question selection:",
            ("Grab All", "Grab specific questions") if entity != "New Spreadsheet" else ("Grab All",),
            horizontal=True,
            key="mode_choice",
        )

    result = None

    # ---------- Helpers ----------
    TEMP_KEYS_PREFIXES = ("__county_all__", "__county_specific__", "__state_all__", "__state_specific__")

    def _reset_temp_flow_keys():
        # Remove only temp keys created by this page/flow
        for k in list(st.session_state.keys()):
            if k.startswith(TEMP_KEYS_PREFIXES) or k in {
                "entity_choice", "mode_choice",
                "state_total", "county_name", "county_fips",
                "state_name", "state_abbr_state",
                "new_state", "new_sheet_id", "new_fips_code",
                "confirm_all_cnty", "confirm_all_state",
                "county_questions", "state_questions",
            }:
                st.session_state.pop(k, None)

    def _refresh_flow():
        _reset_temp_flow_keys()
        st.rerun()
        
    def render_confirm_block(state_key: str, result_payload: dict, df_preview: pd.DataFrame):
        dups_key = f"{state_key}_dups"

        with st.container(border=True):
            st.write("**Preview** – does this look correct?")
            st.dataframe(df_preview, use_container_width=True)  # updated from use_container_width

            c1, c2, c3 = st.columns(3)
            with c1:
                if st.button("✅ Confirm & Add to Database", key=f"{state_key}_confirm"):
                    try:
                        res = add_move_dataframe(df_preview, result_payload) or {}
                    except Exception as e:
                        res = {"success": False, "message": str(e)}

                    if res.get("success"):
                        st.success(res.get("message", "Data added to database."))

                        # store duplicates DataFrame in session
                        dups_df = res.get("duplicates")
                        st.session_state[dups_key] = dups_df

                        total = res.get("total_duplicates", len(dups_df) if dups_df is not None else 0)
                        add_imports_record(
                            "MoVE Data into responses table",
                            result_payload,
                            inserted_rows=len(df_preview) - total,
                            duplicate_rows=total,
                            error_rows=0,
                            status="done",
                            notes=res.get("message", "Data added to database.")
                        )

                        if dups_df is not None and not dups_df.empty:
                            st.caption(f"Skipped {total} duplicate row(s). See below.")
                        st.session_state.pop(state_key, None)
                    else:
                        st.error(res.get("message", "Failed to add data."))
                        add_error_imports_record(
                            "MoVE Data into responses table",
                            result_payload,
                            error_message=res.get("message", "Failed to add MoVE data.")
                        )

            with c2:
                if st.button("❌ Cancel", key=f"{state_key}_cancel"):
                    st.info("Canceled. Nothing added.")
                    st.session_state.pop(state_key, None)
                    st.session_state.pop(dups_key, None)

            with c3:
                st.button("🔄 Reset to Start", key=f"{state_key}_reset", on_click=_refresh_flow)

        # Render duplicates outside the button handler
        dups_df = st.session_state.get(dups_key)
        if isinstance(dups_df, pd.DataFrame) and not dups_df.empty:
            with st.expander(f"🔍 Show duplicates ({len(dups_df)})", expanded=False):
                st.dataframe(dups_df, use_container_width=True)


    def optional_questions():
        with get_session() as s:
            returned = s.execute(text("SELECT question, spreadsheet_idx FROM questions"))
            questions = pd.DataFrame(returned.fetchall(), columns=["question", "spreadsheet_idx"])
        st.dataframe(questions, use_container_width=True)
        if st.form_submit_button("Close Table"):
            st.experimental_rerun()

    def county_fields():
        st.text_input("State (e.g., Maryland)", key="state_total")
        st.text_input("County name (e.g., Baltimore County)", key="county_name")
        st.text_input("County FIPS (optional)", key="county_fips")

    def state_fields():
        st.text_input("State name (e.g., Maryland)", key="state_name")
        st.text_input("State abbreviation (required now) (e.g., MD)", key="state_abbr_state")

    # ---------- County + Grab All ----------
    if entity == "County" and mode == "Grab All":
        with st.form(key="county_all_form"):
            county_fields()
            st.checkbox("Confirm: include ALL questions for this county", value=False, key="confirm_all_cnty")
            submitted = st.form_submit_button("Submit")

            if submitted:
                missing = []
                if not st.session_state.get("state_total", "").strip(): missing.append("State")
                if not st.session_state.get("county_name", "").strip(): missing.append("County name")
                if not st.session_state.get("confirm_all_cnty", False):
                    st.error("Please confirm that you want to include ALL questions for this county.")
                if missing:
                    for label in missing:
                        st.error(f"Please fill out **{label}**.")
                elif st.session_state.get("confirm_all_cnty", False):
                    result = {
                        "entity": "county",
                        "mode": "all",
                        "state": st.session_state.get("state_total", "").strip(),
                        "county_name": st.session_state.get("county_name", "").strip(),
                        "confirm_all": st.session_state.get("confirm_all_cnty", False),
                    }
                    data = get_data_all_county(result)
                    df = pd.DataFrame(data) if not isinstance(data, pd.DataFrame) else data
                    st.session_state["__county_all__"] = {"result": result, "df": df.to_dict("records")}

        if "__county_all__" in st.session_state:
            payload = st.session_state["__county_all__"]["result"]
            df_prev = pd.DataFrame(st.session_state["__county_all__"]["df"])
            render_confirm_block("__county_all__", payload, df_prev)

    # ---------- County + Specific ----------
    elif entity == "County" and mode == "Grab specific questions":
        with st.form(key="county_specific_form"):
            county_fields()
            st.text_area("Questions names or IDs", placeholder="e.g., Q1, Q7, Q12", key="county_questions")
            submitted = st.form_submit_button("Submit")
            view_btn = st.form_submit_button("View Questions you can add")

            if submitted:
                qs_raw = st.session_state.get("county_questions", "")
                questions = [q.strip() for q in qs_raw.split(",") if q.strip()]
                missing = []
                if not st.session_state.get("state_total", "").strip(): missing.append("State")
                if not st.session_state.get("county_name", "").strip(): missing.append("County name")
                if not questions: missing.append("Questions")
                if missing:
                    for label in missing:
                        st.error(f"Please fill out **{label}**.")
                else:
                    result = {
                        "entity": "county",
                        "mode": "specific",
                        "state": st.session_state.get("state_total", "").strip(),
                        "county_name": st.session_state.get("county_name", "").strip(),
                        "county_fips": st.session_state.get("county_fips", "").strip(),
                        "questions": questions,
                    }
                    data = get_data_all_county(result)
                    df = pd.DataFrame(data) if not isinstance(data, pd.DataFrame) else data
                    st.session_state["__county_specific__"] = {"result": result, "df": df.to_dict("records")}

            if view_btn:
                optional_questions()

        if "__county_specific__" in st.session_state:
            payload = st.session_state["__county_specific__"]["result"]
            df_prev = pd.DataFrame(st.session_state["__county_specific__"]["df"])
            render_confirm_block("__county_specific__", payload, df_prev)

    # ---------- State + Grab All ----------
    elif entity == "State" and mode == "Grab All":
        with st.form(key="state_all_form"):
            state_fields()
            st.checkbox("Confirm: include ALL questions for this state", value=False, key="confirm_all_state")
            submitted = st.form_submit_button("Submit")

            if submitted:
                missing = []
                if not st.session_state.get("state_name", "").strip(): missing.append("State name")
                if not st.session_state.get("state_abbr_state", "").strip(): missing.append("State abbreviation")
                if not st.session_state.get("confirm_all_state", False):
                    st.error("Please confirm that you want to include ALL questions for this state.")
                if missing:
                    for label in missing:
                        st.error(f"Please fill out **{label}**.")
                elif st.session_state.get("confirm_all_state", False):
                    result = {
                        "entity": "state",
                        "mode": "all",
                        "state": st.session_state.get("state_name", "").strip(),
                        "state_abbr": st.session_state.get("state_abbr_state", "").strip(),
                        "confirm_all": st.session_state.get("confirm_all_state", False),
                    }
                    data = get_data_all_county(result)
                    df = pd.DataFrame(data) if not isinstance(data, pd.DataFrame) else data
                    st.session_state["__state_all__"] = {"result": result, "df": df.to_dict("records")}

        if "__state_all__" in st.session_state:
            payload = st.session_state["__state_all__"]["result"]
            df_prev = pd.DataFrame(st.session_state["__state_all__"]["df"])
            render_confirm_block("__state_all__", payload, df_prev)

    # ---------- State + Specific ----------
    elif entity == "State" and mode == "Grab specific questions":
        with st.form(key="state_specific_form"):
            state_fields()
            st.text_area(
                "Add a MoVE question (comma-separated)",
                placeholder="e.g., Q1, Q7, Q12",
                key="state_questions",
            )
            submitted = st.form_submit_button("Submit")
            view_btn = st.form_submit_button("View Questions you can add")

            if submitted:
                qs_raw = st.session_state.get("state_questions", "")
                questions = [q.strip() for q in qs_raw.split(",") if q.strip()]
                missing = []
                if not st.session_state.get("state_name", "").strip(): missing.append("State name")
                if not st.session_state.get("state_abbr_state", "").strip(): missing.append("State abbreviation")
                if not questions: missing.append("Questions")
                if missing:
                    for label in missing:
                        st.error(f"Please fill out **{label}**.")
                else:
                    result = {
                        "entity": "state",
                        "mode": "specific",
                        "state": st.session_state.get("state_name", "").strip(),
                        "state_abbr": st.session_state.get("state_abbr_state", "").strip(),
                        "questions": questions,
                    }
                    data = get_data_all_county(result)
                    df = pd.DataFrame(data) if not isinstance(data, pd.DataFrame) else data
                    st.session_state["__state_specific__"] = {"result": result, "df": df.to_dict("records")}

            if view_btn:
                optional_questions()

        if "__state_specific__" in st.session_state:
            payload = st.session_state["__state_specific__"]["result"]
            df_prev = pd.DataFrame(st.session_state["__state_specific__"]["df"])
            render_confirm_block("__state_specific__", payload, df_prev)

    # ---------- New Spreadsheet (leave as-is: no confirm flow) ----------
    else:
        with st.form(key="new_spreadsheet_form"):
            st.text_input("State (e.g., Maryland)", key="new_state")
            st.text_input("Spreadsheet ID (from the URL)", key="new_sheet_id")
            st.text_input("State FIPS code (e.g., 24 for Maryland)", key="new_fips_code")
            submitted = st.form_submit_button("Submit")

            if submitted:
                missing = []
                if not st.session_state.get("new_state", "").strip(): missing.append("State")
                if not st.session_state.get("new_sheet_id", "").strip(): missing.append("Spreadsheet ID")
                if not st.session_state.get("new_fips_code", "").strip(): missing.append("State FIPS code")
                if missing:
                    for label in missing:
                        st.error(f"Please fill out **{label}**.")
                else:
                    result = {
                        "entity": "new_spreadsheet",
                        "state": st.session_state.get("new_state", "").strip(),
                        "sheet_id": st.session_state.get("new_sheet_id", "").strip(),
                        "fips_code": st.session_state.get("new_fips_code", "").strip(),
                    }
                    data = add_sheet_metadata(result)
                    add_imports_record("MoVE Spreadsheet ID", result, inserted_rows=1, duplicate_rows=0, error_rows=0, status='done', notes="Added new spreadsheet ID.")
                    st.write("Current sheet_ids in database:")
                    st.dataframe(pd.DataFrame(data), use_container_width=True)

    return result