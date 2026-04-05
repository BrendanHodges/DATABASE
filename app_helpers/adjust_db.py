import streamlit as st
import pandas as pd
from datetime import datetime
from Data.pipelines import grab_states_with_responses, delete_state_responses
from core.imports.add_imports import grab_import_by_offset, delete_last_import


def render_adjust_database_page():

    # ---------- helpers ----------
    def _reload():
        st.rerun()

    def _reset_flow():
        for k in list(st.session_state.keys()):
            if k.startswith("__adjdb__"):
                st.session_state.pop(k, None)
        st.rerun()

    def _record_to_df(rec: dict) -> pd.DataFrame:
        flat = dict(rec)
        if isinstance(flat.get("source_ref"), (dict, list)):
            flat["source_ref"] = str(flat["source_ref"])
        return pd.DataFrame([flat])

    # ---------- header ----------
    h1, h2 = st.columns([1, 0.30])
    with h1:
        st.header("Adjust Database")
        st.caption("Admin utilities to manage recent data operations.")
    with h2:
        cA, cB = st.columns(2)
        with cA:
            st.button("Reload", help="Re-query and refresh this page", on_click=_reload)
        with cB:
            st.button("Reset Flow", help="Clear this page's temporary state", on_click=_reset_flow)

       # ---------- section: delete selected recent import (single-fetch) ----------
    with st.container(border=True):
        st.markdown("### Delete a Recent Import")
        st.caption("Pick which recent import to remove. Your backend should perform the actual deletion/rollback.")

        # Choose the rank (1=most recent, 2=second, etc.)
        rank = st.selectbox(
            "Choose which recent import to manage",
            options=list(range(1, 21)),  # allow up to the 20th most recent
            format_func=lambda r: "#1 (most recent)" if r == 1 else f"#{r}",
            key="__adjdb__rank",
        )
        offset = rank - 1  # OFFSET 0 => most recent

        # Fetch exactly one row by offset
        error_msg = None
        selected_import = None
        try:
            selected_import = grab_import_by_offset(offset=offset)
        except Exception as e:
            error_msg = f"Failed to fetch import #{rank}: {e}"

        if error_msg:
            st.error(error_msg)
            st.stop()

        if not selected_import:
            st.info(
                "No import found at this rank. Try a smaller number (closer to most recent)."
            )
            st.stop()

        # Details
        with st.expander("🔎 View selected import details", expanded=True):
            st.dataframe(_record_to_df(selected_import), use_container_width=True, hide_index=True)

        st.warning("This action is permanent and cannot be undone from the UI.")

        # Confirmation (ONLY the DELETE phrase)
        col_phrase, col_btn, _ = st.columns([0.5, 0.25, 0.25])
        with col_phrase:
            phrase = st.text_input('Type **DELETE** to confirm', placeholder="DELETE", key="__adjdb__phrase")
        can_delete = phrase.strip().upper() == "DELETE"

        with col_btn:
            delete_clicked = st.button(
                f"❌ Delete Selected Import (ID {selected_import.get('import_id','—')})",
                type="primary",
                disabled=not can_delete,
                help="Deletes the chosen import.",
            )

        if delete_clicked:
            try:
                result = delete_last_import(selected_import.get("import_id"), selected_import.get("source_type"), selected_import.get("state"))
                if isinstance(result, dict) and result.get("success"):
                    st.success(result.get("message", "Deleted selected import."))
                else:
                    msg = (result or {}).get("message", "Failed to delete the selected import.")
                    st.error(msg)
            except Exception as e:
                st.error(f"Error during deletion: {e}")
    st.divider()

         # ---------- section: delete all responses for a state ----------
    with st.container(border=True):
        st.markdown("### Delete A State's Responses")
        st.caption("Delete all responses for a specific state. Your backend should perform the actual deletion/rollback.")

        error_msg = None
        states = []

        try:
            # Placeholder: should return something like
            # [{"state_id": "24", "state_name": "Maryland"}, ...]
            states = grab_states_with_responses()
        except Exception as e:
            error_msg = f"Failed to fetch states: {e}"

        if error_msg:
            st.error(error_msg)
            st.stop()

        if not states:
            st.info("No states with responses were found.")
            st.stop()

        selected_state = st.selectbox(
            "Choose a state to delete responses for",
            options=states,
            format_func=lambda s: f"{s.get('state_name', 'Unknown')} ({s.get('state_id', '—')})",
            key="__adjdb__state_select",
        )

        state_id = selected_state.get("state_id")
        state_name = selected_state.get("state_name", "Unknown")

        with st.expander("🔎 View selected state details", expanded=True):
            st.write(f"State: {state_name}")
            st.write(f"State ID: {state_id}")
            st.write(f"Number of responses: {selected_state.get('response_count', '—')}")

        st.warning(f"This will permanently delete all responses for {state_name}.")

        # Confirmation phrase
        col_phrase, col_btn, _ = st.columns([0.5, 0.25, 0.25])
        with col_phrase:
            phrase = st.text_input(
                f'Type **DELETE {state_name.upper()}** to confirm',
                placeholder=f"DELETE {state_name.upper()}",
                key="__adjdb__state_delete_phrase"
            )

        can_delete = phrase.strip().upper() == f"DELETE {state_name.upper()}"

        with col_btn:
            delete_clicked = st.button(
                f"❌ Delete {state_name} Responses",
                type="primary",
                disabled=not can_delete,
                help="Deletes all responses for the selected state.",
                key="__adjdb__delete_state_btn"
            )

        if delete_clicked:
            try:
                # Placeholder function
                result = delete_state_responses(state_id)

                if isinstance(result, dict) and result.get("success") == True:
                    st.success(result.get("message", f"Deleted all responses for {state_name}."))
                else:
                    msg = (result or {}).get(
                        "message",
                        f"Failed to delete responses for {state_name}."
                    )
                    st.error(msg)
            except Exception as e:
                st.error(f"Error during state response deletion: {e}")