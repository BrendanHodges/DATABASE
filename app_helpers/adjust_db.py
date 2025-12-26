import streamlit as st
import pandas as pd
from datetime import datetime
from core.imports.add_imports import grab_import_by_offset, delete_last_import


def render_adjust_database_page():
    """
    Adjust Database – Delete Last Import (DB-backed if callables provided)

    Args (optional):
      fetch_last_import_fn: () -> dict|None
      delete_last_import_fn: (import_id:int) -> {"success": bool, "message": str}
    """

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

        # Metrics row for the selected import
        m1, m2, m3, m4 = st.columns(4)
        with m1:
            st.metric("Import ID", selected_import.get("import_id", "—"))
        with m2:
            st.metric("Inserted Rows", selected_import.get("inserted_rows", 0))
        with m3:
            st.metric("Type of import", selected_import.get("source_type", "—"))
        with m4:
            st.metric("State", selected_import.get("state", "—"))

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
                result = delete_last_import(selected_import.get("import_id"))
                if isinstance(result, dict) and result.get("success"):
                    st.success(result.get("message", "Deleted selected import."))
                    _reload()
                else:
                    msg = (result or {}).get("message", "Failed to delete the selected import.")
                    st.error(msg)
            except Exception as e:
                st.error(f"Error during deletion: {e}")
