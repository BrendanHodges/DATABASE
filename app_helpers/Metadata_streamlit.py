# UI/metadata_page.py
import streamlit as st
import pandas as pd
from core.Metadata.add_metadata import add_table_metadata, add_column_metadata
from core.Metadata.delete_metadata import delete_table_metadata, delete_column_metadata
import Data.pipelines as p

def render_metadata_page():
    st.header("Database Metadata")
    st.write("View and manage table/column metadata stored in your database.")

    # --- Display metadata ---
    if st.button("Load Metadata", key="load_meta"):
        table_meta = p.get_table_metadata()
        col_meta = p.get_column_metadata()

        st.subheader("Table Metadata")
        if table_meta:
            st.dataframe(pd.DataFrame(table_meta), use_container_width=True)
        else:
            st.info("No table metadata found.")

        st.subheader("Column Metadata")
        if col_meta:
            st.dataframe(pd.DataFrame(col_meta), use_container_width=True)
        else:
            st.info("No column metadata found.")

    # --- Add new table metadata ---
    st.subheader("Add New Metadata")

    with st.expander("Add Table Metadata", expanded=False):
        with st.form("add_table_form"):
            table_name = st.text_input("Table Name", key="tbl_name")
            title = st.text_input("Title", key="tbl_title")
            description = st.text_area("Description", key="tbl_desc")
            owner = st.text_input("Owner", key="tbl_owner")
            source_system = st.text_input("Source System", key="tbl_source_system")
            source_link = st.text_input("Source Link", key="tbl_source_link")
            submitted_table = st.form_submit_button("Add Table Metadata")

            if submitted_table:
                result = add_table_metadata(
                    table_name=table_name,
                    title=title,
                    description=description,
                    owner=owner,
                    source_system=source_system,
                    source_link=source_link,
                )
                if result.get("success"):
                    st.success(result.get("message", "Table metadata added."))
                else:
                    st.error(result.get("error", "Failed to add table metadata."))

    # --- Add new column metadata ---
    with st.expander("Add Column Metadata", expanded=False):
        with st.form("add_column_form"):
            table_name_c = st.text_input("Table Name", key="col_tbl_name")
            column_name = st.text_input("Column Name", key="col_name")
            data_type = st.text_input("Data Type", key="col_type")
            is_primary_key = st.checkbox("Is Primary Key", key="col_pk")
            is_foreign_key = st.checkbox("Is Foreign Key", key="col_fk")
            references_table = st.text_input("References (table.column)", key="col_ref")
            definition = st.text_area("Definition", key="col_def")
            source = st.text_input("Source", key="col_source")
            unit = st.text_input("Unit", key="col_unit")
            notes = st.text_area("Notes", key="col_notes")

            submitted_column = st.form_submit_button("Add Column Metadata")

            if submitted_column:
                # Convert booleans to ints if your DB expects 0/1
                result = add_column_metadata(
                    table_name=table_name_c,
                    column_name=column_name,
                    data_type=data_type,
                    is_primary_key=int(is_primary_key),
                    is_foreign_key=int(is_foreign_key),
                    references_table=references_table,
                    definition=definition,
                    source=source,
                    unit=unit,
                    notes=notes,
                )
                if result.get("success"):
                    st.success(result.get("message", "Column metadata added."))
                else:
                    st.error(result.get("error", "Failed to add column metadata."))

    st.subheader("Delete Metadata")

    with st.expander("Delete Table Metadata", expanded=False):
        with st.form("delete_table_form"):
            table_name = st.text_input("Table Name", key="tbl_name_del")
            submitted_table = st.form_submit_button("Delete Table Metadata")

            if submitted_table:
                result = delete_table_metadata(table_name=table_name)
                if result.get("success"):
                    st.success(result.get("message", "Table metadata deleted."))
                else:
                    st.error(result.get("error", "Failed to Delete table metadata."))

    # --- Add new column metadata ---
    with st.expander("Delete Column Metadata", expanded=False):
        with st.form("delete_column_form"):
            table_name_c = st.text_input("Table Name", key="col_tbl_name_del")
            column_name = st.text_input("Column Name", key="col_name_del")
            submitted_column = st.form_submit_button("Delete Column Metadata")

            if submitted_column:
                # Convert booleans to ints if your DB expects 0/1
                result = delete_column_metadata(
                    table_name=table_name_c,
                    column_name=column_name,
                )
                if result.get("success"):
                    st.success(result.get("message", "Column metadata deleted."))
                else:
                    st.error(result.get("error", "Failed to delete column metadata."))

