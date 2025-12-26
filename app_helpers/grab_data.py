import streamlit as st
import pandas as pd
from typing import Dict, Any
from Data.pipelines import run_query, grab_variables, grab_categories, normalize_state_input

# =========================
# HELPER FUNCTIONS HERE
# =========================
def checkbox_multi(label: str, options: list[str], cols: int = 3) -> list[str]:
    st.subheader(label)

    # Optional quick filter
    q = st.text_input("Search variables", placeholder="Type to filter…").strip().lower()
    filtered = [o for o in options if q in o.lower()] if q else options

    # Select/Clear controls
    c1, c2 = st.columns([1, 1])
    with c1:
        if st.button("Select all"):
            for i, opt in enumerate(filtered):
                st.session_state[f"var_cb_{opt}"] = True
    with c2:
        if st.button("Clear all"):
            for i, opt in enumerate(filtered):
                st.session_state[f"var_cb_{opt}"] = False

    # Checkbox grid
    cols_list = st.columns(cols)
    for i, opt in enumerate(filtered):
        with cols_list[i % cols]:
            st.checkbox(
                opt,
                key=f"var_cb_{opt}",
                value=st.session_state.get(f"var_cb_{opt}", opt == "Overall Population")
            )

    # Collect selections (preserve only those in the full list)
    selected = [opt for opt in options if st.session_state.get(f"var_cb_{opt}", False)]
    return selected

def build_query_interface():
    # st.set_page_config(page_title="MoVE: Query Interface", layout="wide")

    # =========================
    # 🎛️ INTERFACE
    # =========================
    st.title("MoVE: Build Your Query")

    # 🔹 Place filters in the main area (not sidebar)
    st.header("Filters")

    # ⬇️ Variable names selector
    variable_options = grab_variables()
    selected_variables = checkbox_multi("Census variables", variable_options, cols=3)

    # ⬇️ Categories selector
    category_options = grab_categories()
    category_map = {"Reg": "The Provision of Information About Registration (5 item additive scale)", 
                    "Voting": "The Provision of Information About Voting", 
                    "Abuses": "History of Voting Rights Abuses in County (3 item Likert scale)",
                    "pollworkers": "Availability of Poll Workers (6 item additive scale)",
                    "Registration": "Registration Drives (dichotomous 0/1)",
                    "dropboxes": "Alternative Voting: Drop Boxes (dichotomous 0/1)" ,
                    "Vote Centers": "Alternative Voting: Vote Centers (dichotomous 0/1)",
                    "Ease of Registration": "Ease of Registration (4 item additive scale)"}
    category_options = list(category_map.keys())
    default_cat = "Reg" if "Reg" in category_options else (category_options[0] if category_options else "")
    display_labels = [category_map[key] for key in category_options]

    # 🔹 Display descriptive text, but return the actual key
    selected_label = st.selectbox(
        "Question category",
        options=display_labels,
        index=category_options.index(default_cat)
    )

    # 🔹 Reverse-lookup to get the chosen key
    category = next(key for key, label in category_map.items() if label == selected_label)

    # ⌨️ Required: user types a state
    state_input_raw = st.text_input("State (required)", placeholder="e.g., MD")

    # Convert to list if commas present
    if "," in state_input_raw:
        state_input = [s.strip() for s in state_input_raw.split(",") if s.strip()]
    else:
        state_input = [state_input_raw.strip()] if state_input_raw.strip() else []

    # ▶️ Run
    run_clicked = st.button("Run query", type="primary")

    # =========================
    # ✅ WHERE USER RESPONSES ARE READY
    # =========================
    ready_inputs: Dict[str, Any] = {
        "state_input_raw": state_input,
        "state_ids": normalize_state_input(state_input),
        "selected_variables": selected_variables if selected_variables else None,
        "category": category,
    }

    with st.expander("Selections (debug)"):
        st.json(ready_inputs)

    # Guardrails & handoff
    if run_clicked:
        if not state_input:
            st.error("Please enter a state (state_ID or code).")
        elif ready_inputs["state_ids"] is None:
            st.error("Could not resolve the state you entered. Try a numeric state_ID or a supported code (e.g., MD).")
        elif not variable_options or not category_options:
            st.error("Variable and/or category options are empty. Implement the option loaders to proceed.")
        else:
            st.success("Inputs are valid. Handing off to your query function…")
            df = run_query(
                state_ids=ready_inputs["state_ids"],
                selected_variables=ready_inputs["selected_variables"],
                category=ready_inputs["category"]
            )

            if not df.empty:
                st.dataframe(df, use_container_width=True)
                st.download_button(
                    "Download CSV",
                    df.to_csv(index=False).encode("utf-8"),
                    "results.csv",
                    "text/csv"
                )
            else:
                st.info("No rows matched your filters.")
