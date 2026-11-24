import streamlit as st

st.set_page_config(
    page_title="DataPilot AI Test",
    layout="wide",
)

st.title("DataPilot AI")
st.header("Observability Dashboard Test")

st.success("Streamlit is rendering successfully!")

st.metric("Total Requests", 20)
st.metric("Success Rate", "100%")