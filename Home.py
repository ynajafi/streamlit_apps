import streamlit as st
#from snowflake.snowpark import Session

st.set_page_config(
    page_title='Home',
    page_icon="👋"
)
st.header('Snowflake Data Loader & AI Query App')

st.sidebar.info('Select Your Path',icon='👆🏽')

st.write('Use the tabs on the left')
st.write('Load data from your local CSV or Google Sheets directly to Snowflake ❄️')
st.write('Query your data using OpenAI 🤖 and Langchain 🦜⛓️')

st.write('Google Sheets requires additional security parameters')
st.write('To query your data with AI you must create and provide a OpenAI API Key')

