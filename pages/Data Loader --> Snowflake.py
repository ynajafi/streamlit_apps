import os
import time
import streamlit as st
import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.types import *
from snowflake.snowpark.functions import *
import snowflake.snowpark.functions as F
from streamlit_ace import st_ace, LANGUAGES, THEMES, KEYBINDINGS
from gsheetsdb import connect
from google.oauth2 import service_account


def main():

    st.set_page_config(page_title="Snowflake Data Loader", page_icon='❄️')

    # Session initialize
    if 'session' not in st.session_state:
        st.session_state.session = None
    
    if 'credentials' not in st.session_state:
        st.session_state.credentials = None

    if 'account' not in st.session_state:
        st.session_state.account = None

    if 'user' not in st.session_state:
        st.session_state.user = None

    if 'password' not in st.session_state:
        st.session_state.password = None

    if 'role' not in st.session_state:
        st.session_state.role = None

    if 'warehouse' not in st.session_state:
        st.session_state.warehouse = None

    if 'database' not in st.session_state:
        st.session_state.database = None

    if 'schema' not in st.session_state:
        st.session_state.schema = None

    if 'gsheetURL' not in st.session_state:
        st.session_state.gsheetURL = None

    if 'filtered_df' not in st.session_state:
        st.session_state.filtered_df = None

    # Functions
    @st.cache_resource(ttl=600)
    def run_query(query):
        rows = conn.execute(query, headers=1)
        rows = rows.fetchall()
        return rows
    
    def convert_column_datatype(df, column, new_datatype):
        try:
            if 'datetime' in column.lower():
                df[column] = pd.to_datetime(df[column])
            else:
                df[column] = df[column].astype(new_datatype)
            return df
        except ValueError:
            st.error(f"Unable to convert column '{column}' to {new_datatype}.")
            return df

    st.header("CSV Data + Google Sheets ➡ Snowflake ❄️")
    
    with st.sidebar:
        st.write("Upload a CSV file locally or Google Sheets.")
        if st.checkbox("Connect to Snowflake", help="Required if you are planning on loading data into your Snowflake account."):
            with st.form("Input Your Snowflake Account Credentials",clear_on_submit=True):
                st.session_state.account = st.text_input('Account Identifier', placeholder='abcdefg-12345678', help='https://docs.snowflake.com/en/user-guide/admin-account-identifier')
                st.session_state.user = st.text_input('User')
                st.session_state.password = st.text_input('Password', type='password')
                st.session_state.database = st.text_input('Database')
                st.session_state.schema = st.text_input('Schema')
                st.session_state.warehouse = st.text_input('Warehouse')
                st.session_state.role = st.text_input("Role", placeholder='Optional')

                st.session_state.credentials = {
                'account': st.session_state.account,
                'user': st.session_state.user,
                'password': st.session_state.password,
                'role': st.session_state.role,
                'database': st.session_state.database,
                'schema': st.session_state.schema,
                'warehouse': st.session_state.warehouse
                }

                if st.form_submit_button('Connect'):
                    with st.spinner():
                        try:
                            st.session_state.session = Session.builder.configs(st.session_state.credentials).create()
                        
                        except:
                            st.error("Oops! An error occurred while connecting. Please check your credentials and try again.")

        session = st.session_state.session
        
        if session is not None:
            st.success('### Snowpark Session Open 🏂')
        else:
            st.warning('### Snowpark Session Closed 🔒')
        
        # Disconnect from Snowpark
        if session is not None:
            if st.button('Disconnect'):
                session.close()
                st.session_state.session = None
        
        TABLE_NAME = st.text_input("Provide a Table Name", placeholder='my_table', help='Required').upper()
        uploadedFILE = st.file_uploader("Local File", type=["csv"])
        st.session_state.gsheetURL = st.text_input('Private Google Sheets URL', 
                                  placeholder="https://docs.google.com/spreadsheets/...", 
                                  help='You must create a Google Cloud Service Account prior to this. See Streamlit Docs:\nhttps://docs.streamlit.io/knowledge-base/tutorials/databases')
        
        gsheetURL = st.session_state.gsheetURL
        
        if session is not None:
            if st.checkbox('Show Current Snowflake Tables', ):
                with st.spinner('Processing...'):
                    snowflakeTables = pd.DataFrame(session.sql('show tables').collect())[['name','rows','bytes','created_on']]
                    st.dataframe(snowflakeTables, hide_index=True)
    
    # Uploaded File Dataframe
    if uploadedFILE is not None:
        file_df = pd.read_csv(uploadedFILE)
        rows = st.slider("Number of Rows to Sample", min_value=10, max_value=200, value=10, step=1)
        columns = file_df.columns.tolist()
        select_cols = st.multiselect('Select Columns to View', options=columns, default=columns)

        if st.session_state.filtered_df is None:
            filtered_df = file_df[select_cols]
        else:
            filtered_df = st.session_state.filtered_df[select_cols]

        st.dataframe(filtered_df.head(rows),hide_index=True)

        if st.checkbox('Data Summary'):
            data_summary = filtered_df.describe().T
            data_summary['is_null'] = filtered_df.isna().sum()
            st.dataframe(data_summary)

        #selected_column = None
        if st.checkbox('Alter Data Types'):
            modifiedColumns = filtered_df.columns.tolist()
            selected_column = st.selectbox('Select Column', modifiedColumns)

            if selected_column:
                existing_dataype = str(filtered_df[selected_column].dtypes)

                col1, col2 = st.columns(2)

                with col1:
                    st.write('Existing Datatype:')
                    st.write(existing_dataype)

                with col2:
                    new_dataytpe = st.selectbox('Select New Datatype', ["int64", "float64", "object", "bool", "datetime64"])

                if st.button('Convert'):
                    st.session_state.filtered_df = convert_column_datatype(filtered_df, selected_column, new_dataytpe)
        
            filtered_df.dtypes
        
        # Snowflake Load               
        if TABLE_NAME == "":
            st.info('Please provide a table name before loading data into Snowflake.')
        elif session is None:
            st.info('You are not connect to Snowflake, please check your credentials.')
        elif session and st.session_state.database and st.session_state.schema and st.session_state.warehouse:
            if st.button('Load CSV to Snowflake'):
                with st.spinner(f'Loading {uploadedFILE.name} into {TABLE_NAME}'):
                    snowparkDf = session.write_pandas(filtered_df, TABLE_NAME, auto_create_table=True, overwrite=True)
                    st.success(f'{uploadedFILE.name} Successfully Loaded into {TABLE_NAME}', icon="👍🏽")

    elif gsheetURL is not None and gsheetURL != "":
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
            )

        conn = connect(credentials=credentials)

        with st.spinner("In progress..."):
            query = run_query(f'SELECT * FROM "{gsheetURL}"')
            sheetsPandasDF = pd.DataFrame(query)
            rows = st.slider("Number of Rows to Sample", min_value=10, max_value=200, value=10, step=1)
            columns = sheetsPandasDF.columns.tolist()
            select_cols = st.multiselect('Select Columns to View', options=columns, default=columns)


            if st.session_state.filtered_df is None:
                filtered_df = sheetsPandasDF[select_cols]
            else:
                filtered_df = st.session_state.filtered_df[select_cols]

            st.dataframe(filtered_df.head(rows),hide_index=True)

            if st.checkbox('Data Summary'):
                data_summary = filtered_df.describe().T
                data_summary['is_null'] = filtered_df.isna().sum()
                st.dataframe(data_summary)

            #selected_column = None
            if st.checkbox('Alter Data Types'):
                modifiedColumns = filtered_df.columns.tolist()
                selected_column = st.selectbox('Select Column', modifiedColumns)

                if selected_column:
                    existing_dataype = str(filtered_df[selected_column].dtypes)

                    col1, col2 = st.columns(2)

                    with col1:
                        st.write('Existing Datatype:')
                        st.write(existing_dataype)

                    with col2:
                        new_dataytpe = st.selectbox('Select New Datatype', ["int64", "float64", "object", "bool", "datetime64"])

                    if st.button('Convert'):
                        st.session_state.filtered_df = convert_column_datatype(filtered_df, selected_column, new_dataytpe)
            
                filtered_df.dtypes

        # Snowflake Load               
        if TABLE_NAME == "":
            st.info('Please provide a table name before loading data into Snowflake.')
        elif session is None:
            st.info('You are not connect to Snowflake, please check your credentials.')
        elif session and st.session_state.database and st.session_state.schema and st.session_state.warehouse:
            if st.button('Load CSV to Snowflake'):
                with st.spinner(f'Loading Google Sheet into {TABLE_NAME}'):
                    snowparkDf = session.write_pandas(filtered_df, TABLE_NAME, auto_create_table=True, overwrite=True)
                    st.success(f'Google Sheet Successfully Loaded into {TABLE_NAME}', icon="👍🏽")

    if session is not None:

        if st.checkbox('SQL Editor'):

            content = st_ace(
                placeholder=f"--select * from {TABLE_NAME}",
                language=LANGUAGES[145],
                theme=THEMES[27],
                keybinding=KEYBINDINGS[3],
                min_lines=24,
                key='run_query'
            )

            if content:
                try:
                    start_time = int(time.time())
                    st.subheader("Result")
                    st.text(content)
                    with st.spinner('Processing with Snowpark...'):
                        st.dataframe(session.sql(content).collect())
                    end_time = int(time.time())
                    elapsed_time = end_time - start_time
                    st.text(f'Completed in {elapsed_time} sec')
                except:
                    st.error('🚨 Please make sure you are connected to Snowpark')

if __name__ == '__main__':
    main()