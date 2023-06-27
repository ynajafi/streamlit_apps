# Imports
import os
import streamlit as st
from streamlit_chat import message
import pandas as pd
import openai
from dotenv import load_dotenv, find_dotenv
from langchain.agents import create_pandas_dataframe_agent
from langchain.llms import OpenAI
from langchain.callbacks import get_openai_callback
import openai
from streamlit_ace import st_ace
import time

def main():

    # Title and description
    st.set_page_config(page_title='Pandas Query App',page_icon='🐼')

    if 'api_key' not in st.session_state:
        st.session_state.api_key = None

    if 'session' not in st.session_state:
        st.session_state.session = None
    
    if 'snowpark' not in st.session_state:
        st.session_state.snowpark = None

    if 'snowflakeTables' not in st.session_state:
        st.session_state.snowflakeTables = None
    
    if 'snowflakeDF' not in st.session_state:
        st.session_state.snowflakeDF = None
    
    if 'fileDF' not in st.session_state:
        st.session_state.fileDF = None

    # Define Generated and Past Chat Arrays
    if 'generated' not in st.session_state: 
        st.session_state.generated = []

    if 'past' not in st.session_state: 
        st.session_state.past = []
    
    if 'tokens' not in st.session_state:
        st.session_state.tokens = []
    
    if 'cost' not in st.session_state:
        st.session_state.cost = []

    st.title("Dataframe Query App")
    st.write("Upload a CSV file and query answers from your data.")

    # CSS for chat bubbles
    chat_bubble_style = """
    .user-bubble {
        background-color: #DCF8C6;
        color: #1C824C;
        padding: 8px 12px;
        border-radius: 15px;
        display: inline-block;
        max-width: 70%;
    }

    .gpt-bubble {
        background-color: #F3F3F3;
        color: #404040;
        padding: 8px 12px;
        border-radius: 15px;
        display: inline-block;
        max-width: 70%;
        text-align: right;
    }
    """

    st.write(f'<style>{chat_bubble_style}</style>', unsafe_allow_html=True)

    session = st.session_state.session

    file = None

    with st.sidebar:   
        openai.api_key = st.text_input('OpenAI API Key', placeholder='Input API Key and Press Enter')

        if not openai.api_key.startswith('sk-'):
            st.warning('Please enter your OpenAI API key!', icon='⚠')
        else:
            st.write('Query History')
            historyDF = pd.DataFrame({'Questions': st.session_state.past, 'Answers': st.session_state.generated, 'Cost': st.session_state.cost})
            st.dataframe(historyDF)
            st.write(f'OpenAI Total Spend: ${sum(st.session_state.cost): .4f} USD')

    if openai.api_key.startswith('sk-'):
        file = st.file_uploader("Upload CSV file:",type=["csv"])

    if session is not None and openai.api_key.startswith('sk-'):
        if st.checkbox('Load Data from Snowflake'):
            with st.spinner('Processing...'):
                snowflakeTables = session.sql('show tables').collect()
                if snowflakeTables:
                    st.session_state.snowflakeTables = pd.DataFrame(snowflakeTables)
                    st.dataframe(st.session_state.snowflakeTables, hide_index=True)
                else:
                    st.warning('No tables found in Snowflake.')

                tableSelect = st.session_state.snowflakeTables['name']
                selectTable = st.selectbox('Select Table', tableSelect)
                if st.button('Submit'):
                    with st.spinner('Processing with Snowpark...'):
                        startTime = int(time.time())
                        st.session_state.snowflakeDF = pd.DataFrame(session.sql(f'select * from {selectTable}').collect())
                        st.dataframe(st.session_state.snowflakeDF)
                        endTime = int(time.time())
                        elapsed_time = endTime - startTime
                        st.text(f'Completed in {elapsed_time} sec')
        
            data = st.session_state.snowflakeDF

    if file is not None:

        st.session_state.fileDF = pd.read_csv(file)
        data = st.session_state.fileDF

    if file is not None or (st.session_state.snowflakeDF is not None and not st.session_state.snowflakeDF.empty):
        try:
            # Define pandas df agent - 0 ~ no creativity vs 1 ~ very creative
            agent = create_pandas_dataframe_agent(OpenAI(temperature=0.1, openai_api_key=openai.api_key),data,verbose=True) 

            query = st.text_input("Enter a query & press Execute:")

            # Execute Button Logic
            if st.button("Execute") and query:
                #openai.api_key =
                with st.spinner('Generating response...'):
                    try:
                        with get_openai_callback() as cb:
                            # Make the API call
                            response = openai.Completion.create(
                                engine='davinci',
                                prompt=query,
                                max_tokens=100
                            )

                            answer = agent.run(query)
                            cb.total_cost
                            st.session_state.cost.append(cb.total_cost)

                        # Store conversation
                        st.session_state.past.append(query)
                        st.session_state.generated.append(answer)

                        st.subheader('Response')
                    
                        # Display conversation in reverse order
                        for i in range(len(st.session_state.past)-1, -1, -1):
                            st.write(f'<div class="gpt-bubble">{st.session_state.generated[i]}</div>', unsafe_allow_html=True)
                            st.write("")
                            st.write(f'<div class="user-bubble">{st.session_state.past[i]}</div>', unsafe_allow_html=True)
                            st.write("")

                    except Exception as e:
                        st.error(f"An error occurred: {str(e)}")

        except Exception as e:
            st.warning('Upload data locally or from Snowflake')

if __name__ == "__main__":
    main()   