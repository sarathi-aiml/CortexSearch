import json
import streamlit as st
import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session

table_name = "BOOKS_DATASET_RAW"
service_name = "BOOKS_DATASET_SERVICE"

# Function to get attributes from the SHOW CORTEX SEARCH SERVICES output
def get_cortex_search_attributes(session, service_name):
    services_query = f"SHOW CORTEX SEARCH SERVICES LIKE '{service_name}';"
    services_result = session.sql(services_query).collect()
    services_df = pd.DataFrame(services_result)

    if not services_df.empty and 'name' in services_df.columns and 'attribute_columns' in services_df.columns:
        service_row = services_df[services_df['name'] == service_name]

        if not service_row.empty:
            attribute_str = service_row.iloc[0]['attribute_columns']
            attributes = [attr.strip() for attr in attribute_str.split(',')]
            return attributes
        else:
            return []
    else:
        return []

# Function to get available columns from the Snowflake table
def get_available_columns(session, table_name):
    column_query = f"""
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '{table_name}'
    ORDER BY ORDINAL_POSITION;
    """
    columns_df = session.sql(column_query).to_pandas()
    return columns_df['COLUMN_NAME'].tolist()

# Define the function to run the Cortex search
def run_cortex_search(session, database_name, schema_name, service_name, query, columns, filter_condition, limit):
    search_query = f"""
    SELECT PARSE_JSON(
        SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
            '{database_name}.{schema_name}.{service_name}',
            '{{ 
               "query": "{query}",
               "columns": {columns},
               "filter": {filter_condition},
               "limit": {limit}
            }}'
        )
    )['results'] as results;
    """
    
    result_df = session.sql(search_query).to_pandas()

    if not result_df.empty:
        results_json = json.loads(result_df.iloc[0]['RESULTS'])
        structured_results = pd.DataFrame(results_json)
    else:
        structured_results = pd.DataFrame(columns=json.loads(columns))
    
    return structured_results

# Start the Snowflake session
session = get_active_session()

# Get the current database and schema from the session
database_name = session.get_current_database()
schema_name = session.get_current_schema()

# Get the available columns dynamically from the BOOKS_DATASET_RAW table

available_columns = get_available_columns(session, table_name)
attributes = get_cortex_search_attributes(session, service_name)

# Streamlit UI elements for input
st.title("📚 Cortex Search Service with Snowflake")

# Sidebar for inputs
st.sidebar.header("🔍 Search Settings")
query = st.sidebar.text_input("Enter your search query:")
selected_columns = st.sidebar.multiselect("Select columns to retrieve:", available_columns, default=[])
columns = json.dumps(selected_columns)
filter_column = st.sidebar.selectbox("Select a filter column:", attributes)
filter_value = st.sidebar.text_input(f"Enter filter value for {filter_column}:")
filter_condition = json.dumps({"@eq": {filter_column: filter_value}})
limit = st.sidebar.number_input("Enter the limit for search results:", min_value=1, value=10)

# Apply custom styling for better appearance
st.markdown(
    """
    <style>
    .stButton>button {
        background-color: #4CAF50;
        color: white;
        padding: 10px 24px;
        border: none;
        border-radius: 4px;
        cursor: pointer;
    }
    .stButton>button:hover {
        background-color: #45a049;
    }
    </style>
    """, unsafe_allow_html=True
)

# Button to trigger the search
if st.sidebar.button("🚀 Run Search"):
    with st.spinner("Running search..."):
        results = run_cortex_search(session, database_name, schema_name, service_name, query, columns, filter_condition, limit)
        if not results.empty:
            st.success(f"✅ Search completed successfully! Found **{len(results)}** results.")
            st.write("### Results:")
            st.dataframe(results.style.set_properties(**{'background-color': '#f4f4f4', 'color': '#333333', 'border-color': 'black'}))
        else:
            st.warning("⚠️ No results found.")

# Collapsible section for showing the raw SQL query
with st.expander("🛠️ Show SQL Query"):
    st.code(f"""
    SELECT PARSE_JSON(
        SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
            '{database_name}.{schema_name}.{service_name}',
            '{{ 
               "query": "{query}",
               "columns": {columns},
               "filter": {filter_condition},
               "limit": {limit}
            }}'
        )
    )['results'] as results;
    """, language="sql")
