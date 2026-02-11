import streamlit as st
import sys
from pathlib import Path
import pandas as pd

# Add utils to path
sys.path.append(str(Path(__file__).parent.parent.parent / 'utils'))
from connect_db import get_engine
from cache_func import setup_auto_refresh, setup_sidebar_cache_controls

st.set_page_config(
    page_title="Data Overview", 
    layout="wide",
    page_icon="📊"
)

def get_db_connection():
    return get_engine()

def get_schema_info():
    """Get information about database schemas and tables"""
    engine = get_db_connection()
    query = """
    SELECT 
        schemaname as schema_name,
        tablename as table_name,
        (SELECT COUNT(*) FROM information_schema.columns 
         WHERE table_schema = schemaname AND table_name = tablename) as column_count
    FROM pg_tables 
    WHERE schemaname IN ('master', 'bronze_fin', 'bronze_ops', 'silver_ops', 'gold_ops')
    ORDER BY 
        CASE schemaname
            WHEN 'master' THEN 1
            WHEN 'bronze_fin' THEN 2 
            WHEN 'bronze_ops' THEN 3
            WHEN 'silver_ops' THEN 4
            WHEN 'gold_ops' THEN 5
        END,
        tablename;
    """
    return pd.read_sql(query, engine)

def get_table_row_count(schema, table):
    """Get row count for a specific table"""
    engine = get_db_connection()
    try:
        query = f"SELECT COUNT(*) as count FROM {schema}.{table}"
        result = pd.read_sql(query, engine)
        return result['count'].iloc[0]
    except:
        return 0

def get_table_data(schema, table, limit=100):
    """Get sample data from a table"""
    engine = get_db_connection()
    try:
        query = f"SELECT * FROM {schema}.{table} LIMIT {limit}"
        return pd.read_sql(query, engine)
    except Exception as e:
        st.warning(f"Could not load data: {str(e)}")
        return pd.DataFrame()

def main():
    """Data overview and exploration page"""
    
    st.title("Data Explorer")
    st.markdown("Browse your database tables and analyze data")
    
    setup_auto_refresh()
    setup_sidebar_cache_controls()
    
    try:
        schema_info = get_schema_info()
        
        if schema_info.empty:
            st.warning("No tables found in database.")
            return
        
        tab1, tab2 = st.tabs(["Explore Data", "Schema Overview"])
        
        # Data Explorer Tab
        with tab1:
            # Table selector
            col1, col2 = st.columns([1, 1])
            
            with col1:
                selected_schema = st.selectbox("Schema", schema_info['schema_name'].unique())
            
            with col2:
                available_tables = schema_info[schema_info['schema_name'] == selected_schema]['table_name'].tolist()
                selected_table = st.selectbox("Table", available_tables)
            
            st.markdown("---")
            
            # Get table stats
            total_rows = get_table_row_count(selected_schema, selected_table)
            
            if total_rows == 0:
                st.info(f"Table `{selected_schema}.{selected_table}` is empty")
                return
            
            # Controls
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Total Rows", f"{total_rows:,}")
            
            with col2:
                sample_size = st.number_input("Rows to show", min_value=10, max_value=1000, value=100, step=50)
            
            with col3:
                load_button = st.button("Load Data", type="primary", use_container_width=True)
            
            # Load data
            if load_button or st.session_state.get('data_loaded', False):
                st.session_state['data_loaded'] = True
                
                with st.spinner("Loading..."):
                    sample_data = get_table_data(selected_schema, selected_table, sample_size)
                    
                    if not sample_data.empty:
                        st.markdown(f"### {selected_schema}.{selected_table}")
                        
                        # Show data
                        st.dataframe(
                            sample_data, 
                            use_container_width=True, 
                            hide_index=True,
                            height=400
                        )
                        
                        # Column stats
                        with st.expander("Column Information"):
                            col_info = pd.DataFrame({
                                'Column': sample_data.columns,
                                'Type': sample_data.dtypes.astype(str),
                                'Non-Null': sample_data.count(),
                                'Null': sample_data.isnull().sum(),
                                'Null %': (sample_data.isnull().sum() / len(sample_data) * 100).round(1)
                            })
                            st.dataframe(col_info, use_container_width=True, hide_index=True)
                        
                        # Numeric stats
                        numeric_cols = sample_data.select_dtypes(include=['number']).columns
                        if len(numeric_cols) > 0:
                            with st.expander("Numeric Statistics"):
                                st.dataframe(sample_data[numeric_cols].describe(), use_container_width=True)
                    else:
                        st.error("Failed to load data")
        
        # Schema Overview Tab
        with tab2:
            schemas = schema_info['schema_name'].unique()
            
            for schema in schemas:
                with st.expander(f"**{schema}**", expanded=(schema=="master")):
                    schema_tables = schema_info[schema_info['schema_name'] == schema]
                    
                    for _, row in schema_tables.iterrows():
                        row_count = get_table_row_count(schema, row['table_name'])
                        st.markdown(f"• `{row['table_name']}` — {row_count:,} rows")
    
    except Exception as e:
        st.error(f"Error: {e}")

if __name__ == "__main__":
    main()
