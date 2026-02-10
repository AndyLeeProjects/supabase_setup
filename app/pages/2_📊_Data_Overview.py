import streamlit as st
import sys
from pathlib import Path
import pandas as pd

# Add utils to path
sys.path.append(str(Path(__file__).parent.parent.parent / 'utils'))
from connect_db import get_engine

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
    WHERE schemaname IN ('master', 'bronze_fin', 'bronze_ops', 'silver', 'silver_ops', 'gold', 'gold_ops')
    ORDER BY 
        CASE schemaname
            WHEN 'master' THEN 1
            WHEN 'bronze_fin' THEN 2 
            WHEN 'bronze_ops' THEN 3
            WHEN 'silver' THEN 4
            WHEN 'silver_ops' THEN 5
            WHEN 'gold' THEN 6
            WHEN 'gold_ops' THEN 7
        END,
        tablename;
    """
    return pd.read_sql(query, engine)

def get_table_data(schema, table, limit=100):
    """Get sample data from a table"""
    engine = get_db_connection()
    try:
        query = f"SELECT * FROM {schema}.{table} LIMIT {limit}"
        return pd.read_sql(query, engine)
    except Exception as e:
        st.warning(f"Could not load data from {schema}.{table}: {str(e)}")
        return pd.DataFrame()

def get_column_description(column_name, schema_name, table_name):
    """Generate intelligent descriptions for database columns"""
    
    # Common column patterns and their descriptions
    common_descriptions = {
        'id': 'Unique identifier (primary key)',
        'uuid': 'Universally unique identifier',
        'created_at': 'Timestamp when record was created',
        'updated_at': 'Timestamp when record was last modified',
        'deleted_at': 'Timestamp when record was soft deleted (null if active)',
        'is_active': 'Boolean flag indicating if record is currently active',
        'is_deleted': 'Boolean flag indicating if record is deleted',
        'name': 'Display name or title of the entity',
        'slug': 'URL-friendly identifier (lowercase, no spaces)',
        'status': 'Current status or state of the record',
        'email': 'Email address',
        'phone': 'Phone number',
        'address': 'Street address',
        'city': 'City name',
        'state': 'State or province',
        'zip': 'Postal/ZIP code',
        'postal_code': 'Postal/ZIP code',
        'country': 'Country name or code',
        'start_date': 'Date when something begins/started',
        'end_date': 'Date when something ends/ended',
        'effective_start': 'Date when this version becomes effective',
        'effective_end': 'Date when this version expires',
        'version': 'Version number or identifier',
        'notes': 'Additional notes or comments',
        'description': 'Detailed description of the entity',
        'type': 'Category or classification',
        'amount': 'Monetary amount or quantity',
        'price': 'Cost or price value',
        'quantity': 'Number of items or count',
        'total': 'Sum or total amount',
        'patient_id': 'Unique identifier linking to patient record',
        'provider_id': 'Unique identifier linking to provider record',
        'practice_id': 'Unique identifier linking to practice record',
        'client_id': 'Unique identifier linking to client record',
        'location_id': 'Unique identifier linking to location record',
        'template_id': 'Unique identifier linking to template record',
        'field_name': 'Name of a data field or column',
        'data_type': 'Type of data stored (text, number, date, etc.)',
        'unit': 'Unit of measurement',
        'is_required': 'Whether this field is mandatory',
        'source_ref': 'Reference to original data source',
        'ordinal': 'Order or position in a sequence',
        'owner_name': 'Name of the person who owns this entity',
        'owner_field_parent': 'Parent category of owner field classification',
        'owner_field_specific': 'Specific subcategory of owner field',
        'practice_type_parent': 'High-level practice category',
        'practice_type_specific': 'Detailed practice specialty or type',
        'provider_type': 'Type or specialty of healthcare provider',
        'billing_entity': 'Legal entity responsible for billing',
        'primary_contact_name': 'Name of main contact person',
        'primary_contact_email': 'Email of main contact person',
        'primary_contact_phone': 'Phone of main contact person'
    }
    
    # Schema-specific context
    schema_contexts = {
        'master': 'Core reference data',
        'bronze_fin': 'Raw financial data from source systems',
        'bronze_ops': 'Raw operational data from source systems', 
        'silver': 'Cleaned and standardized data',
        'gold': 'Business metrics and analytics-ready data'
    }
    
    # Table-specific context for better descriptions
    table_contexts = {
        'clients': 'top-level customer organizations',
        'practices': 'individual practice locations or business units',
        'providers': 'healthcare professionals',
        'locations': 'physical practice locations',
        'input_templates': 'data import template definitions',
        'input_template_versions': 'versioned template specifications',
        'input_template_fields': 'individual field definitions within templates',
        'practice_aliases': 'alternative names or identifiers for practices',
        'provider_aliases': 'alternative names or identifiers for providers',
        'time_periods': 'standardized date/time ranges for reporting'
    }
    
    col_lower = column_name.lower()
    
    # Check for exact match first
    if col_lower in common_descriptions:
        base_description = common_descriptions[col_lower]
    else:
        # Check for partial matches
        if 'id' in col_lower and col_lower != 'id':
            if col_lower.endswith('_id'):
                entity = col_lower.replace('_id', '')
                base_description = f'Foreign key reference to {entity} table'
            else:
                base_description = 'Identifier field'
        elif 'name' in col_lower:
            base_description = 'Name or title field'
        elif 'date' in col_lower:
            base_description = 'Date value'
        elif 'time' in col_lower:
            base_description = 'Time or timestamp value'
        elif 'email' in col_lower:
            base_description = 'Email address field'
        elif 'phone' in col_lower:
            base_description = 'Phone number field'
        elif 'address' in col_lower:
            base_description = 'Address information'
        elif 'amount' in col_lower or 'price' in col_lower or 'cost' in col_lower:
            base_description = 'Monetary value'
        elif 'count' in col_lower or 'quantity' in col_lower or 'num' in col_lower:
            base_description = 'Numeric count or quantity'
        elif 'is_' in col_lower:
            base_description = 'Boolean flag'
        elif 'type' in col_lower:
            base_description = 'Category or classification'
        elif 'status' in col_lower:
            base_description = 'Status indicator'
        else:
            base_description = 'Data field'
    
    # Add context based on schema and table
    schema_context = schema_contexts.get(schema_name, '')
    table_context = table_contexts.get(table_name, 'this entity')
    
    if schema_context and table_context != 'this entity':
        full_description = f"{base_description} for {table_context} in {schema_context}"
    elif table_context != 'this entity':
        full_description = f"{base_description} for {table_context}"
    elif schema_context:
        full_description = f"{base_description} in {schema_context}"
    else:
        full_description = base_description
    
def get_table_row_count(schema, table):
    """Get row count for a table"""
    engine = get_db_connection()
    try:
        query = f"SELECT COUNT(*) as row_count FROM {schema}.{table}"
        result = pd.read_sql(query, engine)
        return result.iloc[0]['row_count']
    except:
        return 0

def main():
    """Data overview and exploration page"""
    
    st.title("📊 Data Overview & Exploration")
    st.markdown("Explore your database schemas and understand what data lives in each layer.")
    
    tab1, tab2 = st.tabs(["📈 Schema Overview", "🔍 Data Explorer"])
    
    with tab1:
        st.subheader("Database Schema Overview")
        
        try:
            schema_info = get_schema_info()
            
            if not schema_info.empty:
                # Schema summary metrics
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    total_tables = len(schema_info)
                    st.metric("Total Tables", total_tables)
                
                with col2:
                    schema_count = schema_info['schema_name'].nunique()
                    st.metric("Schemas", schema_count)
                    
                with col3:
                    total_columns = schema_info['column_count'].sum()
                    st.metric("Total Columns", total_columns)
                    
                with col4:
                    avg_columns = schema_info['column_count'].mean()
                    st.metric("Avg Columns/Table", f"{avg_columns:.1f}")
                
                st.markdown("---")
                
                # Schema breakdown by layer
                for schema_name in ['master', 'bronze_fin', 'bronze_ops', 'silver', 'gold']:
                    schema_tables = schema_info[schema_info['schema_name'] == schema_name]
                    
                    if not schema_tables.empty:
                        # Schema header with icon
                        schema_icons = {
                            'master': '🏛️',
                            'bronze_fin': '🥉',
                            'bronze_ops': '🥉', 
                            'silver': '🥈',
                            'gold': '🥇'
                        }
                        
                        schema_descriptions = {
                            'master': 'Core business entities and reference data',
                            'bronze_fin': 'Raw financial data from source systems',
                            'bronze_ops': 'Raw operational data from source systems',
                            'silver': 'Cleaned and standardized data',
                            'gold': 'Business-ready analytics and aggregations'
                        }
                        
                        with st.expander(f"{schema_icons[schema_name]} **{schema_name.upper()} Schema** - {schema_descriptions[schema_name]}", expanded=True):
                            # Show tables in a nice format
                            table_col1, table_col2 = st.columns([2, 1])
                            
                            with table_col1:
                                # Create a clean table display with sample descriptions
                                display_tables = schema_tables[['table_name', 'column_count']].copy()
                                display_tables.columns = ['Table Name', 'Columns']
                                
                                # Add table descriptions
                                table_descriptions = {
                                    # Master layer
                                    'clients': 'Top-level customer organizations',
                                    'practices': 'Individual practice locations within clients',
                                    'providers': 'Healthcare professionals at practices',
                                    'locations': 'Physical practice locations',
                                    'input_templates': 'Data import template definitions',
                                    'input_template_versions': 'Template version specifications',
                                    'input_template_fields': 'Field definitions within templates',
                                    'practice_aliases': 'Alternative identifiers for practices',
                                    'provider_aliases': 'Alternative identifiers for providers',
                                    'time_periods': 'Standardized reporting periods',
                                    'client_appointment_type_mappings': 'Client-specific appointment type classifications',
                                    'client_referral_category_mappings': 'Client-specific referral source standardization',
                                    
                                    # Silver layer (canonical facts)
                                    'fact_new_patient_intake': 'Canonical new patient intake events with standardized referral data',
                                    
                                    # Gold layer (metricized outputs)  
                                    'referrals_monthly_summary': 'Monthly aggregated referral metrics (counts, L3M, variance, YTD)',
                                    'referrals_monthly_breakdown': 'Monthly referral breakdowns by category and source name'
                                }
                                
                                display_tables['Purpose'] = display_tables['Table Name'].map(
                                    lambda x: table_descriptions.get(x, 'Data storage table')
                                )
                                
                                st.dataframe(
                                    display_tables, 
                                    use_container_width=True, 
                                    hide_index=True,
                                    column_config={
                                        "Purpose": st.column_config.TextColumn(
                                            "Purpose",
                                            help="What this table is designed to store",
                                            width="large"
                                        )
                                    }
                                )
                            
                            with table_col2:
                                table_count = len(schema_tables)
                                total_cols = schema_tables['column_count'].sum()
                                st.metric("Tables", table_count)
                                st.metric("Total Columns", total_cols)
                    else:
                        st.info(f"No tables found in {schema_name} schema")
                
            else:
                st.warning("No schema information available")
                
        except Exception as e:
            st.error(f"Error loading schema information: {e}")

    with tab2:
        st.subheader("Interactive Data Explorer")
        st.markdown("Select a schema and table to explore the actual data stored in your database.")
        
        try:
            schema_info = get_schema_info()
            
            if not schema_info.empty:
                # Schema and table selection
                col1, col2, col3 = st.columns([2, 2, 1])
                
                with col1:
                    selected_schema = st.selectbox("Select Schema", schema_info['schema_name'].unique())
                
                with col2:
                    available_tables = schema_info[schema_info['schema_name'] == selected_schema]['table_name'].tolist()
                    selected_table = st.selectbox("Select Table", available_tables)
                
                with col3:
                    sample_size = st.number_input("Sample Size", min_value=10, max_value=1000, value=100, step=10)
                
                # Load data button
                if st.button("🔍 Explore Table Data", use_container_width=True):
                    with st.spinner("Loading data..."):
                        # Get row count first
                        total_rows = get_table_row_count(selected_schema, selected_table)
                        
                        if total_rows > 0:
                            # Load sample data
                            sample_data = get_table_data(selected_schema, selected_table, sample_size)
                            
                            if not sample_data.empty:
                                st.success(f"📊 Loaded {len(sample_data)} rows from `{selected_schema}.{selected_table}` (Total: {total_rows:,} rows)")
                                
                                # Data overview metrics
                                overview_col1, overview_col2, overview_col3, overview_col4 = st.columns(4)
                                
                                with overview_col1:
                                    st.metric("Sample Rows", len(sample_data))
                                with overview_col2:
                                    st.metric("Total Rows", f"{total_rows:,}")
                                with overview_col3:
                                    st.metric("Columns", len(sample_data.columns))
                                with overview_col4:
                                    missing_pct = (sample_data.isnull().sum().sum() / (len(sample_data) * len(sample_data.columns)) * 100)
                                    st.metric("Missing Data %", f"{missing_pct:.1f}%")
                                
                                st.markdown("### 📋 Data Preview")
                                st.dataframe(sample_data, use_container_width=True, hide_index=True)
                                
                                # Data analysis tabs
                                analysis_tab1, analysis_tab2, analysis_tab3 = st.tabs(["📊 Column Analysis", "🔢 Statistics", "📁 Data Types"])
                                
                                with analysis_tab1:
                                    st.subheader("Column Information & Descriptions")
                                    
                                    # Create column info with descriptions
                                    col_descriptions = [get_column_description(col, selected_schema, selected_table) 
                                                      for col in sample_data.columns]
                                    
                                    col_info = pd.DataFrame({
                                        'Column': sample_data.columns,
                                        'Description': col_descriptions,
                                        'Data Type': sample_data.dtypes.astype(str),
                                        'Non-Null': sample_data.count(),
                                        'Null': sample_data.isnull().sum(),
                                        'Null %': (sample_data.isnull().sum() / len(sample_data) * 100).round(1)
                                    })
                                    
                                    # Style the dataframe for better readability
                                    st.dataframe(
                                        col_info, 
                                        use_container_width=True, 
                                        hide_index=True,
                                        column_config={
                                            "Description": st.column_config.TextColumn(
                                                "Description",
                                                help="What this column is designed to store",
                                                width="large"
                                            ),
                                            "Column": st.column_config.TextColumn(
                                                "Column",
                                                width="medium"
                                            ),
                                            "Data Type": st.column_config.TextColumn(
                                                "Data Type", 
                                                width="small"
                                            )
                                        }
                                    )
                                
                                with analysis_tab2:
                                    st.subheader("Numeric Column Statistics")
                                    numeric_columns = sample_data.select_dtypes(include=['number']).columns
                                    if len(numeric_columns) > 0:
                                        stats_df = sample_data[numeric_columns].describe()
                                        st.dataframe(stats_df, use_container_width=True)
                                    else:
                                        st.info("No numeric columns found in this table")
                                
                                with analysis_tab3:
                                    st.subheader("Data Type Summary")
                                    type_summary = sample_data.dtypes.value_counts().reset_index()
                                    type_summary.columns = ['Data Type', 'Count']
                                    st.dataframe(type_summary, use_container_width=True, hide_index=True)
                            else:
                                st.warning(f"Could not load data from {selected_schema}.{selected_table}")
                        else:
                            st.info(f"Table `{selected_schema}.{selected_table}` is empty or doesn't exist")
                            
        except Exception as e:
            st.error(f"Error in data explorer: {e}")
    
    # Educational footer
    st.markdown("---")
    st.info("""
    💡 **Understanding Your Data:**
    - **Master**: Core business entities (clients, practices, providers)
    - **Bronze**: Raw data exactly as received from source systems  
    - **Silver**: Cleaned and validated data ready for analysis
    - **Gold**: Business metrics and KPIs optimized for reporting
    """)

if __name__ == "__main__":
    main()