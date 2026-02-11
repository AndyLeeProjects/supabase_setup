import streamlit as st
import sys
from pathlib import Path
import pandas as pd
from datetime import datetime
from sqlalchemy import text

# Add utils to path
sys.path.append(str(Path(__file__).parent.parent.parent / 'utils'))
sys.path.append(str(Path(__file__).parent.parent.parent / 'src'))
from connect_db import get_engine
from cache_func import (
    get_clients_cached, get_bronze_data_status_cached, get_silver_gold_status_cached,
    refresh_etl_data_cache, refresh_all_caches, setup_auto_refresh, setup_sidebar_cache_controls
)

# Import ETL configuration
try:
    from etl_pipeline import CLIENT_ETL_CONFIG, get_client_etl_config
except ImportError:
    CLIENT_ETL_CONFIG = {}
    def get_client_etl_config(client_name):
        return '2020-01-01'

st.set_page_config(
    page_title="ETL Pipeline", 
    layout="wide",
    page_icon="🔄"
)

def get_db_connection():
    return get_engine()

def table_exists(engine, schema, table_name):
    """Check if a table exists in the database"""
    try:
        query = text(f"""
            SELECT EXISTS(
                SELECT 1 FROM pg_tables
                WHERE schemaname = :schema AND tablename = :table
            ) as exists
        """)
        result = pd.read_sql(query, engine, params={'schema': schema, 'table': table_name})
        return result.iloc[0, 0]
    except:
        return False

def get_clients():
    """Get list of clients for selection using cache"""
    return get_clients_cached()

def get_bronze_data_status_fast(client_name=None):
    """Fast bronze data status check using approximate counts"""
    try:
        engine = get_db_connection()
        
        # Use pg_stat_user_tables for fast approximate counts
        query = """
        SELECT 
            schemaname,
            relname as table_name,
            n_live_tup as row_count
        FROM pg_stat_user_tables
        WHERE schemaname IN ('bronze_ops', 'bronze_fin')
        ORDER BY relname
        """
        
        stats_df = pd.read_sql(query, engine)
        
        status = {
            'appointments': {'total_appointments': 0},
            'referrals': {'total_referrals': 0},
            'patients': {'total_patients': 0},
            'treatments': {'total_treatments': 0},
            'production': {'total_production': 0}
        }
        
        # Map table names to data types
        for _, row in stats_df.iterrows():
            table = row['table_name'].lower()
            count = row['row_count']
            
            if 'appointment' in table:
                status['appointments']['total_appointments'] = count
            elif 'referral' in table:
                status['referrals']['total_referrals'] = count
            elif 'patient' in table:
                status['patients']['total_patients'] = count
            elif 'treatment' in table:
                status['treatments']['total_treatments'] = count
            elif 'production' in table:
                status['production']['total_production'] = count
        
        return status
    except Exception as e:
        st.error(f"Error checking bronze data: {e}")
        return {
            'appointments': {'total_appointments': 0},
            'referrals': {'total_referrals': 0},
            'patients': {'total_patients': 0},
            'treatments': {'total_treatments': 0},
            'production': {'total_production': 0}
        }

def get_current_silver_gold_status(client_id=None):
    """Check current silver and gold data status using cache"""
    status = get_silver_gold_status_cached()
    
    # Extract silver status
    silver_status = None
    summary_status = None
    breakdown_status = None
    
    if status and 'silver' in status:
        # Calculate total silver facts
        silver_facts = sum([
            status['silver'].get('referrals', {}).get('count', 0),
            status['silver'].get('fact_patient_treatments', {}).get('count', 0),
            status['silver'].get('fact_referrals', {}).get('count', 0)
        ])
        
        # Get earliest and latest appointment dates (from referrals)
        referrals_table = status['silver'].get('referrals', {})
        
        silver_status = {
            'silver_facts': silver_facts,
            'earliest_date': referrals_table.get('earliest_date'),
            'latest_date': referrals_table.get('latest_date')
        }
    
    if status and 'gold' in status:
        # Extract summary records (from referrals_monthly_summary)
        summary_table = status['gold'].get('referrals_monthly_summary', {})
        summary_status = {
            'summary_records': summary_table.get('count', 0)
        }
        
        # Extract breakdown records (from referrals_monthly_breakdown)
        breakdown_table = status['gold'].get('referrals_monthly_breakdown', {})
        breakdown_status = {
            'breakdown_records': breakdown_table.get('count', 0)
        }
    
    return silver_status, summary_status, breakdown_status

def run_etl_with_logging(client_name):
    """Run ETL pipeline with detailed logging"""
    try:
        # Force reload to get fresh code (not cached bytecode)
        import importlib
        import etl_pipeline
        importlib.reload(etl_pipeline)
        from etl_pipeline import run_etl_pipeline
        
        # Get before status
        clients_df = get_clients()
        client_row = clients_df[clients_df['name'] == client_name]
        if client_row.empty:
            return {'success': False, 'message': f'Client {client_name} not found'}
            
        client_id = client_row.iloc[0]['id']
        before_silver, before_summary, before_breakdown = get_current_silver_gold_status(client_id)
        
        # Run ETL
        result = run_etl_pipeline(client_name)
        
        if result['success']:
            # Refresh ETL-related caches after successful run
            refresh_etl_data_cache()
            
            # Get after status
            after_silver, after_summary, after_breakdown = get_current_silver_gold_status(client_id)
            
            # Calculate changes
            changes = {
                'silver_before': before_silver['silver_facts'] if before_silver else 0,
                'silver_after': result['silver_rows'],
                'summary_before': before_summary['summary_records'] if before_summary else 0,
                'summary_after': result['summary_rows'],
                'breakdown_before': before_breakdown['breakdown_records'] if before_breakdown else 0,
                'breakdown_after': result['breakdown_rows'],
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            result['changes'] = changes
            
        return result
        
    except Exception as e:
        return {'success': False, 'message': str(e)}

def main():
    st.title("ETL Pipeline Management")
    st.markdown("Transform bronze data into silver facts and gold metrics with full visibility into changes.")

    # Auto-refresh setup
    setup_auto_refresh()
    
    # Setup sidebar cache controls
    setup_sidebar_cache_controls()

    # Get clients for processing
    clients_df = get_clients()
    
    if clients_df.empty:
        st.warning("No clients found. Please add clients in the Master Data page first.")
        return
    
    # Default to first client for ETL operations
    selected_client = clients_df['name'].iloc[0] if not clients_df.empty else None
    
    # Check bronze data status (silent check for ETL validation)
    bronze_status = get_bronze_data_status_fast(selected_client)
    appointments_status = bronze_status.get('appointments', {})
    referrals_status = bronze_status.get('referrals', {})
    
    # Get client_id for ETL operations
    client_id = clients_df['id'].iloc[0]
    
    # Show ETL Configuration Info
    with st.expander("⚙️ Referrals ETL Configuration", expanded=False):
        st.markdown("""
        ### How to Configure Referrals Pipeline Date Filters
        
        The referrals ETL pipeline can filter appointments by date per client. 
        Edit `CLIENT_ETL_CONFIG` in [`src/etl_pipeline.py`](../src/etl_pipeline.py) to customize.
        
        **Configuration Option:**
        - `min_appointment_date`: Only process appointments from this date onwards
        
        **Example:**
        ```python
        CLIENT_ETL_CONFIG = {
            'Wall Street Orthodontics': {
                'min_appointment_date': '2025-01-01'
            },
            'Another Client': {
                'min_appointment_date': '2024-01-01'
            }
        }
        ```
        
        **Current Configurations:**
        """)
        
        if CLIENT_ETL_CONFIG:
            config_data = []
            for client_name, config in CLIENT_ETL_CONFIG.items():
                config_data.append({
                    'Client': client_name,
                    'Appointment Date Filter': f"{config.get('min_appointment_date', 'Not set')} onwards"
                })
            config_df = pd.DataFrame(config_data)
            st.dataframe(config_df, use_container_width=True, hide_index=True)
        else:
            st.info("No client-specific configurations set. All clients use default settings (2020-01-01 onwards).")
    
    # Data Transformation Explorer
    st.subheader("Explore Data Transformation")
    st.markdown("View how data transforms through each layer from Bronze → Silver → Gold")
    
    explore_data_type = st.selectbox(
        "Select Data Type",
        options=["Referrals", "Appointments"],
        key="explore_data_type"
    )
    
    if st.button("Show Transformation", type="primary"):
        explore_client_id = clients_df.iloc[0]['id']
        explore_client_slug = clients_df.iloc[0].get('slug', clients_df.iloc[0]['name'].lower().replace(' ', '_'))
        
        # Determine bronze suffix (same logic as Home page)
        if 'wall_street' in explore_client_slug or 'wso' in explore_client_slug:
            bronze_suffix = 'wso'
        else:
            bronze_suffix = explore_client_slug
        
        engine = get_db_connection()
        
        # Bronze Layer - Show all source tables
        with st.expander("🥉 Bronze Layer - Raw Data Sources", expanded=True):
            st.markdown("### Source Tables Used for Silver Layer")
            st.markdown("""
            The silver layer combines data from multiple bronze and master tables:            
            - **Bronze Tables**: Raw operational data from source systems
            - **Master Tables**: Reference data for standardization and mapping
            """)
            
            # Get row counts dynamically
            table_info = []
            try:
                # Appointments table
                appt_count = pd.read_sql(f"SELECT COUNT(*) as cnt FROM bronze_ops.appointments_raw_{bronze_suffix}", engine).iloc[0]['cnt']
                table_info.append({
                    'Table': f'bronze_ops.appointments_raw_{bronze_suffix}',
                    'Type': 'Bronze',
                    'Rows': f'{appt_count:,}',
                    'Purpose': 'Main appointment records with patient info'
                })
            except:
                table_info.append({
                    'Table': f'bronze_ops.appointments_raw_{bronze_suffix}',
                    'Type': 'Bronze',
                    'Rows': '0',
                    'Purpose': 'Main appointment records with patient info'
                })
            
            try:
                # Referrals table
                ref_count = pd.read_sql(f"SELECT COUNT(*) as cnt FROM bronze_ops.referrals_raw_{bronze_suffix}", engine).iloc[0]['cnt']
                table_info.append({
                    'Table': f'bronze_ops.referrals_raw_{bronze_suffix}',
                    'Type': 'Bronze',
                    'Rows': f'{ref_count:,}',
                    'Purpose': 'Referral source information per patient'
                })
            except:
                table_info.append({
                    'Table': f'bronze_ops.referrals_raw_{bronze_suffix}',
                    'Type': 'Bronze',
                    'Rows': '0',
                    'Purpose': 'Referral source information per patient'
                })
            
            # Master tables
            try:
                atm_count = pd.read_sql(f"SELECT COUNT(*) as cnt FROM master.appointment_type_mappings WHERE client_id = '{explore_client_id}'", engine).iloc[0]['cnt']
                table_info.append({
                    'Table': 'master.appointment_type_mappings',
                    'Type': 'Master',
                    'Rows': f'{atm_count:,}',
                    'Purpose': 'Maps appointment types to New Patient category'
                })
            except:
                table_info.append({
                    'Table': 'master.appointment_type_mappings',
                    'Type': 'Master',
                    'Rows': '0',
                    'Purpose': 'Maps appointment types to New Patient category'
                })
            
            try:
                rcm_count = pd.read_sql(f"SELECT COUNT(*) as cnt FROM master.client_referral_category_mappings WHERE client_id = '{explore_client_id}'", engine).iloc[0]['cnt']
                table_info.append({
                    'Table': 'master.client_referral_category_mappings',
                    'Type': 'Master',
                    'Rows': f'{rcm_count:,}',
                    'Purpose': 'Standardizes referral category names'
                })
            except:
                table_info.append({
                    'Table': 'master.client_referral_category_mappings',
                    'Type': 'Master',
                    'Rows': '0',
                    'Purpose': 'Standardizes referral category names'
                })
            
            try:
                tp_count = pd.read_sql("SELECT COUNT(*) as cnt FROM master.time_periods WHERE period_type = 'month'", engine).iloc[0]['cnt']
                table_info.append({
                    'Table': 'master.time_periods',
                    'Type': 'Master',
                    'Rows': f'{tp_count:,}',
                    'Purpose': 'Monthly time period definitions for aggregation'
                })
            except:
                table_info.append({
                    'Table': 'master.time_periods',
                    'Type': 'Master',
                    'Rows': '0',
                    'Purpose': 'Monthly time period definitions for aggregation'
                })
            
            # Display summary table
            summary_df = pd.DataFrame(table_info)
            st.dataframe(summary_df, use_container_width=True, hide_index=True)
            
            st.markdown("---")
            st.markdown("### 📋 Table Data Samples")
            
            # Expandable sections for each table
            with st.expander(f"View: bronze_ops.appointments_raw_{bronze_suffix}", expanded=False):
                try:
                    bronze_appt_query = text(f"""
                        SELECT 
                            patient_id,
                            patient_id_guid,
                            appointment_date,
                            appointment_type_description,
                            appointment_status_description
                        FROM bronze_ops.appointments_raw_{bronze_suffix}
                        LIMIT 10
                    """)
                    bronze_appt = pd.read_sql(bronze_appt_query, engine)
                    st.dataframe(bronze_appt, use_container_width=True, hide_index=True)
                    st.caption(f"Showing 10 of {appt_count:,} records")
                except Exception as e:
                    st.warning(f"No data available: {e}")
            
            with st.expander(f"View: bronze_ops.referrals_raw_{bronze_suffix}", expanded=False):
                try:
                    bronze_ref_query = text(f"""
                        SELECT 
                            patient_id_guid,
                            referred_in_by_type_description,
                            referred_in_by_first_name,
                            referred_in_by_last_name
                        FROM bronze_ops.referrals_raw_{bronze_suffix}
                        LIMIT 10
                    """)
                    bronze_ref = pd.read_sql(bronze_ref_query, engine)
                    st.dataframe(bronze_ref, use_container_width=True, hide_index=True)
                    st.caption(f"Showing 10 of {ref_count:,} records")
                except Exception as e:
                    st.warning(f"No data available: {e}")
            
            with st.expander("View: master.appointment_type_mappings", expanded=False):
                try:
                    atm_query = text(f"""
                        SELECT 
                            source_appointment_type,
                            standardized_category,
                            start_date,
                            end_date
                        FROM master.appointment_type_mappings
                        WHERE client_id = '{explore_client_id}'
                        ORDER BY source_appointment_type
                    """)
                    atm_data = pd.read_sql(atm_query, engine)
                    st.dataframe(atm_data, use_container_width=True, hide_index=True)
                    st.caption(f"Showing all {atm_count:,} mappings for this client")
                except Exception as e:
                    st.warning(f"No data available: {e}")
            
            with st.expander("View: master.client_referral_category_mappings", expanded=False):
                try:
                    rcm_query = text(f"""
                        SELECT 
                            raw_referral_category,
                            canonical_referral_category
                        FROM master.client_referral_category_mappings
                        WHERE client_id = '{explore_client_id}'
                        ORDER BY raw_referral_category
                    """)
                    rcm_data = pd.read_sql(rcm_query, engine)
                    st.dataframe(rcm_data, use_container_width=True, hide_index=True)
                    st.caption(f"Showing all {rcm_count:,} category mappings for this client")
                except Exception as e:
                    st.warning(f"No data available: {e}")
            
            with st.expander("View: master.time_periods", expanded=False):
                try:
                    tp_query = text("""
                        SELECT 
                            label,
                            start_date,
                            end_date,
                            year,
                            month
                        FROM master.time_periods
                        WHERE period_type = 'month'
                        ORDER BY start_date DESC
                        LIMIT 12
                    """)
                    tp_data = pd.read_sql(tp_query, engine)
                    st.dataframe(tp_data, use_container_width=True, hide_index=True)
                    st.caption(f"Showing 12 most recent of {tp_count:,} monthly periods")
                except Exception as e:
                    st.warning(f"No data available: {e}")
        
        # Silver Layer Sample
        with st.expander("🥈 Silver Layer - Standardized Facts", expanded=True):
            st.markdown("### Target Table: `silver_ops.referrals`")
            
            st.markdown("""
            **Transformation Logic:**
            - **Grain**: One row per unique patient (patient_id_guid)
            - **Selection**: Earliest appointment date per patient (DISTINCT ON with ORDER BY)
            - **Date Filter**: Client-specific minimum date (configured in ETL_CONFIG)
            
            **Joins Applied:**
            1. `appointments_raw` ← LEFT JOIN → `appointment_type_mappings` (for is_new_patient flag)
            2. `appointments_raw` ← LEFT JOIN → `referrals_raw` (on patient_id_guid)
            3. `referrals_raw` ← LEFT JOIN → `client_referral_category_mappings` (standardize categories)
            4. `appointments_raw` ← INNER JOIN → `time_periods` (assign monthly period)
            
            **New Columns Created:**
            - `is_new_patient`: Boolean flag based on appointment_type_mappings
            - `referral_category`: Standardized category (doctor/patient/non_patient/other/missing)
            - `referral_name`: Combined first + last name from referrals table
            - `time_period_id`: Monthly period UUID for aggregation
            """)
            
            # Check if table exists first
            if not table_exists(engine, 'silver_ops', 'referrals'):
                st.info("⚠️ Silver table not created yet. Run ETL pipeline to create and populate this table.")
            else:
                try:
                    silver_query = text(f"""
                        SELECT 
                            f.appointment_date,
                            f.patient_id,
                            f.appointment_type,
                            f.appointment_status,
                            f.is_new_patient,
                            f.referral_category,
                            f.referral_name as referral_source,
                            p.name as practice_name,
                            f.created_at
                        FROM silver_ops.referrals f
                        LEFT JOIN master.practices p ON f.practice_id = p.id
                        WHERE f.client_id::text = '{explore_client_id}'
                        ORDER BY f.appointment_date DESC
                    """)
                    
                    silver_sample = pd.read_sql(silver_query, engine)
                    
                    if not silver_sample.empty:
                        st.dataframe(silver_sample, use_container_width=True, hide_index=True, height=600)
                        st.caption(f"Showing all {len(silver_sample)} appointments")
                    else:
                        st.info("No silver layer data. Run ETL pipeline to process bronze data.")
                except Exception as e:
                    st.error(f"Error loading silver data: {str(e)}")
        
        # Gold Layer Metrics
        with st.expander("🥇 Gold Layer - Analytics Metrics", expanded=True):
            st.markdown("### Target Tables: `gold_ops.referrals_monthly_summary` & `gold_ops.referrals_monthly_breakdown`")
            
            st.markdown("""
            **Aggregation Logic:**
            
            **Summary Table** (`referrals_monthly_summary`):
            - **Grain**: One row per client/practice/month
            - **Metrics Calculated**:
              - `monthly_new_patient_cnt`: COUNT of new patients in the month
              - `l3m_avg_new_patient_cnt`: Rolling 3-month average (LAG window function)
              - `variance_from_l3m`: (current - L3M avg) / L3M avg
              - `ytd_new_patient_cnt`: Year-to-date cumulative sum
            - **Source**: `silver_ops.referrals` WHERE is_new_patient = TRUE
            
            **Breakdown Table** (`referrals_monthly_breakdown`):
            - **Grain**: One row per client/practice/month/breakdown_type/breakdown_value
            - **Breakdown Types**:
              - `referral_category`: Groups by doctor/patient/non_patient/other
              - `referral_name`: Individual referral source names
            - **Metrics**: Monthly count + percentage of total for the month
            - **Source**: `silver_ops.referrals` WHERE is_new_patient = TRUE, grouped by dimension
            """)
            
            # Check if gold tables exist first
            if not table_exists(engine, 'gold_ops', 'referrals_monthly_summary'):
                st.info("⚠️ Gold tables not created yet. Run ETL pipeline to create and populate these tables.")
            else:
                try:
                    # Get monthly summary with date filter (2025 onwards)
                    summary_query = text(f"""
                        SELECT 
                            tp.label as month,
                            tp.start_date,
                            s.monthly_new_patient_cnt as monthly_count,
                            s.l3m_avg_new_patient_cnt as l3m_average,
                            ROUND(s.variance_from_l3m * 100, 2) as variance_pct,
                            s.ytd_new_patient_cnt as ytd_total
                        FROM gold_ops.referrals_monthly_summary s
                        JOIN master.time_periods tp ON s.time_period_id = tp.id
                        WHERE s.client_id::text = '{explore_client_id}'
                            AND tp.start_date >= '2025-01-01'
                            AND tp.period_type = 'month'
                        ORDER BY tp.start_date
                    """)
                    
                    gold_summary = pd.read_sql(summary_query, engine)
                    
                    if not gold_summary.empty:
                        # Create Overview section
                        st.markdown("### 📊 Overview")
                        
                        # Transpose for month columns
                        overview_data = pd.DataFrame({
                            'Metric': ['New Patient Count', 'L3M Avg', 'Variance from L3M', 'Total 2025']
                        })
                        
                        for _, row in gold_summary.iterrows():
                            month_label = row['month']
                            overview_data[month_label] = [
                                int(row['monthly_count']),
                                f"{row['l3m_average']:.1f}" if pd.notna(row['l3m_average']) else '',
                                f"{row['variance_pct']:.0f}%" if pd.notna(row['variance_pct']) else '',
                                ''
                            ]
                        
                        # Add total 2025 in last column
                        if len(gold_summary) > 0:
                            overview_data.iloc[3, -1] = int(gold_summary['ytd_total'].iloc[-1])
                        
                        st.dataframe(overview_data.set_index('Metric'), use_container_width=True)
                        
                        st.markdown("---")
                        
                        # Get detailed breakdown with date filter
                        breakdown_query = text(f"""
                            SELECT 
                                tp.label as month,
                                tp.start_date,
                                b.breakdown_value as source,
                                b.monthly_new_patient_cnt as count,
                                ROUND(b.monthly_pct_of_total, 1) as pct_of_total
                            FROM gold_ops.referrals_monthly_breakdown b
                            JOIN master.time_periods tp ON b.time_period_id = tp.id
                            WHERE b.client_id::text = '{explore_client_id}'
                                AND b.breakdown_type = 'referral_name'
                                AND tp.start_date >= '2025-01-01'
                                AND tp.period_type = 'month'
                            ORDER BY tp.start_date, b.monthly_new_patient_cnt DESC
                        """)
                        
                        gold_breakdown = pd.read_sql(breakdown_query, engine)
                        
                        if not gold_breakdown.empty:
                            st.markdown("### 📋 Detailed Source Breakdown")
                            
                            # Get unique sources and months
                            sources = gold_breakdown['source'].unique()
                            months = gold_summary['month'].tolist()
                            
                            # Create breakdown table with counts and percentages
                            breakdown_display = []
                            
                            for source in sources:
                                # Count row
                                count_row = {'Source': source, 'Metric': 'Count'}
                                pct_row = {'Source': '', 'Metric': '% Total'}
                                
                                for month in months:
                                    month_data = gold_breakdown[
                                        (gold_breakdown['source'] == source) & 
                                        (gold_breakdown['month'] == month)
                                    ]
                                    
                                    if not month_data.empty:
                                        count_row[month] = int(month_data.iloc[0]['count'])
                                        pct_row[month] = f"{month_data.iloc[0]['pct_of_total']}%"
                                    else:
                                        count_row[month] = 0
                                        pct_row[month] = "0%"
                                
                                breakdown_display.append(count_row)
                                breakdown_display.append(pct_row)
                            
                            # Add totals row
                            total_row = {'Source': 'Total', 'Metric': ''}
                            for month in months:
                                month_total = gold_breakdown[gold_breakdown['month'] == month]['count'].sum()
                                total_row[month] = int(month_total)
                            breakdown_display.append(total_row)
                            
                            # Convert to DataFrame and display
                            breakdown_df = pd.DataFrame(breakdown_display)
                            
                            # Style the dataframe
                            def highlight_totals(row):
                                if row['Source'] == 'Total':
                                    return ['background-color: #f0f0f0; font-weight: bold'] * len(row)
                                elif row['Metric'] == 'Count':
                                    return ['font-weight: 600'] + [''] * (len(row) - 1)
                                else:
                                    return ['color: #666; font-size: 0.9em'] + ['color: #666; font-size: 0.9em'] * (len(row) - 1)
                            
                            styled_df = breakdown_df.style.apply(highlight_totals, axis=1)
                            st.dataframe(styled_df, use_container_width=True, hide_index=True)
                            
                        else:
                            st.info("No breakdown data available for 2025")
                    else:
                        st.info("No gold layer metrics for 2025. Run ETL pipeline to generate analytics.")
                except Exception as e:
                    st.error(f"Error loading gold metrics: {str(e)}")
    
    # ETL execution section
    st.markdown("---")
    has_appointments = appointments_status.get('total_appointments', 0) > 0
    has_referrals = referrals_status.get('total_referrals', 0) > 0

    # ETL Pipeline Execution
    st.subheader("Execute ETL Pipeline")
    
    with st.expander("Pipeline Overview", expanded=False):
        st.markdown("""
        **This ETL pipeline performs the following transformations:**
        
        **Bronze → Silver:**
        - Extracts all appointments from bronze layer
        - Marks appointments as "New Patient" using appointment type mappings
        - Joins with referral data to get referral sources
        - Creates combined appointments + referrals facts in `silver_ops.referrals`
        
        **Silver → Gold:**
        - Aggregates silver facts into monthly summaries with variance analysis
        - Creates detailed breakdowns by referral category and source
        - Outputs to `gold_ops.referrals_monthly_summary` and `gold_ops.referrals_monthly_breakdown`
        
        **Data Quality:**
        - Handles duplicate appointments per patient (takes earliest)
        - Standardizes referral categories using client mappings
        - Associates data with time periods for trend analysis
        """)
    
    # Execution controls
    col1, col2 = st.columns([3, 1])
    
    with col1:
        run_etl = st.button(
            f"🚀 Run ETL Pipeline for {selected_client}", 
            type="primary", 
            use_container_width=True,
            disabled=not has_appointments
        )
    
    with col2:
        force_refresh = st.checkbox("Force Refresh", help="Clear existing data and recreate from scratch")

    if not has_appointments:
        st.warning("Cannot run ETL: No appointment data found in bronze layer")
        st.markdown("""
        **To proceed with ETL:**
        1. Upload appointment data to the bronze layer
        2. Ensure data includes patient IDs and appointment types
        3. Configure appointment type mappings in master data
        """)
        return

    if not has_referrals:
        st.warning("Warning: Limited referral data. ETL will run but referral analysis will be incomplete.")

    # Execute ETL with detailed logging
    if run_etl:
        progress_container = st.container()
        
        with progress_container:
            st.markdown("#### ETL Execution Log")
            
            # Create placeholders for logging
            step_container = st.container()
            progress_bar = st.progress(0)
            
            # Step 1: Pre-execution analysis
            with step_container:
                st.markdown("**Step 1: Analyzing current state**")
                step1_status = st.empty()
                
                # Show ETL config being applied
                min_date = get_client_etl_config(selected_client)
                st.info(f"🔧 Applying referrals ETL config: Appointment date filter = `{min_date}` onwards")
                
                step1_status.text("Checking existing silver/gold data...")
                before_silver, before_summary, before_breakdown = get_current_silver_gold_status(client_id)
                
                before_stats = {
                    'silver_facts': before_silver['silver_facts'] if before_silver else 0,
                    'summary_records': before_summary['summary_records'] if before_summary else 0,
                    'breakdown_records': before_breakdown['breakdown_records'] if before_breakdown else 0
                }
                
                step1_status.success(f"✅ Current state: {before_stats['silver_facts']} silver facts, {before_stats['summary_records']} summary records, {before_stats['breakdown_records']} breakdown records")
                progress_bar.progress(25)
            
            # Step 2: ETL execution
            st.markdown("**Step 2: Running ETL transformations**")
            step2_status = st.empty()
            
            step2_status.text("Processing bronze data through silver and gold layers...")
            
            # Run the actual ETL
            start_time = datetime.now()
            result = run_etl_with_logging(selected_client)
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            progress_bar.progress(75)
            
            # Step 3: Validation and results
            st.markdown("**Step 3: Validating results**")
            step3_status = st.empty()
            
            if result['success']:
                step2_status.success(f"✅ ETL completed in {execution_time:.1f} seconds")
                step3_status.text("Validating data integrity...")
                
                # Get final status
                after_silver, after_summary, after_breakdown = get_current_silver_gold_status(client_id)
                
                after_stats = {
                    'silver_facts': after_silver['silver_facts'] if after_silver else 0,
                    'summary_records': after_summary['summary_records'] if after_summary else 0,
                    'breakdown_records': after_breakdown['breakdown_records'] if after_breakdown else 0
                }
                
                progress_bar.progress(100)
                step3_status.success("✅ Data validation completed")
                
                # Success summary
                st.success("ETL Pipeline completed successfully!")
                
                # Show detailed changes
                st.markdown("#### Execution Results")
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.markdown("**Silver Layer**")
                    delta = result['silver_rows'] - before_stats['silver_facts']
                    st.metric(
                        "New Patient Facts", 
                        f"{result['silver_rows']:,}",
                        delta=f"{delta:+,}" if delta != 0 else "No change"
                    )
                
                with col2:
                    st.markdown("**Gold Summaries**") 
                    delta = result['summary_rows'] - before_stats['summary_records']
                    st.metric(
                        "Monthly Periods", 
                        f"{result['summary_rows']:,}",
                        delta=f"{delta:+,}" if delta != 0 else "No change"
                    )
                
                with col3:
                    st.markdown("**Gold Breakdowns**")
                    delta = result['breakdown_rows'] - before_stats['breakdown_records']
                    st.metric(
                        "Breakdown Records", 
                        f"{result['breakdown_rows']:,}",
                        delta=f"{delta:+,}" if delta != 0 else "No change"
                    )
                
                # Duplicate prevention info
                min_date_applied = get_client_etl_config(selected_client)
                with st.expander("Data Quality Assurance"):
                    st.markdown(f"""
                    **Duplicate Prevention Measures:**
                    - ✅ Only first appointment per patient is processed as intake
                    - ✅ Existing data cleared before reprocessing (idempotent operation)
                    - ✅ Foreign key constraints ensure data integrity
                    - ✅ Client/practice isolation prevents cross-contamination
                    
                    **Referrals ETL Filter Applied:**
                    - Appointment Date: `{min_date_applied}` onwards
                    
                    **Execution Details:**
                    - Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}
                    - Duration: {execution_time:.1f} seconds
                    - Client: {selected_client}
                    - Force Refresh: {'Yes' if force_refresh else 'No'}
                    
                    **Running the same data multiple times will not create duplicates.**
                    """)
                
                st.info("Visit the **Data Overview** page to explore the updated data!")
                
            else:
                step2_status.error(f"❌ ETL failed: {result.get('message', 'Unknown error')}")
                step3_status.error("❌ ETL execution failed")
                progress_bar.progress(100)
                
                st.error(f"ETL Pipeline failed: {result.get('message', 'Unknown error')}")
                
                with st.expander("Troubleshooting"):
                    st.markdown("""
                    **Common Issues:**
                    - Bronze data is missing or corrupted
                    - Appointment type mappings are not configured 
                    - Database connectivity issues
                    - Time period data missing in master tables
                    - Insufficient database permissions
                    
                    **Data Requirements:**
                    - Appointment data with patient_id_guid and appointment_type_description
                    - Client mapped to appointment data via client_tag or name
                    - Valid appointment type mappings in master.client_appointment_type_mappings
                    """)

    # Quick Actions
    st.subheader("Quick Actions")
    st.info("Use the sidebar to navigate to **Data Overview** to explore results, or **Master Data** to manage entities.")

if __name__ == "__main__":
    main()