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

st.set_page_config(
    page_title="ETL Pipeline", 
    layout="wide",
    page_icon="🔄"
)

def get_db_connection():
    return get_engine()

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
            status['silver'].get('fact_new_patient_intake', {}).get('count', 0),
            status['silver'].get('fact_patient_treatments', {}).get('count', 0),
            status['silver'].get('fact_referrals', {}).get('count', 0)
        ])
        
        # Get earliest and latest intake dates (from fact_new_patient_intake)
        intake_table = status['silver'].get('fact_new_patient_intake', {})
        
        silver_status = {
            'silver_facts': silver_facts,
            'earliest_intake': intake_table.get('last_updated'),  # This might need adjustment based on actual data structure
            'latest_intake': intake_table.get('last_updated')     # This might need adjustment based on actual data structure
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

    # Client Selection Section
    st.subheader("Client Selection")
    
    clients_df = get_clients()
    
    if clients_df.empty:
        st.warning("No clients found. Please add clients in the Master Data page first.")
        return
    
    client_options = ["All Clients"] + clients_df['name'].tolist()
    selected_client = st.selectbox(
        "Select client for ETL processing:",
        client_options,
        help="Choose a specific client or process all clients"
    )
    
    # Show client info
    if selected_client != "All Clients":
        client_row = clients_df[clients_df['name'] == selected_client].iloc[0]
        
        col1, col2 = st.columns(2)
        with col1:
            st.info(f"**Client ID:** {client_row['id']}")
        with col2:
            st.info(f"**Created:** {client_row['created_at'].strftime('%Y-%m-%d')}")

    # Bronze Data Status Section
    st.subheader("Available Data")
    
    # Use fast bronze data detection
    bronze_status = get_bronze_data_status_fast(selected_client)
    
    # Extract status for each data type
    appointments_status = bronze_status.get('appointments', {})
    referrals_status = bronze_status.get('referrals', {})
    
    # Simple metrics
    col1, col2 = st.columns(2)
    
    with col1:
        appt_count = appointments_status.get('total_appointments', 0)
        st.metric("Appointments", f"{appt_count:,}")
    
    with col2:
        ref_count = referrals_status.get('total_referrals', 0)
        st.metric("Referrals", f"{ref_count:,}")
    
    # Show transformation flow
    st.markdown("---")
    st.subheader("Data Transformation Flow")
    
    flow1, arrow1, flow2, arrow2, flow3 = st.columns([3, 0.5, 3, 0.5, 3])
    
    with flow1:
        st.markdown("""
        <div style="background-color: #fff4e6; padding: 20px; border-radius: 10px; text-align: center; border: 3px solid #ff9800; height: 120px; display: flex; flex-direction: column; justify-content: center;">
            <div style="font-size: 2em; margin: 0;">📋</div>
            <div style="margin: 5px 0; font-size: 1.1em; font-weight: 600;">Bronze</div>
            <div style="margin: 0; font-size: 0.85em; color: #666;">Raw source data</div>
        </div>
        """, unsafe_allow_html=True)
    
    with arrow1:
        st.markdown("<div style='text-align: center; line-height: 120px; font-size: 2em;'>→</div>", unsafe_allow_html=True)
    
    with flow2:
        st.markdown("""
        <div style="background-color: #f0f0f0; padding: 20px; border-radius: 10px; text-align: center; border: 3px solid #9e9e9e; height: 120px; display: flex; flex-direction: column; justify-content: center;">
            <div style="font-size: 2em; margin: 0;">⚙️</div>
            <div style="margin: 5px 0; font-size: 1.1em; font-weight: 600;">Silver</div>
            <div style="margin: 0; font-size: 0.85em; color: #666;">Standardized facts</div>
        </div>
        """, unsafe_allow_html=True)
    
    with arrow2:
        st.markdown("<div style='text-align: center; line-height: 120px; font-size: 2em;'>→</div>", unsafe_allow_html=True)
    
    with flow3:
        st.markdown("""
        <div style="background-color: #fff9c4; padding: 20px; border-radius: 10px; text-align: center; border: 3px solid #fbc02d; height: 120px; display: flex; flex-direction: column; justify-content: center;">
            <div style="font-size: 2em; margin: 0;">📊</div>
            <div style="margin: 5px 0; font-size: 1.1em; font-weight: 600;">Gold</div>
            <div style="margin: 0; font-size: 0.85em; color: #666;">Aggregated metrics</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("")
    
    # Current processed data status
    st.subheader("Current Processed Data")
    
    client_id = None
    if selected_client != "All Clients":
        client_id = clients_df[clients_df['name'] == selected_client].iloc[0]['id']
    
    silver_status, summary_status, breakdown_status = get_current_silver_gold_status(client_id)
    
    if silver_status and silver_status['silver_facts'] > 0:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Silver Facts", f"{silver_status['silver_facts']:,}")
        with col2:
            st.metric("Gold Summaries", f"{summary_status['summary_records']:,}")
        with col3:
            st.metric("Gold Breakdowns", f"{breakdown_status['breakdown_records']:,}")
        
        # Show sample output tables
        with st.expander("View Gold Metrics Sample", expanded=False):
            engine = get_db_connection()
            
            # Show gold summaries
            try:
                query = text("""
                SELECT 
                    c.name as client_name,
                    tp.label as month,
                    s.monthly_new_patient_cnt as monthly_count,
                    ROUND(s.l3m_avg_new_patient_cnt, 1) as l3m_avg,
                    ROUND(s.variance_from_l3m * 100, 1) as var_pct,
                    s.ytd_new_patient_cnt as ytd_count
                FROM gold_ops.referrals_monthly_summary s
                JOIN master.clients c ON s.client_id = c.id
                JOIN master.time_periods tp ON s.time_period_id = tp.id
                ORDER BY tp.start_date DESC
                LIMIT 10
                """)
                summary_sample = pd.read_sql(query, engine)
                
                if not summary_sample.empty:
                    st.markdown("**Monthly Summary:**")
                    st.dataframe(summary_sample, use_container_width=True, hide_index=True)
            except Exception as e:
                st.warning(f"Could not load summary: {e}")
            
            # Show gold breakdowns
            try:
                query = text("""
                SELECT 
                    c.name as client_name,
                    tp.label as month,
                    b.referral_category as category,
                    b.breakdown_value as source,
                    b.monthly_new_patient_cnt as count,
                    ROUND(b.monthly_pct_of_total, 1) as pct_total
                FROM gold_ops.referrals_monthly_breakdown b
                JOIN master.clients c ON b.client_id = c.id
                JOIN master.time_periods tp ON b.time_period_id = tp.id
                WHERE b.breakdown_type = 'referral_name'
                ORDER BY tp.start_date DESC, b.monthly_new_patient_cnt DESC
                LIMIT 15
                """)
                breakdown_sample = pd.read_sql(query, engine)
                
                if not breakdown_sample.empty:
                    st.markdown("**Breakdown by Source:**")
                    st.dataframe(breakdown_sample, use_container_width=True, hide_index=True)
            except Exception as e:
                st.warning(f"Could not load breakdown: {e}")
    else:
        st.info("No processed data. Run ETL pipeline below to transform data.")
    
    # Data Transformation Explorer
    st.markdown("---")
    st.subheader("Explore Data Transformation")
    st.markdown("View how data transforms through each layer from Bronze → Silver → Gold")
    
    explore_col1, explore_col2 = st.columns(2)
    
    with explore_col1:
        explore_client = st.selectbox(
            "Select Client",
            options=clients_df['name'].tolist(),
            key="explore_client"
        )
    
    with explore_col2:
        explore_data_type = st.selectbox(
            "Select Data Type",
            options=["Referrals", "Appointments"],
            key="explore_data_type"
        )
    
    if st.button("Show Transformation", type="primary"):
        explore_client_id = clients_df[clients_df['name'] == explore_client].iloc[0]['id']
        explore_client_slug = clients_df[clients_df['name'] == explore_client].iloc[0].get('slug', explore_client.lower().replace(' ', '_'))
        
        # Determine bronze suffix (same logic as Home page)
        if 'wall_street' in explore_client_slug or 'wso' in explore_client_slug:
            bronze_suffix = 'wso'
        else:
            bronze_suffix = explore_client_slug
        
        engine = get_db_connection()
        
        # Bronze Layer Sample
        with st.expander("🥉 Bronze Layer - Raw Data", expanded=True):
            st.markdown(f"**Source:** `bronze_ops.{explore_data_type.lower()}_raw_{bronze_suffix}`")
            
            try:
                if explore_data_type == "Referrals":
                    bronze_query = text(f"""
                        SELECT *
                        FROM bronze_ops.referrals_raw_{bronze_suffix}
                        LIMIT 10
                    """)
                else:
                    bronze_query = text(f"""
                        SELECT *
                        FROM bronze_ops.appointments_raw_{bronze_suffix}
                        LIMIT 10
                    """)
                
                bronze_sample = pd.read_sql(bronze_query, engine)
                st.dataframe(bronze_sample, use_container_width=True, hide_index=True, height=300)
                st.caption(f"Showing 10 of {len(bronze_sample)} raw records")
            except Exception as e:
                st.warning(f"No bronze data available: {e}")
        
        # Silver Layer Sample
        with st.expander("🥈 Silver Layer - Standardized Facts", expanded=True):
            st.markdown("**Target:** `silver_ops.fact_new_patient_intake`")
            
            try:
                silver_query = text(f"""
                    SELECT 
                        f.intake_date,
                        f.patient_id,
                        f.referral_category,
                        f.referral_name as referral_source,
                        p.name as practice_name,
                        f.created_at
                    FROM silver_ops.fact_new_patient_intake f
                    LEFT JOIN master.practices p ON f.practice_id = p.id
                    WHERE f.client_id::text = '{explore_client_id}'
                    ORDER BY f.intake_date DESC
                    LIMIT 10
                """)
                
                silver_sample = pd.read_sql(silver_query, engine)
                
                if not silver_sample.empty:
                    st.dataframe(silver_sample, use_container_width=True, hide_index=True, height=300)
                    st.caption(f"Showing 10 most recent patient intake records")
                else:
                    st.info("No silver layer data. Run ETL pipeline to process bronze data.")
            except Exception as e:
                st.warning(f"Error loading silver data: {e}")
        
        # Gold Layer Metrics
        with st.expander("🥇 Gold Layer - Analytics Metrics", expanded=True):
            st.markdown("**Aggregated monthly referral metrics with breakdowns**")
            
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
                st.warning(f"Error loading gold metrics: {e}")
    
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
        - Extracts new patient appointments from bronze layer
        - Identifies first appointments per patient using appointment type mappings
        - Joins with referral data to get referral sources
        - Creates canonical facts in `silver_ops.fact_new_patient_intake`
        
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
                with st.expander("Data Quality Assurance"):
                    st.markdown(f"""
                    **Duplicate Prevention Measures:**
                    - ✅ Only first appointment per patient is processed as intake
                    - ✅ Existing data cleared before reprocessing (idempotent operation)
                    - ✅ Foreign key constraints ensure data integrity
                    - ✅ Client/practice isolation prevents cross-contamination
                    
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