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

def get_bronze_data_status(client_tag=None):
    """Check status of bronze data for a client using cache"""
    return get_bronze_data_status_cached(client_tag)

def get_current_silver_gold_status(client_id=None):
    """Check current silver and gold data status using cache"""
    return get_silver_gold_status_cached()

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
    st.title("🔄 ETL Pipeline Management")
    st.markdown("Transform bronze data into silver facts and gold metrics with full visibility into changes.")

    # Auto-refresh setup
    setup_auto_refresh()
    
    # Setup sidebar cache controls
    setup_sidebar_cache_controls()

    # Client Selection Section
    st.subheader("📋 Client Selection")
    
    clients_df = get_clients()
    
    if clients_df.empty:
        st.warning("⚠️ No clients found. Please add clients in the Master Data page first.")
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
    st.subheader("🥉 Bronze Data Status")
    
    client_tag = None
    if selected_client != "All Clients":
        # Map client name to tag (this could be more sophisticated)
        client_tag = "wso" if "wall street" in selected_client.lower() else None
    
    bronze_status = get_bronze_data_status(client_tag)
    
    # Extract appointments and referrals data from the cached response
    appointments_status = bronze_status.get('appointments', {})
    referrals_status = bronze_status.get('referrals', {})
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**📅 Appointments Data**")
        if appointments_status['total_appointments'] > 0:
            st.metric("Total Appointments", f"{appointments_status['total_appointments']:,}")
            st.metric("Unique Patients", f"{appointments_status['unique_patients']:,}")
            st.metric("Appointment Types", appointments_status['appointment_types'])
            if appointments_status['earliest_date'] and appointments_status['latest_date']:
                st.info(f"📊 Date Range: {appointments_status['earliest_date']} to {appointments_status['latest_date']}")
        else:
            st.warning("⚠️ No appointment data found")
    
    with col2:
        st.markdown("**👥 Referrals Data**")
        if referrals_status['total_referrals'] > 0:
            st.metric("Total Referrals", f"{referrals_status['total_referrals']:,}")
            st.metric("Referred Patients", f"{referrals_status['unique_referred_patients']:,}")
            st.metric("Referral Types", referrals_status['referral_types'])
        else:
            st.warning("⚠️ No referral data found")

    # Current Silver/Gold Status
    st.subheader("🥈🥇 Current Processed Data")
    
    client_id = None
    if selected_client != "All Clients":
        client_id = clients_df[clients_df['name'] == selected_client].iloc[0]['id']
    
    silver_status, summary_status, breakdown_status = get_current_silver_gold_status(client_id)
    
    if silver_status:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("🥈 Silver Facts", f"{silver_status['silver_facts']:,}")
            if silver_status['earliest_intake'] and silver_status['latest_intake']:
                st.caption(f"From {silver_status['earliest_intake']} to {silver_status['latest_intake']}")
        with col2:
            st.metric("🥇 Monthly Summaries", f"{summary_status['summary_records']:,}")
        with col3:
            st.metric("📊 Breakdown Records", f"{breakdown_status['breakdown_records']:,}")
    else:
        st.info("💡 No processed data found. Run the ETL pipeline to create silver and gold layer data.")

    # ETL Pipeline Execution
    st.subheader("🚀 Execute ETL Pipeline")
    
    with st.expander("📖 Pipeline Overview", expanded=False):
        st.markdown("""
        **This ETL pipeline performs the following transformations:**
        
        **🥉 Bronze → 🥈 Silver:**
        - Extracts new patient appointments from bronze layer
        - Identifies first appointments per patient using appointment type mappings
        - Joins with referral data to get referral sources
        - Creates canonical facts in `silver_ops.fact_new_patient_intake`
        
        **🥈 Silver → 🥇 Gold:**
        - Aggregates silver facts into monthly summaries with variance analysis
        - Creates detailed breakdowns by referral category and source
        - Outputs to `gold_ops.referrals_monthly_summary` and `gold_ops.referrals_monthly_breakdown`
        
        **📊 Data Quality:**
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
            disabled=appointments_status['total_appointments'] == 0
        )
    
    with col2:
        force_refresh = st.checkbox("🔄 Force Refresh", help="Clear existing data and recreate from scratch")

    if appointments_status['total_appointments'] == 0:
        st.warning("⚠️ Cannot run ETL: No appointment data found in bronze layer")
        return

    # Execute ETL
    if run_etl:
        progress_container = st.container()
        
        with progress_container:
            # Show progress
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            status_text.text("🔍 Analyzing current state...")
            progress_bar.progress(25)
            
            status_text.text("🔄 Running ETL transformations...")
            progress_bar.progress(50)
            
            # Run the actual ETL
            result = run_etl_with_logging(selected_client)
            
            progress_bar.progress(75)
            status_text.text("📊 Validating results...")
            
            progress_bar.progress(100)
            
            # Clear progress indicators
            progress_bar.empty()
            status_text.empty()
            
            # Show results
            if result['success']:
                st.success("✅ ETL Pipeline completed successfully!")
                
                # Show detailed changes
                changes = result.get('changes', {})
                
                st.subheader("📈 What Changed")
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.markdown("**🥈 Silver Layer**")
                    if 'silver_before' in changes:
                        delta = result['silver_rows'] - changes['silver_before']
                        st.metric(
                            "New Patient Facts", 
                            f"{result['silver_rows']:,}",
                            delta=f"{delta:+,}" if delta != 0 else "No change"
                        )
                    else:
                        st.metric("New Patient Facts", f"{result['silver_rows']:,}")
                
                with col2:
                    st.markdown("**🥇 Gold Summaries**")
                    if 'summary_before' in changes:
                        delta = result['summary_rows'] - changes['summary_before']
                        st.metric(
                            "Monthly Periods", 
                            f"{result['summary_rows']:,}",
                            delta=f"{delta:+,}" if delta != 0 else "No change"
                        )
                    else:
                        st.metric("Monthly Periods", f"{result['summary_rows']:,}")
                
                with col3:
                    st.markdown("**📊 Gold Breakdowns**")
                    if 'breakdown_before' in changes:
                        delta = result['breakdown_rows'] - changes['breakdown_before']
                        st.metric(
                            "Breakdown Records", 
                            f"{result['breakdown_rows']:,}",
                            delta=f"{delta:+,}" if delta != 0 else "No change"
                        )
                    else:
                        st.metric("Breakdown Records", f"{result['breakdown_rows']:,}")
                
                # Show execution details
                if 'changes' in result:
                    with st.expander("🔍 Execution Details"):
                        st.json({
                            'execution_time': changes.get('timestamp'),
                            'client': selected_client,
                            'before_state': {
                                'silver_facts': changes.get('silver_before', 0),
                                'summary_records': changes.get('summary_before', 0),
                                'breakdown_records': changes.get('breakdown_before', 0)
                            },
                            'after_state': {
                                'silver_facts': changes.get('silver_after', 0),
                                'summary_records': changes.get('summary_after', 0),
                                'breakdown_records': changes.get('breakdown_after', 0)
                            }
                        })
                
                st.info("💡 Visit the **📊 Data Overview** page to explore the updated data!")
                
            else:
                st.error(f"❌ ETL Pipeline failed: {result.get('message', 'Unknown error')}")
                
                with st.expander("🔍 Troubleshooting Tips"):
                    st.markdown("""
                    **Common Issues:**
                    - Ensure bronze data exists and is properly formatted
                    - Check that appointment type mappings are configured
                    - Verify database connectivity and permissions
                    - Make sure time period data exists in master tables
                    """)

    # Quick Actions
    st.subheader("⚡ Quick Actions")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("📊 View Data Overview", use_container_width=True):
            st.switch_page("pages/3_📊_Data_Overview.py")
    
    with col2:
        if st.button("🏢 Manage Master Data", use_container_width=True):
            st.switch_page("pages/1_🏢_Master_Data.py")
    
    with col3:
        if st.button("🏠 Back to Home", use_container_width=True):
            st.switch_page("🏠_Home.py")

if __name__ == "__main__":
    main()