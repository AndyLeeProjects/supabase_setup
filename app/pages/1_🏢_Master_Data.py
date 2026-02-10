import streamlit as st
import sys
from pathlib import Path
import pandas as pd
import time
from sqlalchemy import text

# Add utils to path
sys.path.append(str(Path(__file__).parent.parent.parent / 'utils'))
sys.path.append(str(Path(__file__).parent.parent.parent / 'src'))
from connect_db import get_engine
from cache_func import (
    get_clients_cached, get_practices_cached, get_providers_cached,
    refresh_master_data_cache, setup_auto_refresh, setup_sidebar_cache_controls
)

st.set_page_config(
    page_title="Master Data Management", 
    layout="wide",
    page_icon="🏢"
)

def get_db_connection():
    return get_engine()

# Database functions
def add_client(client_data):
    """Add client to database"""
    engine = get_db_connection()
    with engine.connect() as conn:
        result = conn.execute(
            text("INSERT INTO master.clients (name, slug, status) VALUES (:name, :slug, :status) RETURNING id"),
            client_data
        )
        conn.commit()
        return result.fetchone()[0]

def add_practice(practice_data):
    """Add practice to database"""
    engine = get_db_connection()
    with engine.connect() as conn:
        result = conn.execute(
            text("""
            INSERT INTO master.practices (client_id, name, practice_type_specific, owner_name) 
            VALUES (:client_id, :name, :practice_type_specific, :owner_name) 
            RETURNING id
            """),
            practice_data
        )
        conn.commit()
        return result.fetchone()[0]

def add_provider(provider_data):
    """Add provider to database"""
    engine = get_db_connection()
    with engine.connect() as conn:
        result = conn.execute(
            text("""
            INSERT INTO master.providers (practice_id, name, provider_type) 
            VALUES (:practice_id, :name, :provider_type) 
            RETURNING id
            """),
            provider_data
        )
        conn.commit()
        return result.fetchone()[0]

def get_clients():
    """Get all clients using cache"""
    return get_clients_cached()

def get_practices(client_id=None):
    """Get practices using cache"""
    return get_practices_cached(client_id)

def get_providers(practice_id=None):
    """Get providers using cache"""
    return get_providers_cached(practice_id)

def main():
    """Master data management page"""
    
    st.title("🏢 Master Data Management")
    st.markdown("Set up the core entities that drive your business: clients, practices, and providers.")
    
    # Auto-refresh setup
    setup_auto_refresh()
    
    # Setup sidebar cache controls
    setup_sidebar_cache_controls()
    
    # Show setup progress/status
    clients_df = get_clients()
    practices_df = get_practices()
    providers_df = get_providers()
    
    # Progress indicators
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("👥 Clients", len(clients_df))
    with col2:
        st.metric("🏥 Practices", len(practices_df))
    with col3:
        st.metric("👨‍⚕️ Providers", len(providers_df))
    with col4:
        if len(clients_df) > 0 and len(practices_df) > 0:
            st.metric("✅ Setup", "Complete")
        else:
            st.metric("⏳ Setup", "Incomplete")
    
    st.markdown("---")
    
    # Determine current step based on existing data
    if len(clients_df) == 0:
        step = 1  # Need to add clients first
    elif len(practices_df) == 0:
        step = 2  # Have clients, need practices
    elif len(providers_df) == 0:
        step = 3  # Have clients and practices, need providers
    else:
        step = 4  # All setup, show overview
    
    # Progress bar
    progress = (step - 1) / 3
    st.progress(progress, text=f"Setup Progress: Step {step} of 4")
    
    # Main setup workflow
    if step == 1:
        # Step 1: Must add a client first
        st.subheader("🏗️ Step 1: Add Your First Client")
        st.info("💡 Start by adding a client organization. This is the top-level entity that will contain practices and providers.")
        
        with st.form("client_setup_form", clear_on_submit=True):
            col1, col2 = st.columns(2)
            
            with col1:
                client_name = st.text_input("Client Name *", placeholder="e.g., Wall Street Orthodontics")
                client_slug = st.text_input("Client Slug *", placeholder="e.g., wall_street_ortho", help="Short identifier for data organization")
            
            with col2:
                client_status = st.selectbox("Status", ["active", "inactive", "pending"], index=0)
            
            submitted = st.form_submit_button("➡️ Create Client & Continue", use_container_width=True, type="primary")
            
            if submitted:
                if client_name and client_slug:
                    try:
                        client_data = {
                            "name": client_name,
                            "slug": client_slug.lower().replace(" ", "_"),
                            "status": client_status
                        }
                        
                        client_id = add_client(client_data)
                        st.success(f"✅ Client '{client_name}' created successfully!")
                        st.balloons()
                        time.sleep(1)
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"❌ Error creating client: {str(e)}")
                else:
                    st.error("❌ Please fill in all required fields marked with *")
    
    elif step == 2:
        # Step 2: Add practices to existing clients
        st.subheader("🏗️ Step 2: Add Practice Locations")
        st.info("💡 Now add practice locations for your clients. Each client can have multiple practices.")
        
        # Quick add practice form
        with st.form("practice_quick_form", clear_on_submit=True):
                col1, col2 = st.columns(2)
                
                with col1:
                    client_options = dict(zip(clients_df['name'], clients_df['id']))
                    selected_client = st.selectbox("Select Client *", options=list(client_options.keys()))
                    client_id = client_options[selected_client]
                    
                    practice_name = st.text_input("Practice Name *", placeholder="e.g., Downtown Location")
                    
                with col2:
                    practice_type_specific = st.text_input("Practice Type *", placeholder="e.g., Orthodontics")
                    owner_name = st.text_input("Owner Name", placeholder="e.g., Dr. John Smith")
                
                submitted = st.form_submit_button("Add Practice", use_container_width=True)
                
                if submitted:
                    if practice_name and practice_type_specific:
                        try:
                            practice_data = {
                                "client_id": client_id,
                                "name": practice_name,
                                "practice_type_specific": practice_type_specific,
                                "owner_name": owner_name if owner_name else None
                            }
                            
                            practice_id = add_practice(practice_data)
                            st.success(f"✅ Practice '{practice_name}' added successfully!")
                            time.sleep(1)
                            st.rerun()
                            
                        except Exception as e:
                            st.error(f"❌ Error adding practice: {str(e)}")
                    else:
                        st.error("❌ Please fill in all required fields marked with *")
        
    elif step == 3:
        # Step 3: Add providers (optional but recommended)
        st.subheader("🏗️ Step 3: Add Healthcare Providers")
        st.info("💡 Add healthcare professionals to your practices. This step is optional but recommended for complete setup.")
        
        with st.form("provider_quick_form", clear_on_submit=True):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        practice_options = dict(zip([f"{row['client_name']} - {row['practice_name']}" for _, row in practices_df.iterrows()], practices_df['id']))
                        selected_practice = st.selectbox("Select Practice *", options=list(practice_options.keys()))
                        practice_id = practice_options[selected_practice]
                        
                        provider_name = st.text_input("Provider Name *", placeholder="e.g., Dr. Jane Smith")
                        
                    with col2:
                        provider_type = st.text_input("Provider Type", placeholder="e.g., Orthodontist")
                    
                    submitted = st.form_submit_button("Add Provider", use_container_width=True)
                    
                    if submitted:
                        if provider_name:
                            try:
                                provider_data = {
                                    "practice_id": practice_id,
                                    "name": provider_name,
                                    "provider_type": provider_type if provider_type else None
                                }
                                
                                provider_id = add_provider(provider_data)
                                st.success(f"✅ Provider '{provider_name}' added successfully!")
                                time.sleep(1)
                                st.rerun()
                                
                            except Exception as e:
                                st.error(f"❌ Error adding provider: {str(e)}")
                        else:
                            st.error("❌ Please enter a provider name")
        
        # Option to skip providers and continue to overview
        st.markdown("---")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("⏭️ Skip Providers & Continue", use_container_width=True):
                st.info("Proceeding to overview without adding providers...")
                time.sleep(1)
                st.rerun()
        with col2:
            if st.button("📋 View Overview", use_container_width=True):
                st.rerun()

    # Data Overview Section
    elif step == 4:
        st.subheader("📋 Master Data Overview")
        st.markdown("Complete summary of your master data entities and their relationships.")
        
        # Quick Stats
        col1, col2, col3 = st.columns(3)
        
        try:
            # Get summary statistics
            clients_df = pd.read_sql("SELECT * FROM master.clients", get_db_connection())
            practices_df = pd.read_sql("SELECT * FROM master.practices", get_db_connection())
            providers_df = pd.read_sql("SELECT * FROM master.providers", get_db_connection())
            
            with col1:
                st.metric("👥 Total Clients", len(clients_df))
                if not clients_df.empty:
                    active_clients = len(clients_df[clients_df['status'] == 'active'])
                    st.caption(f"{active_clients} active")
            
            with col2:
                st.metric("🏥 Total Practices", len(practices_df))
                if not practices_df.empty:
                    active_practices = len(practices_df[practices_df['is_active'] == True])
                    st.caption(f"{active_practices} active")
            
            with col3:
                st.metric("👨‍⚕️ Total Providers", len(providers_df))
                if not providers_df.empty:
                    active_providers = len(providers_df[providers_df['is_active'] == True])
                    st.caption(f"{active_providers} active")
        
        except Exception as e:
            st.error(f"Error loading summary statistics: {e}")
        
        st.markdown("---")
        
        # Detailed hierarchical view
        try:
            # Join all data for hierarchical display
            full_data = pd.read_sql("""
                SELECT 
                    c.name as client_name,
                    c.status as client_status,
                    p.name as practice_name,
                    p.practice_type_specific,
                    p.is_active as practice_active,
                    pr.name as provider_name,
                    pr.provider_type,
                    pr.is_active as provider_active
                FROM master.clients c
                LEFT JOIN master.practices p ON c.id = p.client_id
                LEFT JOIN master.providers pr ON p.id = pr.practice_id
                ORDER BY c.name, p.name, pr.name
            """, get_db_connection())
            
            if not full_data.empty:
                st.subheader("🏗️ Data Hierarchy")
                st.dataframe(full_data, use_container_width=True, hide_index=True)
                
                # Show summary by client
                st.subheader("📊 Summary by Client")
                client_summary = full_data.groupby('client_name').agg({
                    'practice_name': 'nunique',
                    'provider_name': 'nunique'
                }).rename(columns={
                    'practice_name': 'Practices',
                    'provider_name': 'Providers'
                })
                st.dataframe(client_summary, use_container_width=True)
                
            else:
                st.info("No master data found. Complete the setup steps above to populate your master data.")
                
        except Exception as e:
            st.error(f"Error loading detailed overview: {e}")
        
        # Action buttons for next steps
        st.markdown("---")
        st.subheader("🚀 Ready for Next Steps")
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🔄 Process ETL Pipeline", use_container_width=True):
                st.switch_page("pages/2_🔄_ETL_Pipeline.py")
        
        with col2:
            if st.button("📊 View Data Analytics", use_container_width=True):
                st.switch_page("pages/3_📊_Data_Overview.py")

if __name__ == "__main__":
    main()