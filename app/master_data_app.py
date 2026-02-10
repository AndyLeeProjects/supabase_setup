import streamlit as st
import sys
from pathlib import Path
import pandas as pd
from datetime import date
from sqlalchemy import text

# Add utils to path
sys.path.append(str(Path(__file__).parent.parent / 'utils'))
from connect_db import get_engine

st.set_page_config(page_title="Master Data Management", layout="wide")

def get_db_connection():
    return get_engine()

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
    """Get all clients"""
    engine = get_db_connection()
    return pd.read_sql("SELECT id, name, slug, status FROM master.clients ORDER BY name", engine)

def get_practices(client_id=None):
    """Get practices for a client"""
    engine = get_db_connection()
    if client_id:
        return pd.read_sql(
            "SELECT id, name, practice_type_specific FROM master.practices WHERE client_id = %(client_id)s ORDER BY name", 
            engine, params={"client_id": client_id}
        )
    return pd.read_sql("""
        SELECT p.id, p.name as practice_name, c.name as client_name, p.practice_type_specific 
        FROM master.practices p
        JOIN master.clients c ON p.client_id = c.id
        ORDER BY c.name, p.name
    """, engine)

# Main App
st.title("🏢 Master Data Management")
st.sidebar.title("Navigation")

# Sidebar navigation
page = st.sidebar.selectbox("Choose a page", ["Add Client", "Add Practice", "Add Provider", "View Data"])

if page == "Add Client":
    st.header("➕ Add New Client")
    
    with st.form("client_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            client_name = st.text_input("Client Name *", placeholder="e.g., Wall Street Orthodontics")
            client_slug = st.text_input("Client Slug *", placeholder="e.g., wall_street_ortho")
        
        with col2:
            client_status = st.selectbox("Status", ["active", "inactive", "pending"])
        
        submitted = st.form_submit_button("Add Client")
        
        if submitted:
            if client_name and client_slug:
                try:
                    client_data = {
                        "name": client_name,
                        "slug": client_slug.lower().replace(" ", "_"),
                        "status": client_status
                    }
                    
                    client_id = add_client(client_data)
                    st.success(f"✅ Client '{client_name}' added successfully with ID: {client_id}")
                    
                except Exception as e:
                    st.error(f"❌ Error adding client: {str(e)}")
            else:
                st.error("❌ Please fill in all required fields marked with *")

elif page == "Add Practice":
    st.header("🏥 Add New Practice")
    
    # Get clients for dropdown
    clients_df = get_clients()
    
    if clients_df.empty:
        st.warning("⚠️ No clients found. Please add a client first.")
    else:
        with st.form("practice_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                client_options = dict(zip(clients_df['name'], clients_df['id']))
                selected_client = st.selectbox("Select Client *", options=list(client_options.keys()))
                client_id = client_options[selected_client]
                
                practice_name = st.text_input("Practice Name *", placeholder="e.g., Downtown Location")
                practice_type_specific = st.text_input("Practice Type *", placeholder="e.g., Orthodontics")
                
            with col2:
                owner_name = st.text_input("Owner Name", placeholder="e.g., Dr. John Smith")
            
            submitted = st.form_submit_button("Add Practice")
            
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
                        st.success(f"✅ Practice '{practice_name}' added successfully with ID: {practice_id}")
                        
                    except Exception as e:
                        st.error(f"❌ Error adding practice: {str(e)}")
                else:
                    st.error("❌ Please fill in all required fields marked with *")

elif page == "Add Provider":
    st.header("👨‍⚕️ Add New Provider")
    
    # Get practices for dropdown
    practices_df = get_practices()
    
    if practices_df.empty:
        st.warning("⚠️ No practices found. Please add a practice first.")
    else:
        with st.form("provider_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                practice_options = dict(zip([f"{row['client_name']} - {row['practice_name']}" for _, row in practices_df.iterrows()], practices_df['id']))
                selected_practice = st.selectbox("Select Practice *", options=list(practice_options.keys()))
                practice_id = practice_options[selected_practice]
                
                provider_name = st.text_input("Provider Name *", placeholder="e.g., Dr. John Smith")
                
            with col2:
                provider_type = st.text_input("Provider Type", placeholder="e.g., Orthodontist")
            
            submitted = st.form_submit_button("Add Provider")
            
            if submitted:
                if provider_name:
                    try:
                        provider_data = {
                            "practice_id": practice_id,
                            "name": provider_name,
                            "provider_type": provider_type if provider_type else None
                        }
                        
                        provider_id = add_provider(provider_data)
                        st.success(f"✅ Provider '{provider_name}' added successfully with ID: {provider_id}")
                        
                    except Exception as e:
                        st.error(f"❌ Error adding provider: {str(e)}")
                else:
                    st.error("❌ Please enter a provider name")

elif page == "View Data":
    st.header("📊 View Master Data")
    
    tab1, tab2, tab3 = st.tabs(["Clients", "Practices", "Providers"])
    
    with tab1:
        st.subheader("Clients")
        try:
            clients_df = pd.read_sql("SELECT * FROM master.clients ORDER BY name", get_db_connection())
            if not clients_df.empty:
                st.dataframe(clients_df, use_container_width=True)
            else:
                st.info("No clients found")
        except Exception as e:
            st.error(f"Error loading clients: {e}")
    
    with tab2:
        st.subheader("Practices")
        try:
            practices_df = pd.read_sql(
                """
                SELECT p.id, c.name as client_name, p.name as practice_name, 
                       p.practice_type_specific, p.owner_name, p.is_active
                FROM master.practices p
                JOIN master.clients c ON p.client_id = c.id
                ORDER BY c.name, p.name
                """, 
                get_db_connection()
            )
            if not practices_df.empty:
                st.dataframe(practices_df, use_container_width=True)
            else:
                st.info("No practices found")
        except Exception as e:
            st.error(f"Error loading practices: {e}")
    
    with tab3:
        st.subheader("Providers")
        try:
            providers_df = pd.read_sql(
                """
                SELECT pr.id, c.name as client_name, p.name as practice_name, 
                       pr.name as provider_name, pr.provider_type, pr.is_active
                FROM master.providers pr
                JOIN master.practices p ON pr.practice_id = p.id
                JOIN master.clients c ON p.client_id = c.id
                ORDER BY c.name, p.name, pr.name
                """, 
                get_db_connection()
            )
            if not providers_df.empty:
                st.dataframe(providers_df, use_container_width=True)
            else:
                st.info("No providers found")
        except Exception as e:
            st.error(f"Error loading providers: {e}")

# Footer
st.sidebar.markdown("---")
st.sidebar.markdown("### 🚀 Quick Start")
st.sidebar.markdown("1. Add Client first")
st.sidebar.markdown("2. Add Practice(s)")  
st.sidebar.markdown("3. Add Provider(s)")
st.sidebar.markdown("4. View data to verify")