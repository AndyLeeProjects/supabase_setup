import streamlit as st
import sys
from pathlib import Path
import pandas as pd
import time
from datetime import date
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
    with engine.begin() as conn:
        result = conn.execute(
            text("INSERT INTO master.clients (name, slug, status) VALUES (:name, :slug, :status) RETURNING id"),
            client_data
        )
        return result.fetchone()[0]

def add_practice(practice_data):
    """Add practice to database"""
    engine = get_db_connection()
    with engine.begin() as conn:
        result = conn.execute(
            text("""
            INSERT INTO master.practices (client_id, name, practice_type_specific, owner_name) 
            VALUES (:client_id, :name, :practice_type_specific, :owner_name) 
            RETURNING id
            """),
            practice_data
        )
        return result.fetchone()[0]

def add_provider(provider_data):
    """Add provider to database"""
    engine = get_db_connection()
    with engine.begin() as conn:
        result = conn.execute(
            text("""
            INSERT INTO master.providers (practice_id, name, provider_type) 
            VALUES (:practice_id, :name, :provider_type) 
            RETURNING id
            """),
            provider_data
        )
        return result.fetchone()[0]

def add_appointment_type_mapping(mapping_data):
    """Add appointment type mapping to database"""
    engine = get_db_connection()
    with engine.begin() as conn:
        result = conn.execute(
            text("""
            INSERT INTO master.appointment_type_mappings 
                (client_id, practice_id, source_appointment_type, standardized_category, 
                 start_date, end_date, notes) 
            VALUES (:client_id, :practice_id, :source_appointment_type, :standardized_category, 
                    :start_date, :end_date, :notes) 
            RETURNING id
            """),
            mapping_data
        )
        return result.fetchone()[0]

def get_appointment_type_mappings(client_id=None, include_inactive=False):
    """Get appointment type mappings"""
    engine = get_db_connection()
    
    query = """
    SELECT 
        m.id,
        m.client_id,
        c.name as client_name,
        m.practice_id,
        p.name as practice_name,
        m.source_appointment_type,
        m.standardized_category,
        m.start_date,
        m.end_date,
        m.notes,
        CASE 
            WHEN m.end_date IS NULL OR m.end_date >= CURRENT_DATE THEN 'Active'
            ELSE 'Inactive'
        END as status
    FROM master.appointment_type_mappings m
    JOIN master.clients c ON m.client_id = c.id
    LEFT JOIN master.practices p ON m.practice_id = p.id
    WHERE 1=1
    """
    
    params = {}
    if client_id:
        query += " AND m.client_id = :client_id"
        params['client_id'] = client_id
    
    if not include_inactive:
        query += " AND (m.end_date IS NULL OR m.end_date >= CURRENT_DATE)"
    
    query += " ORDER BY c.name, m.standardized_category, m.source_appointment_type"
    
    return pd.read_sql(text(query), engine, params=params)

def get_clients():
    """Get all clients using cache"""
    return get_clients_cached()

def get_practices(client_id=None):
    """Get practices using cache"""
    return get_practices_cached(client_id)

def get_providers(practice_id=None):
    """Get providers using cache"""
    return get_providers_cached(practice_id)

def check_setup_completeness():
    """Check which master data setups are complete vs incomplete"""
    clients_df = get_clients()
    practices_df = get_practices()
    providers_df = get_providers()
    
    if clients_df.empty:
        return {'status': 'no_clients'}
    
    if practices_df.empty:
        return {'status': 'no_practices'}
    
    # Build client status list
    client_status = []
    practices_with_providers = set(providers_df['practice_id'].unique()) if not providers_df.empty else set()
    
    for _, client in clients_df.iterrows():
        client_practices = practices_df[practices_df['client_id'] == client['id']]
        
        if client_practices.empty:
            client_status.append({
                'name': client['name'],
                'issue': 'Missing practices'
            })
        else:
            client_status.append({
                'name': client['name'],
                'issue': None
            })
    
    # Build practice status list
    practice_status = []
    for _, practice in practices_df.iterrows():
        if practice['id'] not in practices_with_providers:
            practice_status.append({
                'name': practice['practice_name'],
                'issue': 'Missing providers'
            })
        else:
            practice_status.append({
                'name': practice['practice_name'],
                'issue': None
            })
    
    return {
        'status': 'detailed',
        'clients': client_status,
        'practices': practice_status
    }

def main():
    """Master data management page"""
    
    st.title("Master Data Management")
    st.markdown("Manage your core business entities: clients, practices, and providers.")
    
    # Auto-refresh setup
    setup_auto_refresh()
    
    # Setup sidebar cache controls
    setup_sidebar_cache_controls()
    
    # Get current data
    clients_df = get_clients()
    practices_df = get_practices()
    providers_df = get_providers()
    
    # Status overview
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Clients", len(clients_df))
    with col2:
        st.metric("Practices", len(practices_df))  
    with col3:
        st.metric("Providers", len(providers_df))
    
    # Setup completeness check
    status = check_setup_completeness()
    
    if status['status'] == 'no_clients':
        st.info("Start by adding your first client")
    
    elif status['status'] == 'no_practices':
        st.info("Add practices to your clients")
    
    else:
        # Check if there are any issues
        has_issues = any(c['issue'] for c in status['clients']) or any(p['issue'] for p in status['practices'])
        
        if has_issues:
            with st.expander("⚠️ Incomplete Items", expanded=True):
                # Show clients with issues
                client_issues = [c for c in status['clients'] if c['issue']]
                if client_issues:
                    st.markdown("**Clients:**")
                    for client in status['clients']:
                        if client['issue']:
                            st.markdown(f"⚠️ {client['name']} — {client['issue']}")
                    st.markdown("")
                
                # Show practices with issues
                practice_issues = [p for p in status['practices'] if p['issue']]
                if practice_issues:
                    st.markdown("**Practices:**")
                    for practice in status['practices']:
                        if practice['issue']:
                            st.markdown(f"⚠️ {practice['name']} — {practice['issue']}")
        else:
            st.success("✅ All items configured")
    
    st.markdown("---")
    
    # Main tabs - flexible workflow
    tab1, tab2, tab3, tab4 = st.tabs(["Add Entities", "View & Manage", "Bulk Import", "Data Relationships"])
    
    with tab1:
        st.subheader("Add New Entities")
        st.markdown("Add clients, practices, and providers in any order based on your needs.")
        
        # Entity type selection
        entity_type = st.radio(
            "What do you want to add?",
            ["Client", "Practice", "Provider", "Appointment Type Mapping"],
            horizontal=True
        )
        
        if entity_type == "Client":
            st.markdown("#### Add New Client")
            st.markdown("*A client is the top-level organization that owns practices.*")
            
            with st.form("add_client_form", clear_on_submit=True):
                col1, col2 = st.columns(2)
                
                with col1:
                    client_name = st.text_input("Client Name *", placeholder="e.g., Wall Street Orthodontics")
                    client_slug = st.text_input(
                        "Client Slug *", 
                        placeholder="e.g., wall_street_ortho",
                        help="Short identifier used in data organization"
                    )
                
                with col2:
                    client_status = st.selectbox("Status", ["active", "inactive", "pending"], index=0)
                
                submitted = st.form_submit_button("Add Client", type="primary")
                
                if submitted:
                    if client_name and client_slug:
                        try:
                            client_data = {
                                "name": client_name,
                                "slug": client_slug.lower().replace(" ", "_"),
                                "status": client_status
                            }
                            
                            client_id = add_client(client_data)
                            st.success(f"✅ Client '{client_name}' added successfully!")
                            refresh_master_data_cache()
                            time.sleep(1)
                            st.rerun()
                            
                        except Exception as e:
                            st.error(f"❌ Error: {str(e)}")
                    else:
                        st.error("❌ Please fill in all required fields marked with *")
        
        elif entity_type == "Practice":
            st.markdown("#### Add New Practice")
            st.markdown("*A practice is a location or business unit within a client organization.*")
            
            if clients_df.empty:
                st.warning("⚠️ Add a client first before creating practices.")
            else:
                with st.form("add_practice_form", clear_on_submit=True):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        client_options = dict(zip(clients_df['name'], clients_df['id']))
                        selected_client = st.selectbox("Client *", options=list(client_options.keys()))
                        client_id = client_options[selected_client]
                        
                        practice_name = st.text_input("Practice Name *", placeholder="e.g., Downtown Location")
                    
                    with col2:
                        practice_type = st.text_input("Practice Type", placeholder="e.g., Orthodontics")
                        owner_name = st.text_input("Owner Name", placeholder="e.g., Dr. John Smith")
                    
                    submitted = st.form_submit_button("Add Practice", type="primary")
                    
                    if submitted:
                        if practice_name:
                            try:
                                practice_data = {
                                    "client_id": client_id,
                                    "name": practice_name,
                                    "practice_type_specific": practice_type if practice_type else None,
                                    "owner_name": owner_name if owner_name else None
                                }
                                
                                practice_id = add_practice(practice_data)
                                st.success(f"✅ Practice '{practice_name}' added to {selected_client}!")
                                refresh_master_data_cache()
                                time.sleep(1)
                                st.rerun()
                                
                            except Exception as e:
                                st.error(f"❌ Error: {str(e)}")
                        else:
                            st.error("❌ Please enter a practice name")
        
        elif entity_type == "Provider":
            st.markdown("#### Add New Provider")
            st.markdown("*A provider is a healthcare professional working at a practice.*")
            
            if practices_df.empty:
                st.warning("⚠️ Add a practice first before creating providers.")
            else:
                with st.form("add_provider_form", clear_on_submit=True):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        practice_options = dict(zip(
                            [f"{row['client_name']} - {row['practice_name']}" for _, row in practices_df.iterrows()],
                            practices_df['id']
                        ))
                        selected_practice = st.selectbox("Practice *", options=list(practice_options.keys()))
                        practice_id = practice_options[selected_practice]
                        
                        provider_name = st.text_input("Provider Name *", placeholder="e.g., Dr. Jane Smith")
                    
                    with col2:
                        provider_type = st.text_input("Provider Type", placeholder="e.g., Orthodontist")
                    
                    submitted = st.form_submit_button("Add Provider", type="primary")
                    
                    if submitted:
                        if provider_name:
                            try:
                                provider_data = {
                                    "practice_id": practice_id,
                                    "name": provider_name,
                                    "provider_type": provider_type if provider_type else None
                                }
                                
                                provider_id = add_provider(provider_data)
                                st.success(f"✅ Provider '{provider_name}' added to {selected_practice}!")
                                refresh_master_data_cache()
                                time.sleep(1)
                                st.rerun()
                                
                            except Exception as e:
                                st.error(f"❌ Error: {str(e)}")
                        else:
                            st.error("❌ Please enter a provider name")
        
        elif entity_type == "Appointment Type Mapping":
            st.markdown("#### Add Appointment Type Mapping(s)")
            st.markdown("*Map source appointment codes to standardized categories. Enter multiple codes (one per line) for bulk entry.*")
            
            if clients_df.empty:
                st.warning("⚠️ Add a client first before creating appointment type mappings.")
            else:
                with st.form("add_apt_mapping_form", clear_on_submit=True):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        client_options = dict(zip(clients_df['name'], clients_df['id']))
                        selected_client = st.selectbox("Client *", options=list(client_options.keys()))
                        client_id = client_options[selected_client]
                        
                        # Get practices for selected client
                        client_practices = practices_df[practices_df['client_id'] == client_id]
                        practice_scope = st.radio(
                            "Mapping Scope *",
                            ["All Practices (Client-wide)", "Specific Practice"],
                            index=1,
                            help="Apply this mapping to all practices or just one?"
                        )
                        
                        practice_id = None
                        if practice_scope == "Specific Practice":
                            if not client_practices.empty:
                                practice_options = dict(zip(client_practices['practice_name'], client_practices['id']))
                                selected_practice = st.selectbox("Practice *", options=list(practice_options.keys()))
                                practice_id = practice_options[selected_practice]
                            else:
                                st.warning("No practices found for this client.")
                        
                        source_types = st.text_area(
                            "Source Appointment Type(s) *",
                            placeholder="Enter one or more codes (one per line):\nNPE\nNew Pt\nInitial Consult",
                            help="Enter appointment type codes from source system. One per line for bulk entry.",
                            height=120
                        )
                    
                    with col2:
                        standardized_category = st.selectbox(
                            "Standardized Category *",
                            ["New Patient", "Recall", "Follow-Up", "Emergency", "Consultation", "Treatment", "Other"],
                            help="What should this appointment type be categorized as?"
                        )
                        
                        col2a, col2b = st.columns(2)
                        with col2a:
                            start_date = st.date_input(
                                "Start Date *",
                                value=date(2025, 1, 1),
                                help="When does this mapping become effective?"
                            )
                        with col2b:
                            has_end_date = st.checkbox("Set End Date")
                            end_date = None
                            if has_end_date:
                                end_date = st.date_input(
                                    "End Date",
                                    help="When does this mapping expire? Leave unchecked for indefinite."
                                )
                        
                        notes = st.text_area(
                            "Notes (Optional)",
                            placeholder="Additional context about this mapping...",
                            height=100
                        )
                    
                    submitted = st.form_submit_button("Add Mapping(s)", type="primary")
                    
                    if submitted:
                        if source_types and standardized_category:
                            try:
                                # Split by newlines and clean up
                                source_type_list = [s.strip() for s in source_types.split('\n') if s.strip()]
                                
                                if not source_type_list:
                                    st.error("❌ Please enter at least one source appointment type")
                                else:
                                    success_count = 0
                                    failed_types = []
                                    
                                    scope_text = f"{selected_practice}" if practice_id else "all practices"
                                    
                                    for source_type in source_type_list:
                                        try:
                                            mapping_data = {
                                                "client_id": client_id,
                                                "practice_id": practice_id,
                                                "source_appointment_type": source_type,
                                                "standardized_category": standardized_category,
                                                "start_date": start_date,
                                                "end_date": end_date if has_end_date else None,
                                                "notes": notes if notes else None
                                            }
                                            
                                            add_appointment_type_mapping(mapping_data)
                                            success_count += 1
                                        except Exception as e:
                                            failed_types.append(f"{source_type}: {str(e)}")
                                    
                                    # Show results
                                    if success_count > 0:
                                        if success_count == 1:
                                            st.success(f"✅ Mapping '{source_type_list[0]}' → '{standardized_category}' added for {selected_client} ({scope_text})!")
                                        else:
                                            st.success(f"✅ {success_count} mappings added for {selected_client} ({scope_text}) → '{standardized_category}'")
                                    
                                    if failed_types:
                                        st.error(f"❌ {len(failed_types)} failed:")
                                        for failure in failed_types:
                                            st.caption(f"• {failure}")
                                    
                                    if success_count > 0:
                                        time.sleep(1.5)
                                        st.rerun()
                                
                            except Exception as e:
                                st.error(f"❌ Error: {str(e)}")
                        else:
                            st.error("❌ Please fill in all required fields marked with *")
    
    with tab2:
        st.subheader("View & Manage Existing Data")
        
        view_type = st.radio(
            "What do you want to view?",
            ["All Data (Hierarchy)", "Clients Only", "Practices Only", "Providers Only", "Appointment Type Mappings"],
            horizontal=True
        )
        
        if view_type == "All Data (Hierarchy)":
            if not clients_df.empty:
                # Get hierarchical view
                try:
                    engine = get_db_connection()
                    query = """
                    SELECT 
                        c.name as client_name,
                        c.slug as client_slug,
                        c.status as client_status,
                        p.name as practice_name,
                        p.practice_type_specific,
                        pr.name as provider_name,
                        pr.provider_type
                    FROM master.clients c
                    LEFT JOIN master.practices p ON c.id = p.client_id
                    LEFT JOIN master.providers pr ON p.id = pr.practice_id
                    ORDER BY c.name, p.name, pr.name
                    """
                    hierarchy_df = pd.read_sql(query, engine)
                    
                    # Show as expandable sections by client
                    for client_name in hierarchy_df['client_name'].unique():
                        client_data = hierarchy_df[hierarchy_df['client_name'] == client_name]
                        client_practices = client_data['practice_name'].dropna().unique()
                        total_providers = len(client_data['provider_name'].dropna())
                        
                        with st.expander(f"**{client_name}** ({len(client_practices)} practices, {total_providers} providers)", expanded=False):
                            if len(client_practices) > 0:
                                for practice_name in client_practices:
                                    if pd.notna(practice_name):
                                        practice_data = client_data[client_data['practice_name'] == practice_name]
                                        providers_in_practice = practice_data['provider_name'].dropna()
                                        
                                        st.markdown(f"**📍 {practice_name}**")
                                        if practice_data.iloc[0]['practice_type_specific']:
                                            st.caption(f"Type: {practice_data.iloc[0]['practice_type_specific']}")
                                        
                                        if len(providers_in_practice) > 0:
                                            for provider in providers_in_practice:
                                                provider_row = practice_data[practice_data['provider_name'] == provider].iloc[0]
                                                provider_type = provider_row['provider_type']
                                                if provider_type:
                                                    st.markdown(f"  • **{provider}** ({provider_type})")
                                                else:
                                                    st.markdown(f"  • **{provider}**")
                                        else:
                                            st.markdown("  • *No providers assigned*")
                                        st.markdown("")
                            else:
                                st.markdown("*No practices configured for this client*")
                    
                except Exception as e:
                    st.error(f"Error loading hierarchy: {e}")
            else:
                st.info("No data to display. Add some entities first!")
        
        elif view_type == "Clients Only":
            if not clients_df.empty:
                st.dataframe(
                    clients_df[['name', 'slug', 'status', 'created_at']],
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.info("No clients found.")
        
        elif view_type == "Practices Only":
            if not practices_df.empty:
                display_practices = practices_df[['client_name', 'practice_name', 'practice_type_specific', 'owner_name', 'is_active']]
                st.dataframe(display_practices, use_container_width=True, hide_index=True)
            else:
                st.info("No practices found.")
        
        elif view_type == "Providers Only":
            if not providers_df.empty:
                display_providers = providers_df[['client_name', 'practice_name', 'provider_name', 'provider_type', 'is_active']]
                st.dataframe(display_providers, use_container_width=True, hide_index=True)
            else:
                st.info("No providers found.")
        
        elif view_type == "Appointment Type Mappings":
            st.markdown("#### Appointment Type Mappings")
            
            # Filter options
            col1, col2 = st.columns([3, 1])
            with col1:
                if not clients_df.empty:
                    client_filter = st.selectbox(
                        "Filter by Client",
                        ["All Clients"] + list(clients_df['name'].tolist())
                    )
                    filter_client_id = None
                    if client_filter != "All Clients":
                        filter_client_id = clients_df[clients_df['name'] == client_filter].iloc[0]['id']
                else:
                    filter_client_id = None
            
            with col2:
                include_inactive = st.checkbox("Include Inactive", value=False)
            
            # Load mappings
            try:
                mappings_df = get_appointment_type_mappings(filter_client_id, include_inactive)
                
                if not mappings_df.empty:
                    # Display statistics
                    stat_col1, stat_col2, stat_col3 = st.columns(3)
                    with stat_col1:
                        st.metric("Total Mappings", len(mappings_df))
                    with stat_col2:
                        active_count = len(mappings_df[mappings_df['status'] == 'Active'])
                        st.metric("Active Mappings", active_count)
                    with stat_col3:
                        unique_categories = mappings_df['standardized_category'].nunique()
                        st.metric("Categories", unique_categories)
                    
                    st.markdown("---")
                    
                    # Display by category
                    for category in mappings_df['standardized_category'].unique():
                        category_mappings = mappings_df[mappings_df['standardized_category'] == category]
                        
                        with st.expander(f"**{category}** ({len(category_mappings)} mappings)", expanded=True):
                            display_cols = [
                                'client_name', 'practice_name', 'source_appointment_type', 
                                'start_date', 'end_date', 'status', 'notes'
                            ]
                            display_data = category_mappings[display_cols].copy()
                            display_data.columns = [
                                'Client', 'Practice', 'Source Type', 
                                'Start Date', 'End Date', 'Status', 'Notes'
                            ]
                            
                            # Replace None with "All Practices"
                            display_data['Practice'] = display_data['Practice'].fillna('All Practices')
                            
                            st.dataframe(display_data, use_container_width=True, hide_index=True)
                else:
                    st.info("No appointment type mappings found. Add some in the 'Add Entities' tab!")
            
            except Exception as e:
                st.error(f"Error loading appointment type mappings: {str(e)}")
                st.info("💡 Tip: Make sure you've created the appointment_type_mappings table. Check the SQL file in the project.")
    
    with tab3:
        st.subheader("Bulk Import")
        st.markdown("*Coming soon: Import multiple entities from CSV or Excel files.*")
        st.info("This feature will allow you to upload spreadsheets with client, practice, and provider data for bulk creation.")
    
    with tab4:
        st.subheader("Data Relationships")
        
        if not clients_df.empty:
            # Summary statistics
            st.markdown("#### Relationship Summary")
            
            summary_col1, summary_col2, summary_col3 = st.columns(3)
            
            with summary_col1:
                practices_per_client = practices_df.groupby('client_id').size().mean() if not practices_df.empty else 0
                st.metric("Avg Practices per Client", f"{practices_per_client:.1f}")
            
            with summary_col2:
                providers_per_practice = providers_df.groupby('practice_id').size().mean() if not providers_df.empty else 0  
                st.metric("Avg Providers per Practice", f"{providers_per_practice:.1f}")
            
            with summary_col3:
                total_relationships = len(practices_df) + len(providers_df)
                st.metric("Total Relationships", total_relationships)
            
            # Show detailed breakdown
            if not practices_df.empty:
                st.markdown("#### Detailed Breakdown")
                
                breakdown_data = []
                for _, client in clients_df.iterrows():
                    client_practices = practices_df[practices_df['client_id'] == client['id']]
                    total_providers = 0
                    for _, practice in client_practices.iterrows():
                        practice_providers = providers_df[providers_df['practice_id'] == practice['id']]
                        total_providers += len(practice_providers)
                    
                    breakdown_data.append({
                        'Client': client['name'],
                        'Practices': len(client_practices),
                        'Providers': total_providers,
                        'Status': client['status']
                    })
                
                if breakdown_data:
                    breakdown_df = pd.DataFrame(breakdown_data)
                    st.dataframe(breakdown_df, use_container_width=True, hide_index=True)
        else:
            st.info("No data available for relationship analysis.")
    
    # Quick actions footer
    st.markdown("---")
    st.markdown("#### Next Steps")
    st.info("Use the sidebar to navigate to **ETL Pipeline** to process your data, or **Data Overview** to explore tables.")

if __name__ == "__main__":
    main()