"""
Caching functions for database queries to improve application performance.
Implements automatic cache refresh and manual cache invalidation.
"""

import pandas as pd
import time
from datetime import datetime, timedelta
from typing import Dict, Optional, Any
import streamlit as st
from sqlalchemy import text
from connect_db import get_engine

# Cache storage and metadata
CACHE_STORE: Dict[str, Any] = {}
CACHE_TIMESTAMPS: Dict[str, datetime] = {}
CACHE_TTL_MINUTES = 10  # Cache expires after 10 minutes

def get_cache_key(table_name: str, schema: str = None, filters: str = None) -> str:
    """Generate a unique cache key"""
    key = f"{schema}.{table_name}" if schema else table_name
    if filters:
        key += f"__{filters}"
    return key

def is_cache_valid(cache_key: str) -> bool:
    """Check if cache entry is still valid"""
    if cache_key not in CACHE_TIMESTAMPS:
        return False
    
    cache_time = CACHE_TIMESTAMPS[cache_key]
    expiry_time = cache_time + timedelta(minutes=CACHE_TTL_MINUTES)
    return datetime.now() < expiry_time

def get_cached_data(cache_key: str) -> Optional[pd.DataFrame]:
    """Get data from cache if valid"""
    if cache_key in CACHE_STORE and is_cache_valid(cache_key):
        return CACHE_STORE[cache_key].copy()  # Return copy to avoid mutations
    return None

def set_cached_data(cache_key: str, data: pd.DataFrame) -> None:
    """Store data in cache with timestamp"""
    CACHE_STORE[cache_key] = data.copy()
    CACHE_TIMESTAMPS[cache_key] = datetime.now()

def invalidate_cache(pattern: str = None) -> None:
    """Invalidate cache entries. If pattern provided, only invalidate matching keys"""
    global CACHE_STORE, CACHE_TIMESTAMPS
    
    if pattern:
        keys_to_remove = [key for key in CACHE_STORE.keys() if pattern in key]
        for key in keys_to_remove:
            CACHE_STORE.pop(key, None)
            CACHE_TIMESTAMPS.pop(key, None)
    else:
        CACHE_STORE.clear()
        CACHE_TIMESTAMPS.clear()

# =============================================================================
# Master Data Caching Functions
# =============================================================================

@st.cache_data(ttl=600)  # 10 minutes TTL
def get_clients_cached() -> pd.DataFrame:
    """Get all clients with caching"""
    cache_key = get_cache_key("clients", "master")
    
    # Check cache first
    cached_data = get_cached_data(cache_key)
    if cached_data is not None:
        return cached_data
    
    # Fetch from database
    engine = get_engine()
    query = "SELECT id, slug, name, status, created_at, updated_at FROM master.clients ORDER BY name"
    data = pd.read_sql(query, engine)
    
    # Store in cache
    set_cached_data(cache_key, data)
    return data

@st.cache_data(ttl=600)
def get_practices_cached(client_id: str = None) -> pd.DataFrame:
    """Get practices with caching"""
    cache_key = get_cache_key("practices", "master", f"client_{client_id}" if client_id else None)
    
    # Check cache first
    cached_data = get_cached_data(cache_key)
    if cached_data is not None:
        return cached_data
    
    # Fetch from database
    engine = get_engine()
    if client_id:
        query = """
        SELECT id, client_id, name, practice_type_specific, owner_name, is_active, created_at 
        FROM master.practices 
        WHERE client_id = %(client_id)s 
        ORDER BY name
        """
        data = pd.read_sql(query, engine, params={"client_id": client_id})
    else:
        query = """
        SELECT p.id, p.client_id, p.name as practice_name, c.name as client_name, 
               p.practice_type_specific, p.owner_name, p.is_active, p.created_at
        FROM master.practices p
        JOIN master.clients c ON p.client_id = c.id
        ORDER BY c.name, p.name
        """
        data = pd.read_sql(query, engine)
    
    # Store in cache
    set_cached_data(cache_key, data)
    return data

@st.cache_data(ttl=600)
def get_providers_cached(practice_id: str = None) -> pd.DataFrame:
    """Get providers with caching"""
    cache_key = get_cache_key("providers", "master", f"practice_{practice_id}" if practice_id else None)
    
    # Check cache first
    cached_data = get_cached_data(cache_key)
    if cached_data is not None:
        return cached_data
    
    # Fetch from database
    engine = get_engine()
    if practice_id:
        query = """
        SELECT id, practice_id, name, provider_type, is_active, created_at 
        FROM master.providers 
        WHERE practice_id = %(practice_id)s 
        ORDER BY name
        """
        data = pd.read_sql(query, engine, params={"practice_id": practice_id})
    else:
        query = """
        SELECT pr.id, pr.practice_id, pr.name as provider_name, p.name as practice_name, 
               c.name as client_name, pr.provider_type, pr.is_active, pr.created_at
        FROM master.providers pr
        JOIN master.practices p ON pr.practice_id = p.id
        JOIN master.clients c ON p.client_id = c.id
        ORDER BY c.name, p.name, pr.name
        """
        data = pd.read_sql(query, engine)
    
    # Store in cache
    set_cached_data(cache_key, data)
    return data

# =============================================================================
# Bronze Layer Caching Functions
# =============================================================================

@st.cache_data(ttl=600)
def get_bronze_data_status_cached(client_slug: str = None) -> Dict[str, Any]:
    """Get bronze layer data status with caching"""
    cache_key = get_cache_key("bronze_status", None, client_slug)
    
    # Check cache first
    cached_data = get_cached_data(cache_key)
    if cached_data is not None:
        return cached_data.to_dict('records')[0] if not cached_data.empty else {}
    
    # Fetch from database
    engine = get_engine()
    
    status = {}
    
    try:
        # Get detailed appointments data
        if client_slug:
            appointments_query = f"""
            SELECT 
                COUNT(*) as total_appointments,
                COUNT(DISTINCT patient_id_guid) as unique_patients,
                MIN(appointment_date) as earliest_date,
                MAX(appointment_date) as latest_date,
                COUNT(DISTINCT appointment_type_description) as appointment_types,
                MAX(created_at) as last_updated
            FROM bronze_ops.appointments_raw_wso
            WHERE client_tag = '{client_slug}'
            """
        else:
            appointments_query = """
            SELECT 
                COUNT(*) as total_appointments,
                COUNT(DISTINCT patient_id_guid) as unique_patients,
                MIN(appointment_date) as earliest_date,
                MAX(appointment_date) as latest_date,
                COUNT(DISTINCT appointment_type_description) as appointment_types,
                MAX(created_at) as last_updated
            FROM bronze_ops.appointments_raw_wso
            """
        
        appointments_df = pd.read_sql(appointments_query, engine)
        status['appointments'] = appointments_df.iloc[0].to_dict() if not appointments_df.empty else {
            'total_appointments': 0, 'unique_patients': 0, 'earliest_date': None,
            'latest_date': None, 'appointment_types': 0, 'last_updated': None
        }
        
        # Get detailed referrals data
        if client_slug:
            referrals_query = f"""
            SELECT 
                COUNT(*) as total_referrals,
                COUNT(DISTINCT patient_id_guid) as unique_referred_patients,
                COUNT(DISTINCT referred_in_by_type_description) as referral_types,
                MAX(created_at) as last_updated
            FROM bronze_ops.referrals_raw_wso
            WHERE client_tag = '{client_slug}'
            """
        else:
            referrals_query = """
            SELECT 
                COUNT(*) as total_referrals,
                COUNT(DISTINCT patient_id_guid) as unique_referred_patients,
                COUNT(DISTINCT referred_in_by_type_description) as referral_types,
                MAX(created_at) as last_updated
            FROM bronze_ops.referrals_raw_wso
            """
        
        referrals_df = pd.read_sql(referrals_query, engine)
        status['referrals'] = referrals_df.iloc[0].to_dict() if not referrals_df.empty else {
            'total_referrals': 0, 'unique_referred_patients': 0, 'referral_types': 0, 'last_updated': None
        }
        
    except Exception as e:
        # If tables don't exist, return empty status
        status = {
            'appointments': {'total_appointments': 0, 'unique_patients': 0, 'earliest_date': None,
                           'latest_date': None, 'appointment_types': 0, 'last_updated': None},
            'referrals': {'total_referrals': 0, 'unique_referred_patients': 0, 'referral_types': 0, 'last_updated': None}
        }
    
    # Convert to DataFrame for caching
    status_df = pd.DataFrame([status])
    set_cached_data(cache_key, status_df)
    
    return status

# =============================================================================
# Silver/Gold Layer Caching Functions
# =============================================================================

@st.cache_data(ttl=600)
def get_silver_gold_status_cached() -> Dict[str, Any]:
    """Get silver and gold layer status with caching"""
    cache_key = get_cache_key("silver_gold_status")
    
    # Check cache first
    cached_data = get_cached_data(cache_key)
    if cached_data is not None:
        return cached_data.to_dict('records')[0] if not cached_data.empty else {}
    
    # Fetch from database
    engine = get_engine()
    
    status = {
        'silver': {},
        'gold': {}
    }
    
    # Silver layer tables
    silver_tables = ['referrals', 'fact_patient_treatments', 'fact_referrals']
    for table in silver_tables:
        try:
            query = f"""
            SELECT COUNT(*) as count, MAX(created_at) as last_updated
            FROM silver_ops.{table}
            """
            result = pd.read_sql(query, engine)
            status['silver'][table] = {
                'count': result.iloc[0]['count'] if not result.empty else 0,
                'last_updated': result.iloc[0]['last_updated'] if not result.empty else None
            }
        except Exception as e:
            status['silver'][table] = {'count': 0, 'last_updated': None, 'error': str(e)}
    
    # Gold layer tables
    gold_tables = ['referrals_monthly_summary', 'referrals_monthly_breakdown']
    for table in gold_tables:
        try:
            query = f"""
            SELECT COUNT(*) as count, MAX(created_at) as last_updated
            FROM gold_ops.{table}
            """
            result = pd.read_sql(query, engine)
            status['gold'][table] = {
                'count': result.iloc[0]['count'] if not result.empty else 0,
                'last_updated': result.iloc[0]['last_updated'] if not result.empty else None
            }
        except Exception as e:
            status['gold'][table] = {'count': 0, 'last_updated': None, 'error': str(e)}
    
    # Convert to DataFrame for caching
    status_df = pd.DataFrame([status])
    set_cached_data(cache_key, status_df)
    
    return status

# =============================================================================
# Cache Management Functions
# =============================================================================

def refresh_all_caches():
    """Force refresh of all cached data"""
    st.cache_data.clear()
    invalidate_cache()
    
    # Pre-load critical data
    try:
        get_clients_cached()
        get_practices_cached()
        get_providers_cached()
        get_bronze_data_status_cached()
        get_silver_gold_status_cached()
        return True
    except Exception as e:
        st.error(f"Error refreshing caches: {e}")
        return False

def refresh_master_data_cache():
    """Refresh only master data caches"""
    # Clear Streamlit cache for master data
    get_clients_cached.clear()
    get_practices_cached.clear()
    get_providers_cached.clear()
    
    # Clear internal cache
    invalidate_cache("master")
    
    # Pre-load fresh data
    get_clients_cached()
    get_practices_cached()
    get_providers_cached()

def refresh_etl_data_cache():
    """Refresh ETL-related caches after pipeline runs"""
    get_bronze_data_status_cached.clear()
    get_silver_gold_status_cached.clear()
    
    invalidate_cache("bronze")
    invalidate_cache("silver")
    invalidate_cache("gold")
    
    # Pre-load fresh data
    get_bronze_data_status_cached()
    get_silver_gold_status_cached()

def get_cache_info() -> Dict[str, Any]:
    """Get information about current cache status"""
    return {
        'total_entries': len(CACHE_STORE),
        'cache_keys': list(CACHE_STORE.keys()),
        'timestamps': {k: v.isoformat() for k, v in CACHE_TIMESTAMPS.items()},
        'ttl_minutes': CACHE_TTL_MINUTES
    }

# =============================================================================
# Auto-refresh Setup
# =============================================================================

def setup_auto_refresh():
    """Set up automatic cache refresh in Streamlit session state"""
    if 'last_auto_refresh' not in st.session_state:
        st.session_state.last_auto_refresh = datetime.now()
    
    # Check if it's time for auto refresh
    if datetime.now() - st.session_state.last_auto_refresh > timedelta(minutes=CACHE_TTL_MINUTES):
        refresh_all_caches()
        st.session_state.last_auto_refresh = datetime.now()
        return True
    return False

def setup_sidebar_cache_controls():
    """Set up cache control widgets in the sidebar for all pages"""
    with st.sidebar:
        st.markdown("---")
        st.subheader("📊 Cache Management")
        
        # Cache status
        cache_info = get_cache_info()
        st.metric("Cached Entries", cache_info['total_entries'])
        
        # Last refresh time
        if 'last_auto_refresh' in st.session_state:
            last_refresh = st.session_state.last_auto_refresh
            time_since = datetime.now() - last_refresh
            minutes_since = int(time_since.total_seconds() / 60)
            st.caption(f"Last refresh: {minutes_since} min ago")
        
        # Refresh controls
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🔄 Refresh All", help="Clear and refresh all cached data", use_container_width=True):
                if refresh_all_caches():
                    st.success("✅ Refreshed!")
                    st.rerun()
                else:
                    st.error("❌ Error!")
        
        with col2:
            if st.button("🏢 Master Data", help="Refresh master data only", use_container_width=True):
                refresh_master_data_cache()
                st.success("✅ Master data refreshed!")
                st.rerun()
        
        # ETL data refresh
        if st.button("🔄 ETL Data", help="Refresh ETL pipeline data", use_container_width=True):
            refresh_etl_data_cache()
            st.success("✅ ETL data refreshed!")
            st.rerun()
        
        # Cache info expander
        with st.expander("🔍 Cache Details"):
            st.write(f"**TTL:** {CACHE_TTL_MINUTES} minutes")
            if cache_info['cache_keys']:
                st.write("**Active caches:**")
                for key in cache_info['cache_keys']:
                    st.caption(f"• {key}")
            else:
                st.caption("No active caches")