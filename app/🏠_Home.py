import streamlit as st
import sys
from pathlib import Path

# Add utils to path
sys.path.append(str(Path(__file__).parent.parent / 'utils'))
from cache_func import setup_auto_refresh, setup_sidebar_cache_controls

st.set_page_config(
    page_title="Data Platform", 
    layout="wide",
    page_icon="🏗️",
    initial_sidebar_state="expanded"
)

def main():
    """Main platform page"""
    
    st.title("🏗️ Data Platform")
    st.markdown("### Comprehensive data management and analytics platform")
    
    # Auto-refresh setup
    setup_auto_refresh()
    
    # Setup sidebar cache controls
    setup_sidebar_cache_controls()
    
    # Architecture Overview
    st.markdown("## 🎯 Three-Layer Data Architecture")
    st.markdown("""
    Our data platform follows a **medallion architecture** that processes data through three distinct layers, 
    each serving a specific purpose in the data pipeline.
    """)
    
    # Layer explanations
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        **🥉 Bronze Layer - Raw Data**
        
        - Store raw, unprocessed data
        - Preserves complete data history
        - Organized by business domain
        - Source of truth
        
        Examples: `bronze_fin.revenue`, `bronze_ops.appointments`
        """)
    
    with col2:
        st.markdown("""
        **🥈 Silver Layer - Cleaned Data**
        
        - Clean, validate, and standardize
        - Data quality rules applied
        - Duplicate removal
        - Business logic implementation
        
        Examples: `silver.patients_cleaned`, `silver.revenue_standardized`
        """)
        
    with col3:
        st.markdown("""
        **🥇 Gold Layer - Analytics Ready**
        
        - Business-ready data for reporting
        - Aggregated metrics and KPIs
        - Optimized for query performance
        - Dashboard-ready
        
        Examples: `gold.monthly_revenue`, `gold.patient_satisfaction`
        """)
    
    st.markdown("---")
    
    # Navigation section
    st.markdown("## 🗂️ Platform Sections")
    
    nav_col1, nav_col2 = st.columns(2)
    
    with nav_col1:
        st.markdown("""
        **📊 Data Overview**
        - Explore database schemas and tables
        - View data with intelligent descriptions
        - Analyze table structures and relationships
        """)
    
    with nav_col2:
        st.markdown("""
        **🏢 Master Data**
        - Manage clients, practices, and providers
        - Add new entities with form validation
        - View and edit existing master data
        """)
    
    # Benefits
    st.markdown("### ✨ Why This Architecture?")
    
    benefit_col1, benefit_col2, benefit_col3 = st.columns(3)
    
    with benefit_col1:
        st.markdown("""
        **🔄 Flexibility**
        - Easy to reprocess data
        - Multiple views of same data
        - Add new transformations anytime
        """)
    
    with benefit_col2:
        st.markdown("""
        **📊 Quality**
        - Clear data lineage
        - Validation at each step
        - Easier debugging
        """)
    
    with benefit_col3:
        st.markdown("""
        **⚡ Performance**
        - Optimized for different uses
        - Reduced processing overhead
        - Faster analytics queries
        """)
    
    # Footer
    st.markdown("---")
    st.info("💡 Use the sidebar to navigate between **Master Data**, **ETL Pipeline**, and **Data Overview** sections.")
    
    # Quick navigation
    st.subheader("🚀 Quick Start")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🏢 Manage Master Data", use_container_width=True):
            st.switch_page("pages/1_🏢_Master_Data.py")
    
    with col2:
        if st.button("🔄 Run ETL Pipeline", use_container_width=True):
            st.switch_page("pages/2_🔄_ETL_Pipeline.py")
    
    with col3:
        if st.button("📊 View Data Overview", use_container_width=True):
            st.switch_page("pages/3_📊_Data_Overview.py")

if __name__ == "__main__":
    main()