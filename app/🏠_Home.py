import streamlit as st
import sys
from pathlib import Path
import pandas as pd
from sqlalchemy import text

# Add utils to path
sys.path.append(str(Path(__file__).parent.parent / 'utils'))
from cache_func import setup_auto_refresh, setup_sidebar_cache_controls
from connect_db import get_engine

st.set_page_config(
    page_title="Data Platform",
    layout="wide",
    page_icon="🏗️",
    initial_sidebar_state="expanded"
)

def get_table_structure():
    """Get actual table structure from the database"""
    try:
        engine = get_engine()

        query = text("""
        SELECT
            t.table_schema,
            t.table_name,
            pg_size_pretty(pg_total_relation_size('"'||t.table_schema||'"."'||t.table_name||'"')) as size,
            COALESCE(s.n_live_tup, 0) as row_count,
            array_agg(col.column_name ORDER BY col.ordinal_position) as columns
        FROM information_schema.tables t
        LEFT JOIN pg_stat_user_tables s ON t.table_schema = s.schemaname AND t.table_name = s.relname
        LEFT JOIN information_schema.columns col ON t.table_schema = col.table_schema
            AND t.table_name = col.table_name
        WHERE t.table_schema IN ('master', 'bronze_fin', 'bronze_ops', 'silver_ops', 'gold_ops')
        GROUP BY t.table_schema, t.table_name, s.n_live_tup
        ORDER BY
            CASE t.table_schema
                WHEN 'master' THEN 1
                WHEN 'bronze_fin' THEN 2
                WHEN 'bronze_ops' THEN 3
                WHEN 'silver_ops' THEN 4
                WHEN 'gold_ops' THEN 5
            END,
            t.table_name;
        """)

        df = pd.read_sql(query, engine)
        return df

    except Exception as e:
        st.error(f"Could not load table structure: {e}")
        return pd.DataFrame()

def get_client_table_details(client_slug, bronze_suffix):
    """Get detailed table status for a specific client"""
    try:
        engine = get_engine()

        bronze_ops_tables = ['appointments_raw', 'patients_raw', 'referrals_raw', 'treatments_raw']
        bronze_fin_tables = ['aging_accounts_raw', 'ledger_transactions_raw']
        silver_tables = ['fact_new_patient_intake']
        gold_tables = ['referrals_monthly_summary', 'referrals_monthly_breakdown']

        result = {'bronze': [], 'silver': [], 'gold': []}

        for table_base in bronze_ops_tables:
            table_name = f"{table_base}_{bronze_suffix}"
            exists = pd.read_sql(text(f"""
                SELECT EXISTS(
                    SELECT 1 FROM pg_tables
                    WHERE schemaname = 'bronze_ops' AND tablename = '{table_name}'
                ) as exists
            """), engine).iloc[0, 0]
            result['bronze'].append({'name': table_base, 'exists': exists})

        for table_base in bronze_fin_tables:
            table_name = f"{table_base}_{bronze_suffix}"
            exists = pd.read_sql(text(f"""
                SELECT EXISTS(
                    SELECT 1 FROM pg_tables
                    WHERE schemaname = 'bronze_fin' AND tablename = '{table_name}'
                ) as exists
            """), engine).iloc[0, 0]
            result['bronze'].append({'name': table_base, 'exists': exists})

        client_id = pd.read_sql(text(f"SELECT id FROM master.clients WHERE slug = '{client_slug}'"), engine)
        if not client_id.empty:
            cid = client_id.iloc[0, 0]
            for table_name in silver_tables:
                has_data = pd.read_sql(text(f"""
                    SELECT EXISTS(
                        SELECT 1 FROM silver_ops.{table_name} WHERE client_id = '{cid}' LIMIT 1
                    ) as exists
                """), engine).iloc[0, 0]
                result['silver'].append({'name': table_name, 'exists': has_data})

            for table_name in gold_tables:
                has_data = pd.read_sql(text(f"""
                    SELECT EXISTS(
                        SELECT 1 FROM gold_ops.{table_name} WHERE client_id = '{cid}' LIMIT 1
                    ) as exists
                """), engine).iloc[0, 0]
                result['gold'].append({'name': table_name, 'exists': has_data})

        return result

    except Exception as e:
        st.error(f"Error getting table details: {e}")
        return {'bronze': [], 'silver': [], 'gold': []}

def get_client_health_status():
    """Get data health status for each client across all layers"""
    try:
        engine = get_engine()
        
        # Get all clients
        clients_df = pd.read_sql(text("""
            SELECT
                id::text as id,
                name as client_name,
                slug as client_slug,
                created_at as client_created,
                CASE
                    WHEN slug LIKE '%wall_street%' OR slug LIKE '%wso%' THEN 'wso'
                    ELSE slug
                END as bronze_suffix
            FROM master.clients
            ORDER BY name
        """), engine)
        
        results = []
        total_clients = len(clients_df)
        progress_bar = st.progress(0)
        status_text = st.empty()

        for idx, client in enumerate(clients_df.iterrows()):
            _, client = client
            status_text.text(f"Loading health status for {client['client_name']}...")
            progress_bar.progress((idx + 1) / total_clients)
            client_id = client['id']
            bronze_suffix = client['bronze_suffix']

            # Count bronze tables
            bronze_tables = 0
            bronze_table_names = [
                ('bronze_ops', f"appointments_raw_{bronze_suffix}"),
                ('bronze_ops', f"patients_raw_{bronze_suffix}"),
                ('bronze_ops', f"referrals_raw_{bronze_suffix}"),
                ('bronze_ops', f"treatments_raw_{bronze_suffix}"),
                ('bronze_fin', f"aging_accounts_raw_{bronze_suffix}"),
                ('bronze_fin', f"ledger_transactions_raw_{bronze_suffix}")
            ]

            for schema, table_name in bronze_table_names:
                check_query = text(f"""
                    SELECT EXISTS(
                        SELECT 1 FROM pg_tables
                        WHERE schemaname = '{schema}' AND tablename = '{table_name}'
                    ) as exists
                """)
                exists = pd.read_sql(check_query, engine).iloc[0, 0]
                if exists:
                    bronze_tables += 1

            # Check silver tables
            silver_query = text(f"""
                SELECT COUNT(DISTINCT 'fact_new_patient_intake') as cnt
                FROM silver_ops.fact_new_patient_intake
                WHERE client_id::text = '{client_id}'
            """)
            silver_count = pd.read_sql(silver_query, engine)['cnt'].iloc[0]
            silver_tables = 1 if silver_count > 0 else 0

            silver_update_query = text(f"""
                SELECT MAX(created_at) as max_date
                FROM silver_ops.fact_new_patient_intake
                WHERE client_id::text = '{client_id}'
            """)
            silver_update = pd.read_sql(silver_update_query, engine)['max_date'].iloc[0]

            # Check gold tables
            gold_query_1 = text(f"""
                SELECT COUNT(*) as cnt
                FROM gold_ops.referrals_monthly_summary
                WHERE client_id::text = '{client_id}'
            """)
            gold_count_1 = pd.read_sql(gold_query_1, engine)['cnt'].iloc[0]

            gold_query_2 = text(f"""
                SELECT COUNT(*) as cnt
                FROM gold_ops.referrals_monthly_breakdown
                WHERE client_id::text = '{client_id}'
            """)
            gold_count_2 = pd.read_sql(gold_query_2, engine)['cnt'].iloc[0]

            gold_tables = (1 if gold_count_1 > 0 else 0) + (1 if gold_count_2 > 0 else 0)

            gold_update_query_1 = text(f"""
                SELECT MAX(created_at) as max_date
                FROM gold_ops.referrals_monthly_summary
                WHERE client_id::text = '{client_id}'
            """)
            gold_update_1 = pd.read_sql(gold_update_query_1, engine)['max_date'].iloc[0]

            gold_update_query_2 = text(f"""
                SELECT MAX(created_at) as max_date
                FROM gold_ops.referrals_monthly_breakdown
                WHERE client_id::text = '{client_id}'
            """)
            gold_update_2 = pd.read_sql(gold_update_query_2, engine)['max_date'].iloc[0]

            # Determine last update
            last_update = client['client_created']
            if pd.notna(silver_update):
                last_update = max(last_update, silver_update)
            if pd.notna(gold_update_1):
                last_update = max(last_update, gold_update_1)
            if pd.notna(gold_update_2):
                last_update = max(last_update, gold_update_2)

            results.append({
                'client_name': client['client_name'],
                'client_slug': client['client_slug'],
                'bronze_suffix': bronze_suffix,
                'bronze_tables': bronze_tables,
                'silver_tables': silver_tables,
                'gold_tables': gold_tables,
                'last_update': last_update
            })

        progress_bar.empty()
        status_text.empty()
        return pd.DataFrame(results)

    except Exception as e:
        st.error(f"Could not load client health status: {e}")
        import traceback
        st.code(traceback.format_exc())
        return pd.DataFrame()

def main():
    """Main platform page"""

    st.title("Data Platform")
    st.markdown("Healthcare data management with three-layer architecture")

    # Auto-refresh setup
    setup_auto_refresh()

    # Setup sidebar cache controls
    setup_sidebar_cache_controls()

    st.markdown("---")

    # Data Flow Diagram
    st.subheader("Data Flow Architecture")

    flow1, arrow1, flow2, arrow2, flow3, arrow3, flow4 = st.columns([4, 0.5, 4, 0.5, 4, 0.5, 4])

    with flow1:
        st.markdown("""
        <div style="background-color: #f9f9f9; padding: 20px; border-radius: 10px; text-align: center; border: 2px solid #333; height: 140px; display: flex; flex-direction: column; justify-content: center;">
            <div style="font-size: 2em;">📤</div>
            <div style="margin: 5px 0; font-size: 1.2em; font-weight: 600;">Source</div>
            <div style="font-size: 0.85em; color: #666;">Raw input</div>
        </div>
        """, unsafe_allow_html=True)

    with arrow1:
        st.markdown("<div style='text-align: center; line-height: 140px; font-size: 2em;'>→</div>", unsafe_allow_html=True)

    with flow2:
        st.markdown("""
        <div style="background-color: #fff4e6; padding: 20px; border-radius: 10px; text-align: center; border: 3px solid #ff9800; height: 140px; display: flex; flex-direction: column; justify-content: center;">
            <div style="font-size: 2em;">📋</div>
            <div style="margin: 5px 0; font-size: 1.2em; font-weight: 600;">Bronze</div>
            <div style="font-size: 0.85em; color: #666;">Raw data</div>
        </div>
        """, unsafe_allow_html=True)

    with arrow2:
        st.markdown("<div style='text-align: center; line-height: 140px; font-size: 2em;'>→</div>", unsafe_allow_html=True)

    with flow3:
        st.markdown("""
        <div style="background-color: #f0f0f0; padding: 20px; border-radius: 10px; text-align: center; border: 3px solid #9e9e9e; height: 140px; display: flex; flex-direction: column; justify-content: center;">
            <div style="font-size: 2em;">⚙️</div>
            <div style="margin: 5px 0; font-size: 1.2em; font-weight: 600;">Silver</div>
            <div style="font-size: 0.85em; color: #666;">Standardized</div>
        </div>
        """, unsafe_allow_html=True)

    with arrow3:
        st.markdown("<div style='text-align: center; line-height: 140px; font-size: 2em;'>→</div>", unsafe_allow_html=True)

    with flow4:
        st.markdown("""
        <div style="background-color: #fff9c4; padding: 20px; border-radius: 10px; text-align: center; border: 3px solid #fbc02d; height: 140px; display: flex; flex-direction: column; justify-content: center;">
            <div style="font-size: 2em;">📊</div>
            <div style="margin: 5px 0; font-size: 1.2em; font-weight: 600;">Gold</div>
            <div style="font-size: 0.85em; color: #666;">Analytics</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("")

    # Current Tables Overview
    st.subheader("Current Data Warehouse")

    with st.spinner("Loading table structure..."):
        table_df = get_table_structure()

    if not table_df.empty:
        col1, col2, col3, col4 = st.columns(4)

        try:
            engine = get_engine()
            client_count_df = pd.read_sql(text("SELECT COUNT(*) as count FROM master.clients"), engine)
            client_count = client_count_df['count'].iloc[0]
        except Exception:
            client_count = 0

        bronze_tables = len(table_df[table_df['table_schema'].isin(['bronze_ops', 'bronze_fin'])])
        silver_tables = len(table_df[table_df['table_schema'] == 'silver_ops'])
        gold_tables = len(table_df[table_df['table_schema'] == 'gold_ops'])

        with col1:
            st.metric("Clients", client_count)
        with col2:
            st.metric("Bronze Tables", bronze_tables)
        with col3:
            st.metric("Silver Tables", silver_tables)
        with col4:
            st.metric("Gold Tables", gold_tables)

        st.markdown("---")

        # Client Health Status
        if client_count > 0:
            st.subheader("Client Data Health")

            client_health = get_client_health_status()

            if not client_health.empty:
                for _, client in client_health.iterrows():
                    has_bronze = client['bronze_tables'] > 0
                    has_silver = client['silver_tables'] > 0
                    has_gold = client['gold_tables'] > 0

                    if has_bronze and has_silver and has_gold:
                        status_icon = "✅"
                        status_color = "#d4edda"
                    elif has_bronze and has_silver:
                        status_icon = "⚠️"
                        status_color = "#fff3cd"
                    elif has_bronze:
                        status_icon = "🔄"
                        status_color = "#e2e3e5"
                    else:
                        status_icon = "❌"
                        status_color = "#f8d7da"

                    with st.container():
                        st.markdown(
                            f"""
                            <div style="background-color: {status_color}; padding: 15px; border-radius: 8px; margin-bottom: 10px;">
                                <div style="display: flex; justify-content: space-between; align-items: center;">
                                    <div>
                                        <span style="font-size: 1.5em;">{status_icon}</span>
                                        <strong style="font-size: 1.1em; margin-left: 10px;">{client['client_name']}</strong>
                                    </div>
                                    <div style="text-align: right; color: #666; font-size: 0.9em;">
                                        Last update: {client['last_update'].strftime('%Y-%m-%d %H:%M') if pd.notna(client['last_update']) else 'Never'}
                                    </div>
                                </div>
                                <div style="margin-top: 10px; font-size: 0.9em; color: #333;">
                                    Bronze: {client['bronze_tables']}/6 tables | Silver: {client['silver_tables']}/1 tables | Gold: {client['gold_tables']}/2 tables
                                </div>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )

                        with st.expander("View table details"):
                            with st.spinner("Loading table details..."):
                                table_details = get_client_table_details(client['client_slug'], client['bronze_suffix'])

                            col_b, col_s, col_g = st.columns(3)

                            with col_b:
                                st.markdown("**Bronze Layer**")
                                for table in table_details['bronze']:
                                    icon = "✅" if table['exists'] else "❌"
                                    st.markdown(f"{icon} {table['name']}")

                            with col_s:
                                st.markdown("**Silver Layer**")
                                for table in table_details['silver']:
                                    icon = "✅" if table['exists'] else "❌"
                                    st.markdown(f"{icon} {table['name']}")

                            with col_g:
                                st.markdown("**Gold Layer**")
                                for table in table_details['gold']:
                                    icon = "✅" if table['exists'] else "❌"
                                    st.markdown(f"{icon} {table['name']}")
            else:
                st.info("No client data available yet")

        st.markdown("---")

        # Layer breakdown with tables
        st.subheader("Data Layers")
        
        schemas = {
            'master': {'name': 'Master', 'icon': '🏛️'},
            'bronze_ops': {'name': 'Bronze Operations', 'icon': '📋'},
            'bronze_fin': {'name': 'Bronze Finance', 'icon': '💰'},
            'silver_ops': {'name': 'Silver', 'icon': '⚙️'},
            'gold_ops': {'name': 'Gold', 'icon': '📊'}
        }
        
        for schema_key, schema_meta in schemas.items():
            if schema_key in table_df['table_schema'].values:
                schema_tables = table_df[table_df['table_schema'] == schema_key]
                total_records = schema_tables['row_count'].sum()
                
                with st.expander(f"{schema_meta['icon']} {schema_meta['name']} — {len(schema_tables)} tables, {total_records:,} records"):
                    for _, row in schema_tables.iterrows():
                        table_name = row['table_name']
                        row_count = row['row_count'] if pd.notna(row['row_count']) else 0
                        st.markdown(f"• **{table_name}** — {row_count:,} rows")

if __name__ == "__main__":
    main()