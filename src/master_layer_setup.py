"""
Master Layer Setup Script

This script creates and manages the master schema tables that form the foundational
reference data for the entire data warehouse architecture.

Usage:
    python master_layer_setup.py --action create_all
    python master_layer_setup.py --action create_table --table clients
    python master_layer_setup.py --action populate_sample --client wso
"""

import sys
import os
from pathlib import Path
from datetime import datetime, date
import pandas as pd
import argparse
from typing import Dict, List, Optional
from sqlalchemy import text

# Add utils to path
sys.path.append(str(Path(__file__).parent.parent / 'utils'))
from connect_db import get_engine

def create_master_schema():
    """Create the master schema if it doesn't exist"""
    engine = get_engine()
    
    with engine.connect() as conn:
        # Create schema
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS master"))
        conn.commit()
        print("✅ Master schema created/verified")

def create_clients_table():
    """
    master.clients - Top-level customer organizations
    Used for tenancy, access control, billing, and grouping multiple practices
    """
    sql = """
    CREATE TABLE IF NOT EXISTS master.clients (
        client_id SERIAL PRIMARY KEY,
        client_name VARCHAR(255) NOT NULL,
        client_tag VARCHAR(50) NOT NULL UNIQUE, -- e.g., 'wso', 'abc_dental'
        client_status VARCHAR(50) DEFAULT 'active',
        billing_entity VARCHAR(255),
        contract_start_date DATE,
        contract_end_date DATE,
        primary_contact_name VARCHAR(255),
        primary_contact_email VARCHAR(255),
        primary_contact_phone VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT valid_client_status CHECK (client_status IN ('active', 'inactive', 'pending'))
    )
    """
    return sql

def create_practices_table():
    """
    master.practices - Single operating practices (primary reporting entity)
    This is the main reporting unit across finance, ops, and marketing
    """
    sql = """
    CREATE TABLE IF NOT EXISTS master.practices (
        practice_id SERIAL PRIMARY KEY,
        client_id INTEGER NOT NULL REFERENCES master.clients(client_id),
        practice_name VARCHAR(255) NOT NULL,
        practice_code VARCHAR(100), -- Internal identifier
        practice_type VARCHAR(100), -- e.g., 'orthodontist', 'general_dentist'
        practice_status VARCHAR(50) DEFAULT 'active',
        address_line1 VARCHAR(255),
        address_line2 VARCHAR(255),
        city VARCHAR(100),
        state VARCHAR(50),
        postal_code VARCHAR(20),
        country VARCHAR(50) DEFAULT 'US',
        phone VARCHAR(50),
        email VARCHAR(255),
        website VARCHAR(255),
        practice_start_date DATE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT valid_practice_status CHECK (practice_status IN ('active', 'inactive', 'pending'))
    )
    """
    return sql

def create_locations_table():
    """
    master.locations - Physical or logical locations tied to practices
    Enables future location-level reporting without changing core schemas
    """
    sql = """
    CREATE TABLE IF NOT EXISTS master.locations (
        location_id SERIAL PRIMARY KEY,
        practice_id INTEGER NOT NULL REFERENCES master.practices(practice_id),
        location_name VARCHAR(255) NOT NULL,
        location_code VARCHAR(100),
        location_type VARCHAR(50), -- e.g., 'main', 'satellite', 'virtual'
        location_status VARCHAR(50) DEFAULT 'active',
        address_line1 VARCHAR(255),
        address_line2 VARCHAR(255),
        city VARCHAR(100),
        state VARCHAR(50),
        postal_code VARCHAR(20),
        country VARCHAR(50) DEFAULT 'US',
        phone VARCHAR(50),
        email VARCHAR(255),
        square_footage INTEGER,
        chair_count INTEGER,
        opened_date DATE,
        closed_date DATE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT valid_location_status CHECK (location_status IN ('active', 'inactive', 'pending'))
    )
    """
    return sql

def create_providers_table():
    """
    master.providers - Individual providers (dentists, hygienists, etc.)
    Used for provider-level breakdowns, targets, and ROI analysis
    """
    sql = """
    CREATE TABLE IF NOT EXISTS master.providers (
        provider_id SERIAL PRIMARY KEY,
        practice_id INTEGER NOT NULL REFERENCES master.practices(practice_id),
        provider_first_name VARCHAR(255) NOT NULL,
        provider_last_name VARCHAR(255) NOT NULL,
        provider_code VARCHAR(100), -- Internal identifier
        provider_type VARCHAR(100), -- e.g., 'orthodontist', 'hygienist', 'assistant'
        provider_status VARCHAR(50) DEFAULT 'active',
        license_number VARCHAR(100),
        license_state VARCHAR(50),
        license_expiry_date DATE,
        hire_date DATE,
        termination_date DATE,
        email VARCHAR(255),
        phone VARCHAR(50),
        specialties TEXT[], -- Array of specialties
        production_target_monthly DECIMAL(10,2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT valid_provider_status CHECK (provider_status IN ('active', 'inactive', 'on_leave'))
    )
    """
    return sql

def create_time_periods_table():
    """
    master.time_periods - Canonical time windows (monthly, biweekly, etc.)
    Ensures consistent time-based aggregation and comparisons (YoY, L3M, YTD)
    """
    sql = """
    CREATE TABLE IF NOT EXISTS master.time_periods (
        time_period_id SERIAL PRIMARY KEY,
        period_type VARCHAR(50) NOT NULL, -- 'monthly', 'biweekly', 'quarterly', 'yearly'
        period_start_date DATE NOT NULL,
        period_end_date DATE NOT NULL,
        period_name VARCHAR(100) NOT NULL, -- e.g., 'Jan 2026', 'Q1 2026', 'Biweek 2026-01'
        period_year INTEGER NOT NULL,
        period_month INTEGER, -- 1-12, NULL for non-monthly periods
        period_quarter INTEGER, -- 1-4, NULL for non-quarterly periods
        period_week INTEGER, -- Week of year, for biweekly periods
        is_complete BOOLEAN DEFAULT FALSE, -- Has the period ended?
        fiscal_year INTEGER, -- If different from calendar year
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(period_type, period_start_date, period_end_date)
    )
    """
    return sql

def create_practice_aliases_table():
    """
    master.practice_aliases - Maps raw practice identifiers to canonical practices
    Allows messy or changing client keys without breaking ingestion
    """
    sql = """
    CREATE TABLE IF NOT EXISTS master.practice_aliases (
        alias_id SERIAL PRIMARY KEY,
        practice_id INTEGER NOT NULL REFERENCES master.practices(practice_id),
        client_id INTEGER NOT NULL REFERENCES master.clients(client_id),
        raw_practice_identifier VARCHAR(255) NOT NULL,
        alias_source VARCHAR(100), -- e.g., 'csv_import', 'api', 'manual'
        alias_context VARCHAR(255), -- Additional context about where this alias comes from
        is_active BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(client_id, raw_practice_identifier, alias_source)
    )
    """
    return sql

def create_provider_aliases_table():
    """
    master.provider_aliases - Maps raw provider identifiers to canonical providers
    Supports reliable provider-level aggregation across inconsistent source data
    """
    sql = """
    CREATE TABLE IF NOT EXISTS master.provider_aliases (
        alias_id SERIAL PRIMARY KEY,
        provider_id INTEGER NOT NULL REFERENCES master.providers(provider_id),
        client_id INTEGER NOT NULL REFERENCES master.clients(client_id),
        raw_provider_identifier VARCHAR(255) NOT NULL,
        raw_provider_name VARCHAR(255), -- Original name from source system
        alias_source VARCHAR(100), -- e.g., 'csv_import', 'api', 'manual'
        alias_context VARCHAR(255), -- Additional context
        is_active BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(client_id, raw_provider_identifier, alias_source)
    )
    """
    return sql

def create_input_templates_table():
    """
    master.input_templates - Client and domain-specific input contracts
    Represents how a client is expected to submit raw data at a high level
    """
    sql = """
    CREATE TABLE IF NOT EXISTS master.input_templates (
        template_id SERIAL PRIMARY KEY,
        client_id INTEGER NOT NULL REFERENCES master.clients(client_id),
        template_name VARCHAR(255) NOT NULL, -- e.g., 'WSO Finance Monthly', 'ABC Ops Weekly'
        template_code VARCHAR(100) NOT NULL, -- e.g., 'wso_fin_monthly'
        domain VARCHAR(50) NOT NULL, -- 'finance', 'operations', 'marketing'
        frequency VARCHAR(50), -- 'monthly', 'weekly', 'daily', 'ad_hoc'
        template_description TEXT,
        is_active BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT valid_domain CHECK (domain IN ('finance', 'operations', 'marketing')),
        UNIQUE(client_id, template_code)
    )
    """
    return sql

def create_input_template_versions_table():
    """
    master.input_template_versions - Versioned history of input templates
    Used to detect, track, and audit changes in client data structures over time
    """
    sql = """
    CREATE TABLE IF NOT EXISTS master.input_template_versions (
        version_id SERIAL PRIMARY KEY,
        template_id INTEGER NOT NULL REFERENCES master.input_templates(template_id),
        version_number INTEGER NOT NULL,
        version_name VARCHAR(100), -- e.g., 'v1.0', 'v2.1_with_provider_codes'
        effective_start_date DATE NOT NULL,
        effective_end_date DATE,
        version_notes TEXT,
        schema_hash VARCHAR(64), -- Hash of the field definitions for quick comparison
        is_current BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_by VARCHAR(255),
        UNIQUE(template_id, version_number)
    )
    """
    return sql

def create_input_template_fields_table():
    """
    master.input_template_fields - Field-level definitions for each template version
    Documents expected fields, types, units, and source references for raw data dumps
    """
    sql = """
    CREATE TABLE IF NOT EXISTS master.input_template_fields (
        field_id SERIAL PRIMARY KEY,
        version_id INTEGER NOT NULL REFERENCES master.input_template_versions(version_id),
        field_order INTEGER NOT NULL,
        field_name VARCHAR(255) NOT NULL,
        field_type VARCHAR(50) NOT NULL, -- 'text', 'integer', 'decimal', 'date', 'boolean'
        field_description TEXT,
        is_required BOOLEAN DEFAULT FALSE,
        is_key_field BOOLEAN DEFAULT FALSE, -- Is this a primary identifier?
        expected_format VARCHAR(255), -- e.g., 'YYYY-MM-DD', 'currency_usd'
        validation_rules TEXT, -- JSON or text description of validation
        default_value VARCHAR(255),
        bronze_column_name VARCHAR(255), -- Mapped bronze layer column name
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(version_id, field_name)
    )
    """
    return sql

def get_all_table_creation_functions():
    """Return all table creation functions"""
    return {
        'clients': create_clients_table,
        'practices': create_practices_table,
        'locations': create_locations_table,
        'providers': create_providers_table,
        'time_periods': create_time_periods_table,
        'practice_aliases': create_practice_aliases_table,
        'provider_aliases': create_provider_aliases_table,
        'input_templates': create_input_templates_table,
        'input_template_versions': create_input_template_versions_table,
        'input_template_fields': create_input_template_fields_table
    }

def create_all_tables():
    """Create all master layer tables"""
    engine = get_engine()
    
    print("🏗️  Creating Master Layer Tables")
    print("=" * 50)
    
    # Create schema first
    create_master_schema()
    
    # Create all tables in dependency order
    table_functions = get_all_table_creation_functions()
    
    with engine.connect() as conn:
        for table_name, create_func in table_functions.items():
            try:
                sql = create_func()
                conn.execute(text(sql))
                print(f"✅ master.{table_name}")
            except Exception as e:
                print(f"❌ master.{table_name}: {e}")
        
        conn.commit()
    
    print("\n🎉 Master layer table creation complete!")

def create_single_table(table_name: str):
    """Create a single master table"""
    table_functions = get_all_table_creation_functions()
    
    if table_name not in table_functions:
        print(f"❌ Unknown table: {table_name}")
        print(f"Available tables: {list(table_functions.keys())}")
        return
    
    engine = get_engine()
    create_master_schema()
    
    with engine.connect() as conn:
        try:
            sql = table_functions[table_name]()
            conn.execute(text(sql))
            conn.commit()
            print(f"✅ Created master.{table_name}")
        except Exception as e:
            print(f"❌ Error creating master.{table_name}: {e}")

def add_client(client_data: dict = None, interactive: bool = True):
    """
    Add a new client with practices and providers
    
    Args:
        client_data: Dict with client information (optional)
        interactive: If True, prompt for missing information
    """
    engine = get_engine()
    
    # Default client data structure
    default_data = {
        'client_name': '',
        'client_tag': '',
        'client_status': 'active',
        'billing_entity': '',
        'primary_contact_name': '',
        'primary_contact_email': '',
        'primary_contact_phone': '',
        'practices': []
    }
    
    # Merge provided data with defaults
    if client_data:
        default_data.update(client_data)
    
    # Interactive prompts for missing data
    if interactive:
        print("🏢 Adding New Client")
        print("=" * 40)
        
        if not default_data['client_name']:
            default_data['client_name'] = input("Client Name: ").strip()
        
        if not default_data['client_tag']:
            suggested_tag = default_data['client_name'].lower().replace(' ', '_')[:10]
            tag_input = input(f"Client Tag (suggested: {suggested_tag}): ").strip()
            default_data['client_tag'] = tag_input if tag_input else suggested_tag
        
        if not default_data['billing_entity']:
            default_data['billing_entity'] = input("Billing Entity (optional): ").strip() or default_data['client_name']
        
        if not default_data['primary_contact_name']:
            default_data['primary_contact_name'] = input("Primary Contact Name: ").strip()
        
        if not default_data['primary_contact_email']:
            default_data['primary_contact_email'] = input("Primary Contact Email: ").strip()
        
        if not default_data['primary_contact_phone']:
            default_data['primary_contact_phone'] = input("Primary Contact Phone (optional): ").strip()
    
    # Validate required fields
    required_fields = ['client_name', 'client_tag', 'primary_contact_name']
    missing = [f for f in required_fields if not default_data.get(f)]
    if missing:
        print(f"❌ Missing required fields: {missing}")
        return None
    
    print(f"\n📝 Creating client: {default_data['client_name']} ({default_data['client_tag']})")
    
    with engine.connect() as conn:
        try:
            # 1. Insert client
            client_sql = text("""
            INSERT INTO master.clients (
                client_name, client_tag, client_status, billing_entity, 
                primary_contact_name, primary_contact_email, primary_contact_phone
            )
            VALUES (:name, :tag, :status, :billing, :contact_name, :contact_email, :contact_phone)
            ON CONFLICT (client_tag) DO UPDATE SET
                client_name = EXCLUDED.client_name,
                billing_entity = EXCLUDED.billing_entity,
                primary_contact_name = EXCLUDED.primary_contact_name,
                primary_contact_email = EXCLUDED.primary_contact_email,
                primary_contact_phone = EXCLUDED.primary_contact_phone,
                updated_at = CURRENT_TIMESTAMP
            RETURNING client_id
            """)
            
            result = conn.execute(client_sql, {
                "name": default_data['client_name'],
                "tag": default_data['client_tag'],
                "status": default_data['client_status'],
                "billing": default_data['billing_entity'],
                "contact_name": default_data['primary_contact_name'],
                "contact_email": default_data['primary_contact_email'],
                "contact_phone": default_data['primary_contact_phone']
            })
            
            client_row = result.fetchone()
            client_id = client_row[0]
            print(f"✅ Client created/updated with ID: {client_id}")
            
            # 2. Add practices if provided or prompt
            practices_to_add = default_data.get('practices', [])
            
            if not practices_to_add and interactive:
                add_practice = input(f"\nAdd a practice for {default_data['client_name']}? (y/n): ").lower().startswith('y')
                if add_practice:
                    practices_to_add = [{}]  # Empty practice to be filled interactively
            
            for i, practice_data in enumerate(practices_to_add):
                print(f"\n🏥 Adding Practice {i+1}")
                practice_id = add_practice_to_client(conn, client_id, practice_data, interactive)
                
                if practice_id and interactive:
                    add_provider = input(f"Add providers to this practice? (y/n): ").lower().startswith('y')
                    if add_provider:
                        add_providers_to_practice(conn, practice_id, interactive=True)
            
            conn.commit()
            print(f"\n🎉 Client setup complete for {default_data['client_name']}!")
            return client_id
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error creating client: {e}")
            return None

def add_practice_to_client(conn, client_id: int, practice_data: dict = None, interactive: bool = True):
    """Add a practice to an existing client"""
    default_practice = {
        'practice_name': '',
        'practice_code': '',
        'practice_type': '',
        'city': '',
        'state': '',
        'email': '',
        'phone': ''
    }
    
    if practice_data:
        default_practice.update(practice_data)
    
    if interactive:
        if not default_practice['practice_name']:
            default_practice['practice_name'] = input("Practice Name: ").strip()
        
        if not default_practice['practice_code']:
            suggested_code = default_practice['practice_name'].upper().replace(' ', '-')[:15]
            code_input = input(f"Practice Code (suggested: {suggested_code}): ").strip()
            default_practice['practice_code'] = code_input if code_input else suggested_code
        
        if not default_practice['practice_type']:
            practice_types = ['orthodontist', 'general_dentist', 'oral_surgeon', 'pediatric_dentist', 'periodontist']
            print(f"Practice types: {', '.join(practice_types)}")
            default_practice['practice_type'] = input("Practice Type: ").strip() or 'general_dentist'
        
        if not default_practice['city']:
            default_practice['city'] = input("City: ").strip()
        
        if not default_practice['state']:
            default_practice['state'] = input("State: ").strip()
        
        if not default_practice['email']:
            default_practice['email'] = input("Practice Email (optional): ").strip()
        
        if not default_practice['phone']:
            default_practice['phone'] = input("Practice Phone (optional): ").strip()
    
    try:
        practice_sql = text("""
        INSERT INTO master.practices (
            client_id, practice_name, practice_code, practice_type, 
            city, state, email, phone
        )
        VALUES (:client_id, :name, :code, :type, :city, :state, :email, :phone)
        RETURNING practice_id
        """)
        
        result = conn.execute(practice_sql, {
            "client_id": client_id,
            "name": default_practice['practice_name'],
            "code": default_practice['practice_code'],
            "type": default_practice['practice_type'],
            "city": default_practice['city'],
            "state": default_practice['state'],
            "email": default_practice['email'],
            "phone": default_practice['phone']
        })
        
        practice_row = result.fetchone()
        practice_id = practice_row[0]
        print(f"✅ Practice '{default_practice['practice_name']}' created with ID: {practice_id}")
        return practice_id
        
    except Exception as e:
        print(f"❌ Error creating practice: {e}")
        return None

def add_providers_to_practice(conn, practice_id: int, providers_data: list = None, interactive: bool = True):
    """Add providers to a practice"""
    providers_to_add = providers_data or []
    
    if interactive and not providers_to_add:
        while True:
            print(f"\n👨‍⚕️ Adding Provider to Practice ID {practice_id}")
            
            first_name = input("Provider First Name: ").strip()
            if not first_name:
                break
                
            last_name = input("Provider Last Name: ").strip()
            if not last_name:
                break
            
            suggested_code = f"{last_name.upper()}-{first_name[0].upper()}"
            provider_code = input(f"Provider Code (suggested: {suggested_code}): ").strip() or suggested_code
            
            provider_types = ['orthodontist', 'dentist', 'hygienist', 'assistant', 'therapist']
            print(f"Provider types: {', '.join(provider_types)}")
            provider_type = input("Provider Type: ").strip() or 'dentist'
            
            providers_to_add.append({
                'first_name': first_name,
                'last_name': last_name,
                'provider_code': provider_code,
                'provider_type': provider_type
            })
            
            add_another = input("Add another provider? (y/n): ").lower().startswith('y')
            if not add_another:
                break
    
    # Insert providers
    for provider in providers_to_add:
        try:
            provider_sql = text("""
            INSERT INTO master.providers (
                practice_id, provider_first_name, provider_last_name, 
                provider_code, provider_type
            )
            VALUES (:practice_id, :first, :last, :code, :type)
            """)
            
            conn.execute(provider_sql, {
                "practice_id": practice_id,
                "first": provider['first_name'],
                "last": provider['last_name'],
                "code": provider['provider_code'],
                "type": provider['provider_type']
            })
            
            print(f"✅ Provider {provider['first_name']} {provider['last_name']} added")
            
        except Exception as e:
            print(f"❌ Error adding provider {provider.get('first_name', '')}: {e}")

def populate_sample_data_for_wso():
    """Backward compatibility - populate WSO using the new generic function"""
    wso_data = {
        'client_name': 'Wall Street Orthodontics',
        'client_tag': 'wso',
        'billing_entity': 'Wall Street Orthodontics LLC',
        'primary_contact_name': 'Dr. Lucas Shapiro',
        'primary_contact_email': 'contact@wallstreetortho.com',
        'practices': [{
            'practice_name': 'Wall Street Orthodontics',
            'practice_code': 'WSO-MAIN',
            'practice_type': 'orthodontist',
            'city': 'New York',
            'state': 'NY',
            'email': 'info@wallstreetortho.com'
        }]
    }
    
    print("📝 Populating WSO data using generic client setup...")
    return add_client(wso_data, interactive=False)

def generate_time_periods(start_year: int = 2024, end_year: int = 2027):
    """Generate standard time periods for the given year range"""
    engine = get_engine()
    
    print(f"📅 Generating time periods for {start_year}-{end_year}")
    
    periods = []
    
    for year in range(start_year, end_year + 1):
        # Monthly periods
        for month in range(1, 13):
            start_date = date(year, month, 1)
            if month == 12:
                end_date = date(year + 1, 1, 1) - pd.Timedelta(days=1)
            else:
                end_date = date(year, month + 1, 1) - pd.Timedelta(days=1)
            
            periods.append({
                'period_type': 'monthly',
                'period_start_date': start_date,
                'period_end_date': end_date,
                'period_name': start_date.strftime('%b %Y'),
                'period_year': year,
                'period_month': month,
                'period_quarter': (month - 1) // 3 + 1,
                'is_complete': end_date < date.today()
            })
        
        # Quarterly periods
        for quarter in range(1, 5):
            start_month = (quarter - 1) * 3 + 1
            start_date = date(year, start_month, 1)
            
            if quarter == 4:
                end_date = date(year + 1, 1, 1) - pd.Timedelta(days=1)
            else:
                end_date = date(year, start_month + 3, 1) - pd.Timedelta(days=1)
            
            periods.append({
                'period_type': 'quarterly',
                'period_start_date': start_date,
                'period_end_date': end_date,
                'period_name': f'Q{quarter} {year}',
                'period_year': year,
                'period_quarter': quarter,
                'is_complete': end_date < date.today()
            })
        
        # Yearly period
        periods.append({
            'period_type': 'yearly',
            'period_start_date': date(year, 1, 1),
            'period_end_date': date(year, 12, 31),
            'period_name': str(year),
            'period_year': year,
            'is_complete': year < date.today().year
        })
    
    # Insert periods
    df = pd.DataFrame(periods)
    with engine.connect() as conn:
        df.to_sql('time_periods', conn, schema='master', if_exists='append', index=False, method='multi')
        conn.commit()
    
    print(f"✅ Generated {len(periods)} time periods")

def main():
    parser = argparse.ArgumentParser(description='Master Layer Setup Script')
    parser.add_argument('--action', required=True, 
                       choices=['create_all', 'create_table', 'add_client', 'populate_sample', 'generate_time_periods'],
                       help='Action to perform')
    parser.add_argument('--table', help='Specific table name (for create_table action)')
    parser.add_argument('--client', help='Client tag (for populate_sample action)', default='wso')
    parser.add_argument('--client-name', help='Client name (for add_client action)')
    parser.add_argument('--client-tag', help='Client tag/code (for add_client action)')
    parser.add_argument('--contact-name', help='Primary contact name (for add_client action)')
    parser.add_argument('--contact-email', help='Primary contact email (for add_client action)')
    parser.add_argument('--interactive', action='store_true', default=True, help='Use interactive mode')
    parser.add_argument('--non-interactive', action='store_true', help='Disable interactive prompts')
    parser.add_argument('--start-year', type=int, default=2024, help='Start year for time periods')
    parser.add_argument('--end-year', type=int, default=2027, help='End year for time periods')
    
    args = parser.parse_args()
    
    # Handle interactive mode
    interactive = args.interactive and not args.non_interactive
    
    if args.action == 'create_all':
        create_all_tables()
    elif args.action == 'create_table':
        if not args.table:
            print("❌ --table argument required for create_table action")
            return
        create_single_table(args.table)
    elif args.action == 'add_client':
        client_data = {}
        if args.client_name:
            client_data['client_name'] = args.client_name
        if args.client_tag:
            client_data['client_tag'] = args.client_tag
        if args.contact_name:
            client_data['primary_contact_name'] = args.contact_name
        if args.contact_email:
            client_data['primary_contact_email'] = args.contact_email
        
        add_client(client_data if client_data else None, interactive=interactive)
    elif args.action == 'populate_sample':
        if args.client == 'wso':
            populate_sample_data_for_wso()
        else:
            print(f"❌ Sample data not implemented for client: {args.client}")
            print(f"💡 Use --action add_client to add a new client")
    elif args.action == 'generate_time_periods':
        generate_time_periods(args.start_year, args.end_year)

if __name__ == "__main__":
    main()