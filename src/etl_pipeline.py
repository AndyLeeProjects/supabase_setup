#!/usr/bin/env python3
"""
ETL Pipeline: Bronze → Silver → Gold Transformation
==================================================

This script transforms raw bronze layer data into cleaned silver facts
and aggregated gold metrics for reporting.

Bronze → Silver:
- Extract all appointments from bronze_ops.appointments_raw_wso
- Join with referral data from bronze_ops.referrals_raw_wso  
- Apply client-specific mappings and create canonical facts in silver_ops.referrals
- Mark appointments as 'New Patient' using appointment_type_mappings

Silver → Gold:
- Aggregate silver facts into monthly summaries in gold_ops.referrals_monthly_summary
- Create monthly breakdowns by category/referrer in gold_ops.referrals_monthly_breakdown
"""

import sys
import logging
from pathlib import Path
from datetime import datetime, date
import pandas as pd

# Add utils to path
sys.path.append(str(Path(__file__).parent.parent / 'utils'))

from connect_db import get_engine
from sqlalchemy import text

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Client-specific ETL configuration for referrals pipeline
CLIENT_ETL_CONFIG = {
    'Wall Street Orthodontics': {
        'min_appointment_date': '2025-01-01',  # Only process appointments from this date onwards
        'description': 'Appointments from 2025-01-01 onwards'
    }
    # Add more clients here as needed
    # 'Another Client': {
    #     'min_appointment_date': '2024-01-01',
    #     'description': 'Appointments from 2024-01-01 onwards'
    # }
}

def get_client_etl_config(client_name):
    """Get ETL configuration for a specific client"""
    config = CLIENT_ETL_CONFIG.get(client_name, {})
    min_date = config.get('min_appointment_date', '2020-01-01')  # Default to 2020 if not specified
    logger.info(f"ETL config for {client_name}: min_appointment_date = {min_date}")
    return min_date

def get_client_id(connection, client_name='Wall Street Orthodontics'):
    """Get client ID for Wall Street Orthodontics"""
    query = """
    SELECT id FROM master.clients 
    WHERE name ILIKE :client_name
    LIMIT 1
    """
    result = connection.execute(text(query), {'client_name': f'%{client_name}%'}).fetchone()
    if result:
        return str(result[0])
    else:
        # Create the client if it doesn't exist
        insert_query = """
        INSERT INTO master.clients (name, slug, status)
        VALUES (:name, :slug, :status)
        RETURNING id
        """
        slug = client_name.lower().replace(' ', '_').replace("'", "")
        result = connection.execute(text(insert_query), {
            'name': client_name, 
            'slug': slug, 
            'status': 'active'
        }).fetchone()
        logger.info(f"Created new client: {client_name}")
        return str(result[0])

def get_practice_id(connection, client_id, practice_name='Wall Street Orthodontics Main'):
    """Get or create practice ID"""
    query = """
    SELECT id FROM master.practices 
    WHERE client_id = :client_id AND name ILIKE :practice_name
    LIMIT 1
    """
    result = connection.execute(text(query), {
        'client_id': client_id, 
        'practice_name': f'%{practice_name}%'
    }).fetchone()
    if result:
        return str(result[0])
    else:
        # Create the practice if it doesn't exist
        insert_query = """
        INSERT INTO master.practices (client_id, name, is_active)
        VALUES (:client_id, :name, :is_active)
        RETURNING id
        """
        result = connection.execute(text(insert_query), {
            'client_id': client_id,
            'name': practice_name,
            'is_active': True
        }).fetchone()
        logger.info(f"Created new practice: {practice_name}")
        return str(result[0])

def ensure_silver_table_exists(connection):
    """Create silver.referrals table if it doesn't exist"""
    logger.info("Ensuring silver table exists...")
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS silver_ops.referrals (
        id UUID NOT NULL DEFAULT gen_random_uuid(),
        
        client_id UUID NOT NULL,
        practice_id UUID NOT NULL,
        
        patient_id_guid TEXT NOT NULL,
        patient_id TEXT,
        
        appointment_date DATE NOT NULL,
        appointment_type TEXT,
        appointment_status TEXT,
        is_new_patient BOOLEAN DEFAULT FALSE,
        
        time_period_id UUID NOT NULL,
        
        referral_category TEXT,
        referral_name TEXT,
        
        source_system TEXT NOT NULL DEFAULT 'practice_management',
        
        created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
        updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
        
        CONSTRAINT referrals_pkey PRIMARY KEY (id),
        CONSTRAINT referrals_client_id_fkey 
            FOREIGN KEY (client_id) REFERENCES master.clients(id),
        CONSTRAINT referrals_practice_id_fkey 
            FOREIGN KEY (practice_id) REFERENCES master.practices(id),
        CONSTRAINT referrals_time_period_id_fkey 
            FOREIGN KEY (time_period_id) REFERENCES master.time_periods(id),
        CONSTRAINT referrals_unique 
            UNIQUE (client_id, practice_id, patient_id_guid)
    );
    
    CREATE INDEX IF NOT EXISTS idx_referrals_client_practice_period
        ON silver_ops.referrals (client_id, practice_id, time_period_id);
    
    CREATE INDEX IF NOT EXISTS idx_referrals_client_practice_date
        ON silver_ops.referrals (client_id, practice_id, appointment_date);
    
    CREATE INDEX IF NOT EXISTS idx_referrals_is_new_patient
        ON silver_ops.referrals (client_id, practice_id, is_new_patient);
    
    CREATE INDEX IF NOT EXISTS idx_referrals_category
        ON silver_ops.referrals (client_id, practice_id, referral_category);
    """
    
    connection.execute(text(create_table_sql))
    logger.info("✅ Silver table ready")

def ensure_time_periods(connection):
    """Ensure time periods exist for the data range"""
    logger.info("Creating time periods...")
    
    # Create time periods for 2024-2026 by month
    time_periods_sql = """
    INSERT INTO master.time_periods (period_type, start_date, end_date, label, year, month)
    SELECT 
        'month' as period_type,
        date_trunc('month', generate_series) as start_date,
        (date_trunc('month', generate_series) + interval '1 month' - interval '1 day')::date as end_date,
        to_char(generate_series, 'YYYY-MM') as label,
        EXTRACT(year FROM generate_series) as year,
        EXTRACT(month FROM generate_series) as month
    FROM generate_series('2024-01-01'::date, '2026-12-01'::date, '1 month')
    ON CONFLICT (period_type, start_date, end_date) DO NOTHING;
    """
    connection.execute(text(time_periods_sql))

def create_appointment_type_mappings(connection, client_id):
    """Create appointment type mappings for Wall Street Orthodontics using new schema"""
    logger.info("Creating appointment type mappings...")
    
    mappings_sql = """
    INSERT INTO master.appointment_type_mappings 
    (client_id, practice_id, source_appointment_type, standardized_category, start_date, end_date, notes)
    VALUES 
    (:client_id, NULL, 'EXAM ADULT LS, ITERO SCAN, PHOTOS, CBCT', 'New Patient', '2024-01-01', NULL, 'Adult exam with full diagnostic records'),
    (:client_id, NULL, 'LS EXAM, ITERO SCAN, 1 SMILING PHOTO (TO EVAL IF ELIGIBILE FOR C5)', 'New Patient', '2024-01-01', NULL, 'Adult exam for clear aligner evaluation'),
    (:client_id, NULL, 'EXAM CHILD LS, ITERO SCAN, PHOTOS, CBCT', 'New Patient', '2024-01-01', NULL, 'Child exam with full diagnostic records'),
    (:client_id, NULL, 'DIAGNOSTIC RECORDS - PHOTOS, CBCT, ITERO SCAN', 'New Patient', '2024-01-01', NULL, 'Diagnostic records appointment'),
    (:client_id, NULL, 'P-Consultation', 'New Patient', '2024-01-01', NULL, 'Patient consultation'),
    (:client_id, NULL, 'EXAM TRANSFER IN CHILD LS', 'New Patient', '2024-01-01', NULL, 'Transfer patient examination')
    ON CONFLICT (client_id, practice_id, source_appointment_type, start_date) DO NOTHING;
    """
    
    connection.execute(text(mappings_sql), {'client_id': client_id})

def create_referral_category_mappings(connection, client_id):
    """Create referral category mappings for Wall Street Orthodontics"""
    logger.info("Creating referral category mappings...")
    
    mappings_sql = """
    INSERT INTO master.client_referral_category_mappings 
    (client_id, source_system, raw_referral_category, canonical_referral_category, notes)
    VALUES 
    (:client_id, 'practice_management', 'Doctor', 'doctor', 'Referring physician'),
    (:client_id, 'practice_management', 'Patient', 'patient', 'Existing patient referral'),
    (:client_id, 'practice_management', 'Non-Patient', 'non_patient', 'Non-patient referral'),
    (:client_id, 'practice_management', 'Other', 'other', 'Other referral source'),
    (:client_id, 'practice_management', 'Billing Party', 'billing_party', 'Billing party referral'),
    (:client_id, 'practice_management', '', 'missing', 'Empty/null referral category'),
    (:client_id, 'practice_management', 'Unknown', 'missing', 'Unknown referral category')
    ON CONFLICT (client_id, source_system, raw_referral_category) DO NOTHING;
    """
    
    connection.execute(text(mappings_sql), {'client_id': client_id})

def extract_transform_to_silver(connection, client_id, practice_id, client_name='Wall Street Orthodontics'):
    """Extract bronze data and transform to silver layer
    
    Creates one row per unique patient with their EARLIEST appointment date.
    Applies client-specific date filters from CLIENT_ETL_CONFIG.
    """
    logger.info("Transforming bronze → silver...")
    
    # Get client-specific configuration
    min_appointment_date = get_client_etl_config(client_name)
    
    # Clear existing data for this client/practice
    clear_silver_sql = """
    DELETE FROM silver_ops.referrals 
    WHERE client_id = CAST(:client_id AS uuid) 
    AND practice_id = CAST(:practice_id AS uuid);
    """
    connection.execute(text(clear_silver_sql), {'client_id': client_id, 'practice_id': practice_id})
    
    # Transform bronze to silver - ONE ROW PER PATIENT with their EARLIEST appointment
    # Applies custom date filtering per client
    transform_sql = """
    INSERT INTO silver_ops.referrals 
    (client_id, practice_id, patient_id_guid, patient_id, appointment_date, 
     appointment_type, appointment_status, is_new_patient, time_period_id, 
     referral_category, referral_name, source_system)
    SELECT DISTINCT ON (patient_id_guid)
        CAST(:client_id AS uuid) as client_id,
        CAST(:practice_id AS uuid) as practice_id,
        CAST(REPLACE(REPLACE(a.patient_id_guid, '{', ''), '}', '') AS uuid) as patient_id_guid,
        a.patient_id,
        a.appointment_date::date as appointment_date,
        a.appointment_type_description as appointment_type,
        a.appointment_status_description as appointment_status,
        -- Mark as New Patient if it matches appointment type mappings
        CASE 
            WHEN atm.id IS NOT NULL THEN TRUE
            ELSE FALSE
        END as is_new_patient,
        tp.id as time_period_id,
        COALESCE(rcm.canonical_referral_category, 
                 CASE 
                     WHEN LOWER(COALESCE(r.referred_in_by_type_description, '')) = '' THEN 'missing'
                     WHEN LOWER(r.referred_in_by_type_description) = 'doctor' THEN 'doctor'
                     WHEN LOWER(r.referred_in_by_type_description) = 'patient' THEN 'patient'
                     WHEN LOWER(r.referred_in_by_type_description) = 'non-patient' THEN 'non_patient'
                     WHEN LOWER(r.referred_in_by_type_description) = 'other' THEN 'other'
                     WHEN LOWER(r.referred_in_by_type_description) = 'billing party' THEN 'billing_party'
                     ELSE 'missing'
                 END) as referral_category,
        CONCAT(r.referred_in_by_first_name, ' ', r.referred_in_by_last_name) as referral_name,
        'practice_management' as source_system
    FROM bronze_ops.appointments_raw_wso a
    LEFT JOIN master.appointment_type_mappings atm 
        ON atm.client_id = CAST(:client_id AS uuid)
        AND atm.standardized_category = 'New Patient'
        AND a.appointment_type_description = atm.source_appointment_type
        AND a.appointment_date::date >= atm.start_date
        AND (atm.end_date IS NULL OR a.appointment_date::date <= atm.end_date)
        AND (atm.practice_id IS NULL OR atm.practice_id = CAST(:practice_id AS uuid))
    LEFT JOIN bronze_ops.referrals_raw_wso r 
        ON REPLACE(REPLACE(a.patient_id_guid, '{', ''), '}', '') = REPLACE(REPLACE(r.patient_id_guid, '{', ''), '}', '')
    LEFT JOIN master.client_referral_category_mappings rcm 
        ON rcm.client_id = CAST(:client_id AS uuid)
        AND rcm.raw_referral_category = r.referred_in_by_type_description
    INNER JOIN master.time_periods tp 
        ON tp.period_type = 'month' 
        AND a.appointment_date::date >= tp.start_date 
        AND a.appointment_date::date <= tp.end_date
    WHERE a.appointment_date IS NOT NULL
        AND a.patient_id_guid IS NOT NULL
        AND a.appointment_date::date >= CAST(:min_appointment_date AS date)
    ORDER BY patient_id_guid, a.appointment_date ASC;
    """
    
    result = connection.execute(text(transform_sql), {
        'client_id': client_id, 
        'practice_id': practice_id,
        'min_appointment_date': min_appointment_date
    })
    rows_inserted = result.rowcount
    
    logger.info(f"✅ Inserted {rows_inserted} unique patients into silver_ops.referrals (filtered: appointments >= {min_appointment_date})")
    return rows_inserted

def aggregate_to_gold_summary(connection, client_id, practice_id):
    """Aggregate silver data to gold monthly summary"""
    logger.info("Aggregating silver → gold summary...")
    
    # Clear existing gold summary data
    clear_sql = """
    DELETE FROM gold_ops.referrals_monthly_summary 
    WHERE client_id = :client_id AND practice_id = :practice_id;
    """
    connection.execute(text(clear_sql), {'client_id': client_id, 'practice_id': practice_id})
    
    # Aggregate to gold summary
    aggregate_sql = """
    INSERT INTO gold_ops.referrals_monthly_summary 
    (client_id, practice_id, time_period_id, monthly_new_patient_cnt, 
     l3m_avg_new_patient_cnt, variance_from_l3m, ytd_new_patient_cnt)
    WITH monthly_counts AS (
        SELECT 
            client_id,
            practice_id,
            time_period_id,
            COUNT(*) as monthly_new_patient_cnt,
            tp.start_date
        FROM silver_ops.referrals f
        INNER JOIN master.time_periods tp ON f.time_period_id = tp.id
        WHERE client_id = CAST(:client_id AS uuid) 
            AND practice_id = CAST(:practice_id AS uuid)
            AND is_new_patient = TRUE
        GROUP BY client_id, practice_id, time_period_id, tp.start_date
    ),
    with_l3m AS (
        SELECT *,
            AVG(monthly_new_patient_cnt) OVER (
                PARTITION BY client_id, practice_id 
                ORDER BY start_date 
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) as l3m_avg_new_patient_cnt
        FROM monthly_counts
    ),
    with_variance AS (
        SELECT *,
            CASE 
                WHEN l3m_avg_new_patient_cnt > 0 THEN 
                    (monthly_new_patient_cnt::numeric / l3m_avg_new_patient_cnt) - 1
                ELSE NULL 
            END as variance_from_l3m
        FROM with_l3m
    ),
    with_ytd AS (
        SELECT *,
            SUM(monthly_new_patient_cnt) OVER (
                PARTITION BY client_id, practice_id, EXTRACT(year FROM start_date)
                ORDER BY start_date
            ) as ytd_new_patient_cnt
        FROM with_variance
    )
    SELECT 
        client_id, practice_id, time_period_id, monthly_new_patient_cnt,
        ROUND(l3m_avg_new_patient_cnt, 2) as l3m_avg_new_patient_cnt,
        ROUND(variance_from_l3m, 4) as variance_from_l3m,
        ytd_new_patient_cnt
    FROM with_ytd;
    """
    
    result = connection.execute(text(aggregate_sql), {'client_id': client_id, 'practice_id': practice_id})
    rows_inserted = result.rowcount

    
    logger.info(f"✅ Inserted {rows_inserted} rows into gold_ops.referrals_monthly_summary")
    return rows_inserted

def aggregate_to_gold_breakdown(connection, client_id, practice_id):
    """Aggregate silver data to gold monthly breakdown"""
    logger.info("Aggregating silver → gold breakdown...")
    
    # Clear existing gold breakdown data
    clear_sql = """
    DELETE FROM gold_ops.referrals_monthly_breakdown 
    WHERE client_id = :client_id AND practice_id = :practice_id;
    """
    connection.execute(text(clear_sql), {'client_id': client_id, 'practice_id': practice_id})
    
    # Aggregate to gold breakdown
    breakdown_sql = """
    INSERT INTO gold_ops.referrals_monthly_breakdown 
    (client_id, practice_id, time_period_id, breakdown_type, breakdown_value, 
     referral_category, monthly_new_patient_cnt, monthly_pct_of_total)
    WITH monthly_totals AS (
        SELECT 
            client_id, practice_id, time_period_id,
            COUNT(*) as total_monthly_cnt
        FROM silver_ops.referrals
        WHERE client_id = CAST(:client_id AS uuid) 
            AND practice_id = CAST(:practice_id AS uuid)
            AND is_new_patient = TRUE
        GROUP BY client_id, practice_id, time_period_id
    ),
    category_breakdown AS (
        SELECT 
            f.client_id, f.practice_id, f.time_period_id,
            'referral_category' as breakdown_type,
            f.referral_category as breakdown_value,
            f.referral_category,
            COUNT(*) as monthly_new_patient_cnt,
            ROUND(COUNT(*)::numeric / mt.total_monthly_cnt * 100, 2) as monthly_pct_of_total
        FROM silver_ops.referrals f
        INNER JOIN monthly_totals mt 
            ON f.client_id = mt.client_id 
            AND f.practice_id = mt.practice_id 
            AND f.time_period_id = mt.time_period_id
        WHERE f.client_id = CAST(:client_id AS uuid) 
            AND f.practice_id = CAST(:practice_id AS uuid)
            AND f.is_new_patient = TRUE
        GROUP BY f.client_id, f.practice_id, f.time_period_id, f.referral_category, mt.total_monthly_cnt
    ),
    name_breakdown AS (
        SELECT 
            f.client_id, f.practice_id, f.time_period_id,
            'referral_name' as breakdown_type,
            COALESCE(f.referral_name, 'Unknown') as breakdown_value,
            f.referral_category,
            COUNT(*) as monthly_new_patient_cnt,
            ROUND(COUNT(*)::numeric / mt.total_monthly_cnt * 100, 2) as monthly_pct_of_total
        FROM silver_ops.referrals f
        INNER JOIN monthly_totals mt 
            ON f.client_id = mt.client_id 
            AND f.practice_id = mt.practice_id 
            AND f.time_period_id = mt.time_period_id
        WHERE f.client_id = CAST(:client_id AS uuid) 
            AND f.practice_id = CAST(:practice_id AS uuid)
            AND f.is_new_patient = TRUE
        GROUP BY f.client_id, f.practice_id, f.time_period_id, f.referral_name, f.referral_category, mt.total_monthly_cnt
    )
    SELECT * FROM category_breakdown
    UNION ALL
    SELECT * FROM name_breakdown
    ON CONFLICT (client_id, practice_id, time_period_id, breakdown_type, breakdown_value) DO NOTHING;
    """
    
    result = connection.execute(text(breakdown_sql), {'client_id': client_id, 'practice_id': practice_id})
    rows_inserted = result.rowcount
    
    logger.info(f"✅ Inserted {rows_inserted} rows into gold_ops.referrals_monthly_breakdown")
    return rows_inserted

def run_etl_pipeline(client_name='Wall Street Orthodontics'):
    """Run the complete ETL pipeline"""
    logger.info("🚀 Starting Referrals ETL Pipeline...")
    
    # Log the ETL configuration being applied
    min_date = get_client_etl_config(client_name)
    logger.info(f"📋 ETL config for {client_name}: min_appointment_date = {min_date}")
    
    engine = get_engine()
    
    with engine.begin() as connection:
        # Get or create client and practice
        client_id = get_client_id(connection, client_name)
        practice_id = get_practice_id(connection, client_id)
        
        # Ensure supporting data and tables exist
        ensure_silver_table_exists(connection)
        ensure_time_periods(connection)
        create_appointment_type_mappings(connection, client_id)
        create_referral_category_mappings(connection, client_id)
        
        # Run ETL transformations
        silver_rows = extract_transform_to_silver(connection, client_id, practice_id, client_name)
        
        if silver_rows > 0:
            summary_rows = aggregate_to_gold_summary(connection, client_id, practice_id)
            breakdown_rows = aggregate_to_gold_breakdown(connection, client_id, practice_id)
            
            logger.info("🎉 ETL Pipeline completed successfully!")
            logger.info(f"📊 Results:")
            logger.info(f"  - Silver facts: {silver_rows} rows")
            logger.info(f"  - Gold summary: {summary_rows} rows") 
            logger.info(f"  - Gold breakdown: {breakdown_rows} rows")
            
            return {
                'success': True,
                'silver_rows': silver_rows,
                'summary_rows': summary_rows,
                'breakdown_rows': breakdown_rows
            }
        else:
            logger.warning("⚠️ No silver data created - check bronze data and appointment type mappings")
            return {
                'success': False,
                'message': 'No qualifying appointment data found in bronze layer'
            }

if __name__ == "__main__":
    run_etl_pipeline()