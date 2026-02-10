#!/usr/bin/env python3
"""
ETL Pipeline: Bronze → Silver → Gold Transformation
==================================================

This script transforms raw bronze layer data into cleaned silver facts
and aggregated gold metrics for reporting.

Bronze → Silver:
- Extract new patient appointments from bronze_ops.appointments_raw_wso
- Join with referral data from bronze_ops.referrals_raw_wso  
- Apply client-specific mappings and create canonical facts in silver_ops.fact_new_patient_intake

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
        connection.commit()
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
        connection.commit()
        logger.info(f"Created new practice: {practice_name}")
        return str(result[0])

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
    connection.commit()

def create_appointment_type_mappings(connection, client_id):
    """Create appointment type mappings for Wall Street Orthodontics"""
    logger.info("Creating appointment type mappings...")
    
    mappings_sql = """
    INSERT INTO master.client_appointment_type_mappings 
    (client_id, domain, source_system, appointment_type_code, canonical_type, notes)
    VALUES 
    (:client_id, 'ops', 'practice_management', 'EXAM ADULT LS, ITERO SCAN, PHOTOS, CBCT', 'new_patient_intake', 'Adult exam with full diagnostic records'),
    (:client_id, 'ops', 'practice_management', 'LS EXAM, ITERO SCAN, 1 SMILING PHOTO (TO EVAL IF ELIGIBILE FOR C5)', 'new_patient_intake', 'Adult exam for clear aligner evaluation'),
    (:client_id, 'ops', 'practice_management', 'EXAM CHILD LS, ITERO SCAN, PHOTOS, CBCT', 'new_patient_intake', 'Child exam with full diagnostic records'),
    (:client_id, 'ops', 'practice_management', 'DIAGNOSTIC RECORDS - PHOTOS, CBCT, ITERO SCAN', 'new_patient_intake', 'Diagnostic records appointment'),
    (:client_id, 'ops', 'practice_management', 'P-Consultation', 'new_patient_intake', 'Patient consultation'),
    (:client_id, 'ops', 'practice_management', 'EXAM TRANSFER IN CHILD LS', 'new_patient_intake', 'Transfer patient examination')
    ON CONFLICT (client_id, source_system, appointment_type_code, canonical_type) DO NOTHING;
    """
    
    connection.execute(text(mappings_sql), {'client_id': client_id})
    connection.commit()

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
    connection.commit()

def extract_transform_to_silver(connection, client_id, practice_id):
    """Extract bronze data and transform to silver layer"""
    logger.info("Transforming bronze → silver...")    
    # Clear existing data for this client/practice
    clear_silver_sql = """
    DELETE FROM silver_ops.fact_new_patient_intake 
    WHERE client_id = CAST(:client_id AS uuid) 
    AND practice_id = CAST(:practice_id AS uuid);
    """
    connection.execute(text(clear_silver_sql), {'client_id': client_id, 'practice_id': practice_id})
    connection.commit()
    
    # Transform bronze to silver
    transform_sql = """
    INSERT INTO silver_ops.fact_new_patient_intake 
    (client_id, practice_id, patient_id_guid, patient_id, intake_date, time_period_id, 
     referral_category, referral_name, source_system)
    SELECT 
        CAST(:client_id AS uuid) as client_id,
        CAST(:practice_id AS uuid) as practice_id,
        CAST(REPLACE(REPLACE(a.patient_id_guid, '{', ''), '}', '') AS uuid) as patient_id_guid,
        a.patient_id,
        a.appointment_date::date as intake_date,
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
    FROM (
        -- Get first appointment per patient that matches new patient intake criteria
        SELECT DISTINCT ON (a.patient_id_guid)
            a.*
        FROM bronze_ops.appointments_raw_wso a
        INNER JOIN master.client_appointment_type_mappings atm 
            ON atm.client_id = CAST(:client_id AS uuid)
            AND atm.canonical_type = 'new_patient_intake'
            AND a.appointment_type_description = atm.appointment_type_code
        WHERE a.patient_id_guid IS NOT NULL
        ORDER BY a.patient_id_guid, a.appointment_date
    ) a
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
    ON CONFLICT (client_id, practice_id, patient_id_guid) DO NOTHING;
    """
    
    result = connection.execute(text(transform_sql), {'client_id': client_id, 'practice_id': practice_id})
    rows_inserted = result.rowcount
    connection.commit()
    
    logger.info(f"✅ Inserted {rows_inserted} rows into silver_ops.fact_new_patient_intake")
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
        FROM silver_ops.fact_new_patient_intake f
        INNER JOIN master.time_periods tp ON f.time_period_id = tp.id
        WHERE client_id = CAST(:client_id AS uuid) AND practice_id = CAST(:practice_id AS uuid)
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
    connection.commit()
    
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
        FROM silver_ops.fact_new_patient_intake
        WHERE client_id = CAST(:client_id AS uuid) AND practice_id = CAST(:practice_id AS uuid)
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
        FROM silver_ops.fact_new_patient_intake f
        INNER JOIN monthly_totals mt 
            ON f.client_id = mt.client_id 
            AND f.practice_id = mt.practice_id 
            AND f.time_period_id = mt.time_period_id
        WHERE f.client_id = CAST(:client_id AS uuid) AND f.practice_id = CAST(:practice_id AS uuid)
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
        FROM silver_ops.fact_new_patient_intake f
        INNER JOIN monthly_totals mt 
            ON f.client_id = mt.client_id 
            AND f.practice_id = mt.practice_id 
            AND f.time_period_id = mt.time_period_id
        WHERE f.client_id = CAST(:client_id AS uuid) AND f.practice_id = CAST(:practice_id AS uuid)
        GROUP BY f.client_id, f.practice_id, f.time_period_id, f.referral_name, f.referral_category, mt.total_monthly_cnt
    )
    SELECT * FROM category_breakdown
    UNION ALL
    SELECT * FROM name_breakdown
    ON CONFLICT (client_id, practice_id, time_period_id, breakdown_type, breakdown_value) DO NOTHING;
    """
    
    result = connection.execute(text(breakdown_sql), {'client_id': client_id, 'practice_id': practice_id})
    rows_inserted = result.rowcount
    connection.commit()
    
    logger.info(f"✅ Inserted {rows_inserted} rows into gold_ops.referrals_monthly_breakdown")
    return rows_inserted

def run_etl_pipeline(client_name='Wall Street Orthodontics'):
    """Run the complete ETL pipeline"""
    logger.info("🚀 Starting ETL Pipeline...")
    
    engine = get_engine()
    
    with engine.connect() as connection:
        # Get or create client and practice
        client_id = get_client_id(connection, client_name)
        practice_id = get_practice_id(connection, client_id)
        
        # Ensure supporting data exists
        ensure_time_periods(connection)
        create_appointment_type_mappings(connection, client_id)
        create_referral_category_mappings(connection, client_id)
        
        # Run ETL transformations
        silver_rows = extract_transform_to_silver(connection, client_id, practice_id)
        
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