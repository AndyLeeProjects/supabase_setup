-- ============================================================================
-- Master Table: Appointment Type Mappings
-- ============================================================================
-- Purpose: Maps source system appointment type codes to standardized categories
-- Use Case: Different practices use different codes for "New Patient" appointments
-- Features: 
--   - Supports time-based mappings (start_date, end_date)
--   - Practice-specific or client-wide mappings
--   - Extensible for multiple appointment categories
-- ============================================================================

-- CREATE TABLE IF NOT EXISTS master.appointment_type_mappings (
--     id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
--     -- Foreign Keys
--     client_id UUID NOT NULL REFERENCES master.clients(id) ON DELETE CASCADE,
--     practice_id UUID REFERENCES master.practices(id) ON DELETE CASCADE,
    
--     -- Mapping Fields
--     source_appointment_type VARCHAR(255) NOT NULL,  -- Raw value from source system (e.g., "NPE", "New Pt", "NP1")
--     standardized_category VARCHAR(100) NOT NULL,    -- Standardized value (e.g., "New Patient", "Recall", "Emergency")
    
--     -- Temporal Validity
--     start_date DATE NOT NULL,                       -- When this mapping becomes effective
--     end_date DATE,                                  -- When this mapping expires (NULL = active indefinitely)
    
--     -- Metadata
--     notes TEXT,                                     -- Optional notes about this mapping
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
--     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
--     -- Constraints
--     CONSTRAINT valid_date_range CHECK (end_date IS NULL OR end_date >= start_date),
--     CONSTRAINT unique_mapping UNIQUE (client_id, practice_id, source_appointment_type, start_date)
-- );

-- -- ============================================================================
-- -- Indexes for Performance
-- -- ============================================================================

-- CREATE INDEX idx_apt_mapping_client ON master.appointment_type_mappings(client_id);
-- CREATE INDEX idx_apt_mapping_practice ON master.appointment_type_mappings(practice_id);
-- CREATE INDEX idx_apt_mapping_dates ON master.appointment_type_mappings(start_date, end_date);
-- CREATE INDEX idx_apt_mapping_category ON master.appointment_type_mappings(standardized_category);
-- CREATE INDEX idx_apt_mapping_source_type ON master.appointment_type_mappings(source_appointment_type);

-- ============================================================================
-- Sample Data for Wall Street Orthodontics
-- ============================================================================

-- Insert sample mappings (adjust client_id and practice_id as needed)
-- Uncomment and modify the following lines after verifying your client/practice IDs

/*
INSERT INTO master.appointment_type_mappings 
    (client_id, practice_id, source_appointment_type, standardized_category, start_date, end_date, notes)
VALUES 
    -- New Patient appointment types
    ('your-client-id-here', NULL, 'NPE', 'New Patient', '2024-01-01', NULL, 'New patient exam'),
    ('your-client-id-here', NULL, 'New Patient', 'New Patient', '2024-01-01', NULL, 'Standard new patient appointment'),
    ('your-client-id-here', NULL, 'NP', 'New Patient', '2024-01-01', NULL, 'New patient abbreviation'),
    ('your-client-id-here', NULL, 'Initial Consult', 'New Patient', '2024-01-01', NULL, 'Initial consultation counts as new patient'),
    
    -- Recall/Follow-up appointment types
    ('your-client-id-here', NULL, 'Recall', 'Recall', '2024-01-01', NULL, 'Regular recall appointment'),
    ('your-client-id-here', NULL, 'Follow Up', 'Follow-Up', '2024-01-01', NULL, 'Follow-up visit'),
    ('your-client-id-here', NULL, 'Recare', 'Recall', '2024-01-01', NULL, 'Recare appointment'),
    
    -- Emergency appointment types
    ('your-client-id-here', NULL, 'Emergency', 'Emergency', '2024-01-01', NULL, 'Emergency visit'),
    ('your-client-id-here', NULL, 'Walk-In', 'Emergency', '2024-01-01', NULL, 'Walk-in emergency');
*/

-- ============================================================================
-- Trigger for updated_at timestamp
-- ============================================================================

CREATE OR REPLACE FUNCTION update_appointment_type_mappings_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_appointment_type_mappings_updated_at
    BEFORE UPDATE ON master.appointment_type_mappings
    FOR EACH ROW
    EXECUTE FUNCTION update_appointment_type_mappings_updated_at();

-- ============================================================================
-- Helper View: Active Mappings
-- ============================================================================

CREATE OR REPLACE VIEW master.active_appointment_type_mappings AS
SELECT 
    m.*,
    c.name as client_name,
    p.name as practice_name
FROM master.appointment_type_mappings m
JOIN master.clients c ON m.client_id = c.id
LEFT JOIN master.practices p ON m.practice_id = p.id
WHERE (m.end_date IS NULL OR m.end_date >= CURRENT_DATE);

-- ============================================================================
-- Query Examples
-- ============================================================================

/*
-- Find all active "New Patient" mappings for a client
SELECT * FROM master.active_appointment_type_mappings 
WHERE client_name = 'Wall Street Orthodontics' 
  AND standardized_category = 'New Patient';

-- Find what a specific appointment type maps to
SELECT * FROM master.active_appointment_type_mappings
WHERE source_appointment_type = 'NPE'
  AND CURRENT_DATE BETWEEN start_date AND COALESCE(end_date, CURRENT_DATE);

-- Get mapping for a specific date (historical lookup)
SELECT * FROM master.appointment_type_mappings
WHERE source_appointment_type = 'NPE'
  AND '2024-06-15'::DATE BETWEEN start_date AND COALESCE(end_date, '2099-12-31'::DATE);
*/
