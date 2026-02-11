-- 1. Create the Gold Schema first
CREATE SCHEMA IF NOT EXISTS ENERGY_PROJECT.GOLD;

-- 2. Create the analytics view in that schema
CREATE OR REPLACE VIEW ENERGY_PROJECT.GOLD.DAILY_CARBON_METRICS AS
SELECT 
    DATE(start_time) as report_date,
    intensity_level,
    ROUND(AVG(actual_intensity), 2) as avg_actual_intensity,
    COUNT(*) as reading_count
FROM ENERGY_PROJECT.SILVER.CLEAN_ENERGY_DATA
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;

-- 3. THE GRAND FINALE: View your production-ready report!
SELECT * FROM ENERGY_PROJECT.GOLD.DAILY_CARBON_METRICS;