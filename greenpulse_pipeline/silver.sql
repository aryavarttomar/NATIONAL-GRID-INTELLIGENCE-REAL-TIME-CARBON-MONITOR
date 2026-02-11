USE DATABASE ENERGY_PROJECT;
USE SCHEMA SILVER;

-- Transform the raw JSON into the structured Silver table
CREATE OR REPLACE TABLE CLEAN_ENERGY_DATA AS
SELECT 
    raw_json:from::TIMESTAMP_NTZ as start_time,
    raw_json:to::TIMESTAMP_NTZ as end_time,
    raw_json:intensity.forecast::INT as forecast_intensity,
    raw_json:intensity.actual::INT as actual_intensity,
    raw_json:intensity.index::STRING as intensity_level,
    ingested_at
FROM ENERGY_PROJECT.BRONZE.RAW_INTENSITY;

-- Verify the Silver table has data
SELECT * FROM CLEAN_ENERGY_DATA LIMIT 10;