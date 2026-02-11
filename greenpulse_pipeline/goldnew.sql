CREATE OR REPLACE VIEW ENERGY_PROJECT.GOLD.STRATEGIC_DEEP_ANALYSIS AS
WITH BASE_DATA AS (
    SELECT 
        start_time,
        intensity_level,
        actual_intensity,
        forecast_intensity,
        -- 1. Forecast Bias (Dirtier or Cleaner than predicted?)
        (actual_intensity - forecast_intensity) as forecast_error,
        -- 2. Forecast Accuracy % (100% is perfect prediction)
        (1 - ABS(actual_intensity - forecast_intensity) / NULLIF(actual_intensity, 0)) * 100 as prediction_accuracy_score
    FROM ENERGY_PROJECT.SILVER.CLEAN_ENERGY_DATA
)
SELECT 
    *,
    -- 3. Velocity of Change (How fast is intensity rising/falling vs previous reading?)
    actual_intensity - LAG(actual_intensity) OVER (ORDER BY start_time) as intensity_velocity,
    -- 4. % Change vs Previous Hour (Standard financial metric for "Volatility")
    ((actual_intensity - LAG(actual_intensity) OVER (ORDER BY start_time)) / 
        NULLIF(LAG(actual_intensity) OVER (ORDER BY start_time), 0)) * 100 as percentage_change,
    -- 5. Peak Identification (Is this the highest reading in the last 4 hours?)
    CASE 
        WHEN actual_intensity = MAX(actual_intensity) OVER (ORDER BY start_time ROWS BETWEEN 8 PRECEDING AND CURRENT ROW) 
        THEN 'PEAK' ELSE 'NORMAL' 
    END as peak_status,
    -- 6. Grid Stability Metric (Standard Deviation of the last 10 readings)
    STDDEV(actual_intensity) OVER (ORDER BY start_time ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) as grid_stability_score
FROM BASE_DATA;