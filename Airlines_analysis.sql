-- Creating database
CREATE DATABASE airline_analysis;

USE airline_analysis;

-- Creating tables
CREATE TABLE aviation_accidents (
    event_id VARCHAR(50),
    investigation_type VARCHAR(50),
    accident_number VARCHAR(50),
    event_date VARCHAR(50),
    location VARCHAR(500),
    country VARCHAR(200),
    latitude VARCHAR(50),
    longitude VARCHAR(50),
    airport_code VARCHAR(10),
    airport_name VARCHAR(500),
    injury_severity VARCHAR(50),
    aircraft_damage VARCHAR(50),
    aircraft_category VARCHAR(50),
    registration_number VARCHAR(50),
    make VARCHAR(500),
    model VARCHAR(100),
    amateur_built VARCHAR(10),
    number_of_engines VARCHAR(10),
    engine_type VARCHAR(50),
    far_description VARCHAR(300),
    schedule VARCHAR(50),
    purpose_of_flight VARCHAR(100),
    air_carrier VARCHAR(600),
    total_fatalities VARCHAR(10),
    total_serious_injuries varchar(10),
    total_minor_injuries varchar(10),
	total_uninjured VARCHAR(10),
    weather_condition VARCHAR(50),
    broad_phase_of_flight VARCHAR(50),
    report_status VARCHAR(50),
    publication_date VARCHAR(50)
);

-- dataset https://www.kaggle.com/datasets/grumpylew123/dataset

-- Loading  data into database
LOAD DATA INFILE 'C:\Users\tejes\Desktop\Data Analyst\Portfolio projects\SQL\Airlines analysis\AviationData.csv'
INTO TABLE aviation_accidents
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


SELECT * FROM aviation_accidents WHERE event_id = '20001218X45444';

SELECT COUNT(*) FROM aviation_accidents;

-- ============================================
-- BASIC DATA EXPLORATION
-- ============================================

-- Checking total records
SELECT COUNT(*) as total_records FROM aviation_accidents;

-- Checking data types and structure
DESCRIBE aviation_accidents;


-- Checking for duplicates on key fields
SELECT 
    event_id,
    COUNT(*) as duplicate_count
FROM aviation_accidents
GROUP BY event_id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 10;


SELECT a.*
FROM aviation_accidents a
JOIN (
    SELECT 
        event_id, investigation_type, accident_number, event_date, location, country,
        latitude, longitude, airport_code, airport_name, injury_severity, aircraft_damage,
        aircraft_category, registration_number, make, model, amateur_built, number_of_engines,
        engine_type, far_description, schedule, purpose_of_flight, air_carrier,
        total_fatalities, total_serious_injuries, total_minor_injuries, total_uninjured,
        weather_condition, broad_phase_of_flight, report_status, publication_date
    FROM aviation_accidents
    GROUP BY 
        event_id, investigation_type, accident_number, event_date, location, country,
        latitude, longitude, airport_code, airport_name, injury_severity, aircraft_damage,
        aircraft_category, registration_number, make, model, amateur_built, number_of_engines,
        engine_type, far_description, schedule, purpose_of_flight, air_carrier,
        total_fatalities, total_serious_injuries, total_minor_injuries, total_uninjured,
        weather_condition, broad_phase_of_flight, report_status, publication_date
    HAVING COUNT(*) > 1
) d
ON a.event_id = d.event_id
ORDER BY a.event_id 
LIMIT 10;

-- Removing duplicates

SELECT COUNT(*) AS total_rows,
       COUNT(DISTINCT CONCAT_WS('|',
           event_id, investigation_type, accident_number, event_date, location, country,
           latitude, longitude, airport_code, airport_name, injury_severity, aircraft_damage,
           aircraft_category, registration_number, make, model, amateur_built, number_of_engines,
           engine_type, far_description, schedule, purpose_of_flight, air_carrier,
           total_fatalities, total_serious_injuries, total_minor_injuries, total_uninjured,
           weather_condition, broad_phase_of_flight, report_status, publication_date
       )) AS distinct_rows
FROM aviation_accidents;

-- Creating new table to insert distinct records

CREATE TABLE aviation_accidents_new LIKE aviation_accidents;

INSERT INTO aviation_accidents_new
SELECT DISTINCT * FROM aviation_accidents;

SELECT 
    (SELECT COUNT(*) FROM aviation_accidents) as old_count,
    (SELECT COUNT(*) FROM aviation_accidents_new) as new_count;

RENAME TABLE aviation_accidents TO aviation_accidents_old;
RENAME TABLE aviation_accidents_new TO aviation_accidents;

SELECT COUNT(*) FROM aviation_accidents;

-- Dropping old table
DROP TABLE aviation_accidents_old;



-- ============================================
-- MISSING VALUE ANALYSIS
-- ============================================

-- Checking missing values percentage for each column
SELECT 
    'event_id' as column_name,
    COUNT(*) as total_rows,
    SUM(CASE WHEN event_id IS NULL OR event_id = '' THEN 1 ELSE 0 END) as missing_count,
    ROUND((SUM(CASE WHEN event_id IS NULL OR event_id = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as missing_percentage
FROM aviation_accidents
UNION ALL
SELECT 
    'event_date',
    COUNT(*),
    SUM(CASE WHEN event_date IS NULL OR event_date = '' THEN 1 ELSE 0 END),
    ROUND((SUM(CASE WHEN event_date IS NULL OR event_date = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2)
FROM aviation_accidents
UNION ALL
SELECT 
    'location',
    COUNT(*),
    SUM(CASE WHEN location IS NULL OR location = '' THEN 1 ELSE 0 END),
    ROUND((SUM(CASE WHEN location IS NULL OR location = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2)
FROM aviation_accidents
UNION ALL
SELECT 
    'country',
    COUNT(*),
    SUM(CASE WHEN country IS NULL OR country = '' THEN 1 ELSE 0 END),
    ROUND((SUM(CASE WHEN country IS NULL OR country = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2)
FROM aviation_accidents
UNION ALL
SELECT 
    'total_fatalities',
    COUNT(*),
    SUM(CASE WHEN total_fatalities IS NULL OR total_fatalities = '' THEN 1 ELSE 0 END),
    ROUND((SUM(CASE WHEN total_fatalities IS NULL OR total_fatalities = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2)
FROM aviation_accidents
ORDER BY missing_percentage DESC;

-- ============================================
-- DATA CLEANING - DATE COLUMNS
-- ============================================

-- Checking current date formats
SELECT 
    event_date,
    LENGTH(event_date) as date_length,
    COUNT(*) as count
FROM aviation_accidents
WHERE event_date IS NOT NULL AND event_date != ''
GROUP BY event_date, LENGTH(event_date)
ORDER BY count DESC
LIMIT 10;

-- Creating cleaned date columns
ALTER TABLE aviation_accidents 
ADD COLUMN event_date_clean DATE,
ADD COLUMN publication_date_clean DATE;

DESCRIBE aviation_accidents;

-- Fixing event_date - handles various formats

SELECT 
    event_date,
    LENGTH(event_date) as date_length,
    COUNT(*) as count
FROM aviation_accidents
WHERE event_date IS NOT NULL AND event_date != ''
GROUP BY event_date, LENGTH(event_date)
ORDER BY count DESC
LIMIT 20;


-- Updating the query to handle multiple formats
UPDATE aviation_accidents 
SET event_date_clean = 
    CASE 
        
        WHEN event_date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN STR_TO_DATE(event_date, '%d-%m-%Y')
        
       
        WHEN event_date LIKE '%/%/%' THEN STR_TO_DATE(event_date, '%m/%d/%Y')
        
   
        WHEN event_date LIKE '%-%-%' AND event_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
            THEN STR_TO_DATE(event_date, '%Y-%m-%d')
        
        
        WHEN event_date REGEXP '^[0-9]{8}$' THEN STR_TO_DATE(event_date, '%Y%m%d')
        
   
        WHEN event_date REGEXP '^[0-9]{4}/[0-9]{2}/[0-9]{2}$' THEN STR_TO_DATE(event_date, '%Y/%m/%d')
        
        
        WHEN event_date REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN STR_TO_DATE(event_date, '%d/%m/%Y')
        
        ELSE NULL
    END
WHERE event_date IS NOT NULL AND event_date != '';





-- Fixing publication_date

SELECT 
    publication_date,
    LENGTH(publication_date) as date_length,
    COUNT(*) as count
FROM aviation_accidents
WHERE publication_date IS NOT NULL AND publication_date != ''
GROUP BY publication_date, LENGTH(publication_date)
ORDER BY count DESC
LIMIT 20;


UPDATE aviation_accidents 
SET publication_date_clean = 
    CASE 
        
        WHEN publication_date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN STR_TO_DATE(publication_date, '%d-%m-%Y')
        
        
        WHEN publication_date LIKE '%/%/%' THEN STR_TO_DATE(publication_date, '%m/%d/%Y')
        

        WHEN publication_date LIKE '%-%-%' AND publication_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
            THEN STR_TO_DATE(publication_date, '%Y-%m-%d')
        
        
        WHEN publication_date REGEXP '^[0-9]{8}$' THEN STR_TO_DATE(publication_date, '%Y%m%d')
        
        
        WHEN publication_date REGEXP '^[0-9]{4}/[0-9]{2}/[0-9]{2}$' THEN STR_TO_DATE(publication_date, '%Y/%m/%d')
        
        
        WHEN publication_date REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN STR_TO_DATE(publication_date, '%d/%m/%Y')
        
        ELSE NULL
    END
WHERE publication_date IS NOT NULL AND publication_date != '';




-- ============================================
-- DATA CLEANING - NUMERIC COLUMNS
-- ============================================

-- Converting text numeric columns to proper numeric types
ALTER TABLE aviation_accidents 
ADD COLUMN latitude_clean DECIMAL(10,6),
ADD COLUMN longitude_clean DECIMAL(10,6),
ADD COLUMN total_fatalities_clean INT,
ADD COLUMN total_serious_injuries_clean INT,
ADD COLUMN total_minor_injuries_clean INT,
ADD COLUMN total_uninjured_clean INT,
ADD COLUMN number_of_engines_clean INT;


UPDATE aviation_accidents 
SET latitude_clean = 
    CASE 
        WHEN latitude REGEXP '^-?[0-9]+(\.[0-9]+)?$' THEN CAST(latitude AS DECIMAL(10,6))
        WHEN latitude LIKE '%N' THEN CAST(REPLACE(latitude, 'N', '') AS DECIMAL(10,6))
        WHEN latitude LIKE '%S' THEN -CAST(REPLACE(latitude, 'S', '') AS DECIMAL(10,6))
        ELSE NULL
    END
WHERE latitude IS NOT NULL AND latitude != '';


UPDATE aviation_accidents 
SET longitude_clean = 
    CASE 
        WHEN longitude REGEXP '^-?[0-9]+(\.[0-9]+)?$' THEN CAST(longitude AS DECIMAL(10,6))
        WHEN longitude LIKE '%E' THEN CAST(REPLACE(longitude, 'E', '') AS DECIMAL(10,6))
        WHEN longitude LIKE '%W' THEN -CAST(REPLACE(longitude, 'W', '') AS DECIMAL(10,6))
        ELSE NULL
    END
WHERE longitude IS NOT NULL AND longitude != '';


UPDATE aviation_accidents 
SET total_fatalities_clean = 
    CASE 
        WHEN total_fatalities REGEXP '^[0-9]+$' THEN CAST(total_fatalities AS UNSIGNED)
        WHEN total_fatalities = 'N/A' THEN 0
        ELSE 0
    END,
total_serious_injuries_clean = 
    CASE 
        WHEN total_serious_injuries REGEXP '^[0-9]+$' THEN CAST(total_serious_injuries AS UNSIGNED)
        WHEN total_serious_injuries = 'N/A' THEN 0
        ELSE 0
    END,
total_minor_injuries_clean = 
    CASE 
        WHEN total_minor_injuries REGEXP '^[0-9]+$' THEN CAST(total_minor_injuries AS UNSIGNED)
        WHEN total_minor_injuries = 'N/A' THEN 0
        ELSE 0
    END,
total_uninjured_clean = 
    CASE 
        WHEN total_uninjured REGEXP '^[0-9]+$' THEN CAST(total_uninjured AS UNSIGNED)
        WHEN total_uninjured = 'N/A' THEN 0
        ELSE 0
    END,
number_of_engines_clean = 
    CASE 
        WHEN number_of_engines REGEXP '^[0-9]+$' THEN CAST(number_of_engines AS UNSIGNED)
        ELSE NULL
    END;

-- ============================================
-- TEXT DATA CLEANING
-- ============================================

-- Cleaning location field
UPDATE aviation_accidents 
SET location = TRIM(BOTH ' ' FROM location),
    country = TRIM(BOTH ' ' FROM country),
    airport_name = TRIM(BOTH ' ' FROM airport_name),
    make = TRIM(BOTH ' ' FROM make),
    model = TRIM(BOTH ' ' FROM model);


UPDATE aviation_accidents
SET country = 
    CASE 
        WHEN UPPER(country) LIKE '%USA%' OR UPPER(country) LIKE '%UNITED STATES%' THEN 'United States'
        WHEN UPPER(country) LIKE '%UK%' OR UPPER(country) LIKE '%UNITED KINGDOM%' THEN 'United Kingdom'
        WHEN UPPER(country) LIKE '%UAE%' THEN 'United Arab Emirates'
        WHEN UPPER(country) LIKE '%RUSSIA%' THEN 'Russia'
        WHEN UPPER(country) LIKE '%CANADA%' THEN 'Canada'
        WHEN UPPER(country) LIKE '%AUSTRALIA%' THEN 'Australia'
        ELSE CONCAT(UCASE(LEFT(country,1)), LCASE(SUBSTRING(country,2)))
    END
WHERE country IS NOT NULL AND country != '';


-- ============================================
-- EXPLORATORY DATA ANALYSIS
-- ============================================

-- Accidents by Year
SELECT 
    YEAR(event_date_clean) as accident_year,
    COUNT(*) as accident_count
FROM aviation_accidents
WHERE event_date_clean IS NOT NULL
GROUP BY YEAR(event_date_clean)
ORDER BY accident_year;

-- Accidents by Month
SELECT 
    MONTHNAME(event_date_clean) as month_name,
    COUNT(*) as accident_count
FROM aviation_accidents
WHERE event_date_clean IS NOT NULL
GROUP BY MONTH(event_date_clean), month_name
ORDER BY MONTH(event_date_clean);

-- Top 10 Countries with Most Accidents
SELECT 
    country,
    COUNT(*) as accident_count,
    ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM aviation_accidents WHERE country IS NOT NULL AND country != '')), 2) as percentage
FROM aviation_accidents
WHERE country IS NOT NULL AND country != ''
GROUP BY country
ORDER BY accident_count DESC
LIMIT 10;

-- Injury Severity Distribution
SELECT 
    injury_severity,
    COUNT(*) as count,
    SUM(total_fatalities_clean) as total_fatalities,
    SUM(total_serious_injuries_clean) as total_serious_injuries,
    SUM(total_minor_injuries_clean) as total_minor_injuries
FROM aviation_accidents
WHERE injury_severity IS NOT NULL AND injury_severity != ''
GROUP BY injury_severity
ORDER BY count DESC;

-- Aircraft Damage Analysis
SELECT 
    aircraft_damage,
    COUNT(*) as accident_count,
    ROUND(AVG(total_fatalities_clean), 2) as avg_fatalities,
    ROUND(AVG(total_serious_injuries_clean), 2) as avg_serious_injuries
FROM aviation_accidents
WHERE aircraft_damage IS NOT NULL AND aircraft_damage != ''
GROUP BY aircraft_damage
ORDER BY accident_count DESC;

-- Weather Condition Impact
SELECT 
    weather_condition,
    COUNT(*) as accident_count,
    SUM(total_fatalities_clean) as total_fatalities,
    ROUND(AVG(total_fatalities_clean), 2) as avg_fatalities_per_accident
FROM aviation_accidents
WHERE weather_condition IS NOT NULL AND weather_condition != ''
GROUP BY weather_condition
ORDER BY accident_count DESC;

-- Phase of Flight Analysis
SELECT 
    broad_phase_of_flight,
    COUNT(*) as accident_count,
    SUM(total_fatalities_clean) as total_fatalities,
    ROUND((SUM(total_fatalities_clean) * 100.0 / NULLIF(SUM(SUM(total_fatalities_clean)) OVER(), 0)), 2) as fatality_percentage
FROM aviation_accidents
WHERE broad_phase_of_flight IS NOT NULL AND broad_phase_of_flight != ''
GROUP BY broad_phase_of_flight
ORDER BY total_fatalities DESC;

-- ============================================
-- AIRCRAFT MANUFACTURER ANALYSIS
-- ============================================

-- Top 10 Aircraft Manufacturers by Accident Count
SELECT 
    make,
    COUNT(*) as accident_count,
    SUM(total_fatalities_clean) as total_fatalities,
    ROUND(AVG(total_fatalities_clean), 2) as avg_fatalities_per_accident
FROM aviation_accidents
WHERE make IS NOT NULL AND make != ''
GROUP BY make
ORDER BY accident_count DESC
LIMIT 10;

-- Most Dangerous Aircraft Models
SELECT 
    make,
    model,
    COUNT(*) as accident_count,
    SUM(total_fatalities_clean) as total_fatalities,
    ROUND(AVG(total_fatalities_clean), 2) as avg_fatalities_per_accident
FROM aviation_accidents
WHERE make IS NOT NULL AND make != '' AND model IS NOT NULL AND model != ''
GROUP BY make, model
HAVING COUNT(*) >= 5
ORDER BY avg_fatalities_per_accident DESC
LIMIT 10;

-- ============================================
-- TIME TREND ANALYSIS
-- ============================================

-- Accidents and Fatalities by Year
SELECT 
    YEAR(event_date_clean) as year,
    COUNT(*) as accident_count,
    SUM(total_fatalities_clean) as total_fatalities,
    ROUND(AVG(total_fatalities_clean), 2) as avg_fatalities_per_accident,
    SUM(total_serious_injuries_clean) as total_serious_injuries
FROM aviation_accidents
WHERE event_date_clean IS NOT NULL
    AND YEAR(event_date_clean) BETWEEN 1980 AND 2023
GROUP BY YEAR(event_date_clean)
ORDER BY year;

-- 5-Year Moving Average of Accidents
WITH yearly_data AS (
    SELECT 
        YEAR(event_date_clean) as year,
        COUNT(*) as accident_count
    FROM aviation_accidents
    WHERE event_date_clean IS NOT NULL
    GROUP BY YEAR(event_date_clean)
)
SELECT 
    year,
    accident_count,
    ROUND(AVG(accident_count) OVER(ORDER BY year ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING), 2) as moving_avg_5yr
FROM yearly_data
ORDER BY year;

-- ============================================
-- GEOGRAPHICAL ANALYSIS
-- ============================================

-- Accidents by Country with fatality rates
SELECT 
    country,
    COUNT(*) as accident_count,
    SUM(total_fatalities_clean) as total_fatalities,
    ROUND(SUM(total_fatalities_clean) * 1.0 / COUNT(*), 2) as fatalities_per_accident,
    SUM(total_serious_injuries_clean) as total_serious_injuries
FROM aviation_accidents
WHERE country IS NOT NULL AND country != ''
GROUP BY country
HAVING COUNT(*) >= 10
ORDER BY fatalities_per_accident DESC
LIMIT 15;

-- Checking for invalid coordinates
SELECT 
    COUNT(*) as total_records,
    SUM(CASE WHEN latitude_clean IS NULL THEN 1 ELSE 0 END) as missing_lat,
    SUM(CASE WHEN longitude_clean IS NULL THEN 1 ELSE 0 END) as missing_lon,
    SUM(CASE WHEN ABS(latitude_clean) > 90 THEN 1 ELSE 0 END) as invalid_lat,
    SUM(CASE WHEN ABS(longitude_clean) > 180 THEN 1 ELSE 0 END) as invalid_lon
FROM aviation_accidents;

-- Fixing invalid coordinates
UPDATE aviation_accidents 
SET latitude_clean = NULL 
WHERE ABS(latitude_clean) > 90;

UPDATE aviation_accidents 
SET longitude_clean = NULL 
WHERE ABS(longitude_clean) > 180;

-- ============================================
-- DATA QUALITY CHECKS
-- ============================================

-- Checking for data inconsistencies
SELECT 
    'Total Fatalities > 0 but Injury Severity not Fatal' as check_type,
    COUNT(*) as record_count
FROM aviation_accidents
WHERE total_fatalities_clean > 0 
    AND injury_severity NOT LIKE '%Fatal%'
UNION ALL
SELECT 
    'No injuries but aircraft is Destroyed',
    COUNT(*)
FROM aviation_accidents
WHERE total_fatalities_clean = 0 
    AND total_serious_injuries_clean = 0 
    AND total_minor_injuries_clean = 0
    AND aircraft_damage = 'Destroyed'
UNION ALL
SELECT 
    'Event date after publication date',
    COUNT(*)
FROM aviation_accidents
WHERE event_date_clean > publication_date_clean
    AND event_date_clean IS NOT NULL 
    AND publication_date_clean IS NOT NULL;

-- Checking for outlier values
WITH stats AS (
    SELECT 
        AVG(total_fatalities_clean) as avg_fatalities,
        STDDEV(total_fatalities_clean) as std_fatalities
    FROM aviation_accidents
    WHERE total_fatalities_clean IS NOT NULL
)
SELECT 
    COUNT(*) as outlier_count
FROM aviation_accidents a, stats s
WHERE a.total_fatalities_clean > s.avg_fatalities + (3 * s.std_fatalities);

-- ============================================
-- CLEANED VIEW FOR ANALYSIS
-- ============================================
 
CREATE OR REPLACE VIEW aviation_accidents_clean AS
SELECT 

    TRIM(event_id) as event_id,
    TRIM(investigation_type) as investigation_type,
    TRIM(accident_number) as accident_number,
    event_date_clean as event_date,
    TRIM(location) as location,
    TRIM(country) as country,
    latitude_clean as latitude,
    longitude_clean as longitude,
    TRIM(airport_code) as airport_code,
    TRIM(airport_name) as airport_name,
    TRIM(injury_severity) as injury_severity,
    TRIM(aircraft_damage) as aircraft_damage,
    TRIM(aircraft_category) as aircraft_category,
    TRIM(registration_number) as registration_number,
    TRIM(make) as make,
    TRIM(model) as model,
    TRIM(amateur_built) as amateur_built,
    number_of_engines_clean as number_of_engines,
    TRIM(engine_type) as engine_type,
    TRIM(far_description) as far_description,
    TRIM(schedule) as schedule,
    TRIM(purpose_of_flight) as purpose_of_flight,
    TRIM(air_carrier) as air_carrier,
    total_fatalities_clean as total_fatalities,
    total_serious_injuries_clean as total_serious_injuries,
    total_minor_injuries_clean as total_minor_injuries,
    total_uninjured_clean as total_uninjured,
    TRIM(weather_condition) as weather_condition,
    TRIM(broad_phase_of_flight) as broad_phase_of_flight,
    TRIM(report_status) as report_status,
    publication_date_clean as publication_date,
    
    CASE 
        WHEN total_fatalities_clean > 0 THEN 'Fatal'
        WHEN total_serious_injuries_clean > 0 THEN 'Serious'
        WHEN total_minor_injuries_clean > 0 THEN 'Minor'
        ELSE 'No Injury'
    END as accident_severity_category,
    CASE 
        WHEN total_fatalities_clean + total_serious_injuries_clean + total_minor_injuries_clean + total_uninjured_clean > 0 
        THEN total_fatalities_clean + total_serious_injuries_clean + total_minor_injuries_clean + total_uninjured_clean
        ELSE NULL
    END as total_occupants,
    CASE 
        WHEN total_fatalities_clean > 0 THEN total_fatalities_clean * 1.0 / 
            NULLIF(total_fatalities_clean + total_serious_injuries_clean + total_minor_injuries_clean + total_uninjured_clean, 0)
        ELSE 0
    END as fatality_rate,
    
    YEAR(event_date_clean) as event_year,
    MONTH(event_date_clean) as event_month,
    QUARTER(event_date_clean) as event_quarter,
    DAYOFWEEK(event_date_clean) as event_day_of_week
FROM aviation_accidents
WHERE event_date_clean IS NOT NULL; 

-- ============================================
-- FINAL SUMMARY STATISTICS
-- ============================================

SELECT 
    'Total Records' as metric,
    COUNT(*) as value
FROM aviation_accidents_clean
UNION ALL
SELECT 
    'Total Fatal Accidents',
    COUNT(CASE WHEN total_fatalities > 0 THEN 1 END)
FROM aviation_accidents_clean
UNION ALL
SELECT 
    'Total Fatalities',
    SUM(total_fatalities)
FROM aviation_accidents_clean
UNION ALL
SELECT 
    'Total Serious Injuries',
    SUM(total_serious_injuries)
FROM aviation_accidents_clean
UNION ALL
SELECT 
    'Average Fatalities per Accident',
    ROUND(AVG(total_fatalities), 2)
FROM aviation_accidents_clean
WHERE total_fatalities > 0
UNION ALL
SELECT 
    'Most Common Country',
    (SELECT country FROM aviation_accidents_clean GROUP BY country ORDER BY COUNT(*) DESC LIMIT 1)
FROM DUAL
UNION ALL
SELECT 
    'Date Range',
    CONCAT(MIN(event_date), ' to ', MAX(event_date))
FROM aviation_accidents_clean;

-- ============================================
-- EXPORT READY QUERIES FOR REPORTS/VISUALIZATION
-- ============================================

-- Yearly accident trends
SELECT 
    event_year,
    COUNT(*) as accident_count,
    SUM(total_fatalities) as total_fatalities,
    ROUND(AVG(total_fatalities), 2) as avg_fatalities_per_accident
FROM aviation_accidents_clean
GROUP BY event_year
ORDER BY event_year;

-- Top 10 most dangerous aircraft models
SELECT 
    make,
    model,
    COUNT(*) as accident_count,
    SUM(total_fatalities) as total_fatalities,
    ROUND(AVG(total_fatalities), 2) as avg_fatalities_per_accident
FROM aviation_accidents_clean
WHERE make IS NOT NULL AND make != ''
GROUP BY make, model
HAVING COUNT(*) >= 5
ORDER BY total_fatalities DESC
LIMIT 10;

-- Weather impact analysis
SELECT 
    weather_condition,
    COUNT(*) as accident_count,
    SUM(total_fatalities) as total_fatalities,
    ROUND(AVG(total_fatalities), 2) as avg_fatalities,
    ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM aviation_accidents_clean)), 2) as percentage_of_total
FROM aviation_accidents_clean
WHERE weather_condition IS NOT NULL
GROUP BY weather_condition
ORDER BY accident_count DESC;

-- Phase of flight risk analysis
SELECT 
    broad_phase_of_flight,
    COUNT(*) as accident_count,
    SUM(total_fatalities) as total_fatalities,
    ROUND(AVG(total_fatalities), 2) as avg_fatalities,
    ROUND((SUM(total_fatalities) * 100.0 / (SELECT SUM(total_fatalities) FROM aviation_accidents_clean)), 2) as fatality_percentage
FROM aviation_accidents_clean
WHERE broad_phase_of_flight IS NOT NULL
GROUP BY broad_phase_of_flight
ORDER BY total_fatalities DESC;

-- Country analysis (for choropleth map)
SELECT 
    country,
    COUNT(*) as accident_count,
    SUM(total_fatalities) as total_fatalities,
    ROUND(AVG(total_fatalities), 2) as avg_fatalities_per_accident,
    SUM(total_serious_injuries) as total_serious_injuries
FROM aviation_accidents_clean
WHERE country IS NOT NULL
GROUP BY country
ORDER BY total_fatalities DESC;





