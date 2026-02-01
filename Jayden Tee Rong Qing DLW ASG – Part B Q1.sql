-- =========================================================
-- CONTEXT & SESSION SETUP
-- =========================================================
USE ROLE training_role;
ALTER SESSION SET QUERY_TAG= '(CHIPMUNK)- Assignment: Working with External Table in Data Lake';
USE WAREHOUSE CHIPMUNK_WH;
ALTER WAREHOUSE CHIPMUNK_WH SET WAREHOUSE_SIZE = 'XSMALL';
USE DATABASE CHIPMUNK_DB;
USE SCHEMA RAW;

-- =========================================================
-- FILE FORMAT DEFINITION
-- Handles NULL values and malformed rows safely
-- =========================================================
CREATE OR REPLACE FILE FORMAT RAW.ONTIME_REPORTING_CSV_CHIPMUNK
TYPE = CSV
SKIP_HEADER = 0
COMPRESSION = AUTO
FIELD_DELIMITER = ','
FIELD_OPTIONALLY_ENCLOSED_BY = '"' -- Fields may be enclosed in double quotes
NULL_IF = ('\\N', 'NULL', '')  -- Treat \N, NULL, and empty strings as NULL
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE; -- Prevent query failure if column counts differ between rows

-- =========================================================
-- STORED PROCEDURE
-- Unloads filtered ONTIME_REPORTING data into a
-- year/quarter/month folder structure in the data lake
-- =========================================================
CREATE OR REPLACE PROCEDURE RAW.ASG_UNLOAD_ONTIME_REPORTING_CSV
(
  FOLDER  VARCHAR,   -- Root folder name in the external stage
  YEAR    VARCHAR,   -- Year filter and folder name
  QUARTER VARCHAR,   -- Quarter filter and folder name
  MONTH   VARCHAR    -- Month filter and folder name
)
RETURNS FLOAT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
var unload_sql = `
COPY INTO @raw.datalake_stage/${FOLDER}/year=${YEAR}/quarter=${QUARTER}/month=${MONTH}/ontime_ 
FROM (
  SELECT *
  FROM raw.ONTIME_REPORTING
  WHERE YEAR = '${YEAR}'
    AND QUARTER = '${QUARTER}'
    AND MONTH = '${MONTH}'
)
FILE_FORMAT = (
  TYPE = 'CSV'
  COMPRESSION = NONE
  FIELD_DELIMITER = ','
  SKIP_HEADER = 0
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
)
OVERWRITE = TRUE
`;
snowflake.execute({ sqlText: unload_sql });
return 1;
$$;

-- =========================================================
-- DATA UNLOAD EXECUTION
-- Loading Q3 (Jul–Sep) data across multiple years
-- ========================================================= 

-- Each CALL exports one month of data into its own partition folder

-- ===== 2018 Q3 =====
CALL RAW.ASG_UNLOAD_ONTIME_REPORTING_CSV('CHIPMUNK_ASG','2018','3','7');
CALL RAW.ASG_UNLOAD_ONTIME_REPORTING_CSV('CHIPMUNK_ASG','2018','3','8');
CALL RAW.ASG_UNLOAD_ONTIME_REPORTING_CSV('CHIPMUNK_ASG','2018','3','9');

-- ===== 2019 Q3 =====
CALL RAW.ASG_UNLOAD_ONTIME_REPORTING_CSV('CHIPMUNK_ASG','2019','3','7');
CALL RAW.ASG_UNLOAD_ONTIME_REPORTING_CSV('CHIPMUNK_ASG','2019','3','8');
CALL RAW.ASG_UNLOAD_ONTIME_REPORTING_CSV('CHIPMUNK_ASG','2019','3','9');

-- ===== 2020 Q3 =====
CALL RAW.ASG_UNLOAD_ONTIME_REPORTING_CSV('CHIPMUNK_ASG','2020','3','7');
CALL RAW.ASG_UNLOAD_ONTIME_REPORTING_CSV('CHIPMUNK_ASG','2020','3','8');
CALL RAW.ASG_UNLOAD_ONTIME_REPORTING_CSV('CHIPMUNK_ASG','2020','3','9');

-- ===== 2021 Q3 =====
CALL RAW.ASG_UNLOAD_ONTIME_REPORTING_CSV('CHIPMUNK_ASG','2021','3','7');
CALL RAW.ASG_UNLOAD_ONTIME_REPORTING_CSV('CHIPMUNK_ASG','2021','3','8');
CALL RAW.ASG_UNLOAD_ONTIME_REPORTING_CSV('CHIPMUNK_ASG','2021','3','9');

-- Confirm files exist (note the folder pattern year=/quarter=/month=)
LIST @RAW.DATALAKE_STAGE/CHIPMUNK_ASG;


-- =========================================================
-- EXTERNAL TABLE — NO PARTITION (BASELINE)
-- Full file scan, used for performance comparison
-- ========================================================= 

CREATE OR REPLACE EXTERNAL TABLE RAW.EXTERNAL_ONTIME_ASG_CHIPMUNK_NOPART  (
  year          INT    AS (TRY_CAST(value:c1::string as int)),
  quarter       INT    AS (TRY_CAST(value:c2::string as int)),
  month         INT    AS (TRY_CAST(value:c3::string as int)),
  day_of_month  INT    AS (TRY_CAST(value:c4::string as int)),
  day_of_week   INT    AS (TRY_CAST(value:c5::string as int)),
  fl_date       DATE   AS (TRY_CAST(value:c6::string as date)),
  op_carrier    STRING AS (value:c9::string),
  origin        STRING AS (value:c15::string),
  dest          STRING AS (value:c24::string), 
  arr_delay     INT    AS (TRY_CAST(value:c43::string as int)), 
  cancelled     INT    AS (TRY_CAST(value:c48::string as int))
)
LOCATION = @raw.datalake_stage/CHIPMUNK_ASG
FILE_FORMAT = (FORMAT_NAME = 'RAW.ONTIME_REPORTING_CSV_CHIPMUNK');

-- =========================================================
-- EXTERNAL TABLE — COARSER PARTITION (YEAR + QUARTER)
-- Demonstrates basic partition pruning
-- ========================================================= 
   
CREATE OR REPLACE EXTERNAL TABLE RAW.EXTERNAL_ONTIME_ASG_CHIPMUNK_PART (
    year INT AS (SPLIT_PART(SPLIT_PART(metadata$filename,'year=',2), '/', 1)::int),
    quarter INT AS (SPLIT_PART(SPLIT_PART(metadata$filename,'quarter=',2), '/', 1)::int),
    month INT AS (TRY_CAST(value:c3::string AS INT)), -- Month from data
    day_of_month INT AS (TRY_CAST(value:c4::string AS INT)),
    day_of_week  INT AS (TRY_CAST(value:c5::string AS INT)),
    fl_date      DATE AS (TRY_CAST(value:c6::string AS DATE)),
    op_carrier   STRING AS (value:c9::string),
    origin       STRING AS (value:c15::string),
    dest         STRING AS (value:c24::string),
    arr_delay    INT AS (TRY_CAST(value:c43::string AS INT)),
    cancelled    INT AS (TRY_CAST(value:c48::string AS INT))
)
PARTITION BY (year, quarter)
LOCATION = @raw.datalake_stage/CHIPMUNK_ASG
FILE_FORMAT = (FORMAT_NAME = 'RAW.ONTIME_REPORTING_CSV_CHIPMUNK');


-- =========================================================
-- EXTERNAL TABLE — GRANULAR PARTITION (YEAR + QUARTER + MONTH)
-- Demonstrates granular partition pruning
-- ========================================================= 
CREATE OR REPLACE EXTERNAL TABLE RAW.EXTERNAL_ONTIME_ASG_CHIPMUNK_GRAN_PART (
    year        INT AS (SPLIT_PART(SPLIT_PART(metadata$filename, 'year=', 2), '/', 1)::int), -- Year extracted from folder path
    quarter     INT AS (SPLIT_PART(SPLIT_PART(metadata$filename, 'quarter=', 2), '/', 1)::int), -- Quarter extracted from folder path
    month       INT AS (SPLIT_PART(SPLIT_PART(metadata$filename, 'month=', 2), '/', 1)::int), -- Month extracted from folder path
    day_of_month INT    AS (TRY_CAST(value:c4::string as int)),
    day_of_week  INT    AS (TRY_CAST(value:c5::string as int)),
    fl_date      DATE   AS (TRY_CAST(value:c6::string as date)),
    op_carrier   STRING AS (value:c9::string),
    origin       STRING AS (value:c15::string),
    dest         STRING AS (value:c24::string),
    arr_delay    INT    AS (TRY_CAST(value:c43::string as int)),
    cancelled    INT    AS (TRY_CAST(value:c48::string as int))
)
PARTITION BY (year, quarter, month) -- Most granular partitioning for maximum partition pruning
LOCATION = @raw.datalake_stage/CHIPMUNK_ASG/
FILE_FORMAT = (FORMAT_NAME = 'RAW.ONTIME_REPORTING_CSV_CHIPMUNK');


-- =========================================================
-- PERFORMANCE COMPARISON QUERIES (PARTITION)
-- Cached results disabled for fair comparison
-- ========================================================= 
-- Ensure queries are executed fresh and not served from cache
ALTER SESSION SET USE_CACHED_RESULT = FALSE;

-- No partition
SELECT year, quarter, month, origin, dest, op_carrier, arr_delay
FROM RAW.EXTERNAL_ONTIME_ASG_CHIPMUNK_NOPART
WHERE year = 2019 AND quarter = 3 AND month = 8 AND cancelled = 0

-- Partitioned 
SELECT year, quarter, month, origin, dest, op_carrier, arr_delay
FROM RAW.EXTERNAL_ONTIME_ASG_CHIPMUNK_PART
WHERE year = 2019 AND quarter = 3 AND month = 8 AND cancelled = 0;


-- More granular partitioning
SELECT year, quarter, month, origin, dest, op_carrier, arr_delay
FROM RAW.EXTERNAL_ONTIME_ASG_CHIPMUNK_GRAN_PART
WHERE year = 2019 AND quarter = 3 AND month = 8 AND cancelled = 0


-- =========================================================
-- MODELED LAYER — VIEW & MATERIALIZED VIEW
-- =========================================================

USE SCHEMA MODELED;

CREATE OR REPLACE VIEW MODELED.OTP_FY2019_SEA_MARKETS_CHIPMUNK_ASG
-- Logical view applying business filters
(year, quarter, op_carrier, dest, origin, arr_delay) AS
(
  SELECT year, quarter, op_carrier, dest, origin, arr_delay
  FROM RAW.EXTERNAL_ONTIME_ASG_CHIPMUNK_PART
  WHERE origin = 'SEA'
    AND dest IN ('SFO','LAX','ORD','JFK','SLC','HNL','DEN','BOS','IAH','ATL')
    AND cancelled = 0
    AND year = 2019
    AND quarter = 3
    AND arr_delay IS NOT NULL
);

CREATE OR REPLACE MATERIALIZED VIEW MODELED.AVG_OTP_FY2019_SEA_MARKETS_CHIPMUNK_ASG AS
-- Pre-aggregated result set for faster analytical queries
(
  SELECT dest, op_carrier, AVG(arr_delay) AS avg_arr_delay
  FROM RAW.EXTERNAL_ONTIME_ASG_CHIPMUNK_PART
  WHERE origin = 'SEA'
    AND dest IN ('SFO','LAX','ORD','JFK','SLC','HNL','DEN','BOS','IAH','ATL')
    AND cancelled = 0
    AND year = 2019
    AND quarter = 3
    AND arr_delay IS NOT NULL
  GROUP BY 1,2
);

ALTER SESSION SET USE_CACHED_RESULT = FALSE;

-- =========================================================
-- PERFORMANCE COMPARISON QUERIES (MODELED LAYER)
-- ========================================================= 
-- Aggregation via standard view
SELECT dest, op_carrier, AVG(arr_delay) AS avg_arr_delay
FROM MODELED.OTP_FY2019_SEA_MARKETS_CHIPMUNK_ASG
GROUP BY 1,2
ORDER BY 1,2;

-- Aggregation via materialized view
SELECT dest, op_carrier, avg_arr_delay
FROM MODELED.AVG_OTP_FY2019_SEA_MARKETS_CHIPMUNK_ASG
ORDER BY 1,2;


-- Suspend
ALTER SESSION UNSET QUERY_TAG;
ALTER WAREHOUSE CHIPMUNK_WH SET WAREHOUSE_SIZE = 'XSMALL';
ALTER WAREHOUSE CHIPMUNK_WH SUSPEND;
