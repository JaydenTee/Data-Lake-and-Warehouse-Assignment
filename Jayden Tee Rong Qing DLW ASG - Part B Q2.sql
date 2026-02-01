-- ============================================================
-- DLW Part B - Q2 (CHIPMUNK)
-- Practical-aligned pipeline:
--   DIRECTORY TABLE + STAGE STREAM + PYTHON UDF + TASK DAG (T1 -> T2 -> T3)
--
-- Task responsibilities :
--   T1: Refresh stage + insert RAW FILE METADATA into RAW table
--   T2: Parse PDFs (Python UDF) + insert EXTRACTED TEXT into PROCESSED table
--   T3: Build final MODELED views joining:
--       - PROCESSED PDF data
--       - Part A curated dataset
--       - Part A external table stats (PART vs NOPART) for pruning comparison
-- ============================================================


-- ============================================================
-- 0) CONTEXT / SESSION SETUP
-- ============================================================
USE ROLE TRAINING_ROLE;                                         
USE DATABASE CHIPMUNK_DB;                                      
USE SCHEMA RAW;                                                
USE WAREHOUSE CHIPMUNK_WH;                                      
ALTER SESSION SET QUERY_TAG =
  '(CHIPMUNK)- Assignment: Automating Transformations with Unstructured Data Using UDFs, Streams, and Tasks'; 
ALTER SESSION SET USE_CACHED_RESULT = FALSE;                   


-- ============================================================
-- 1) HARD RESET (SAFE RE-RUN)
--    Goal: drop old tasks/streams/tables/views/stage so script can re-run cleanly
-- ============================================================

-- ---- Suspend tasks (if they exist) ----
ALTER TASK IF EXISTS RAW.CHIPMUNK_T3 SUSPEND;
ALTER TASK IF EXISTS RAW.CHIPMUNK_T2 SUSPEND;
ALTER TASK IF EXISTS RAW.CHIPMUNK_T1 SUSPEND;

-- ---- Drop tasks ----
DROP TASK IF EXISTS RAW.CHIPMUNK_T3;
DROP TASK IF EXISTS RAW.CHIPMUNK_T2;
DROP TASK IF EXISTS RAW.CHIPMUNK_T1;

-- ---- Drop views (raw + modeled) ----
DROP VIEW IF EXISTS RAW.CHIPMUNK_PDF_RAW_FILE_CATALOG_VW;        -- Helper view (optional)
DROP VIEW IF EXISTS MODELED.CHIPMUNK_FINAL_INTEGRATED_PART;      -- Final view (partitioned)
DROP VIEW IF EXISTS MODELED.CHIPMUNK_FINAL_INTEGRATED_NOPART;    -- Final view (no partition)

-- ---- Drop streams ----
DROP STREAM IF EXISTS RAW.CHIPMUNK_PDF_STAGE_STREAM;             -- Stage stream (detect new PDFs)
DROP STREAM IF EXISTS RAW.CHIPMUNK_PDF_STAGE_STREAM_T2;          -- Table stream (detect new RAW rows)

-- ---- Drop tables ----
DROP TABLE IF EXISTS RAW.CHIPMUNK_PDF_PROCESSED_FILE_CATALOG;    -- Processed PDFs (text + metadata)
DROP TABLE IF EXISTS RAW.CHIPMUNK_PDF_RAW_FILE_CATALOG;          -- Raw file metadata

-- ---- Drop UDF ----
DROP FUNCTION IF EXISTS RAW.CHIPMUNK_GET_PDF_PAYLOAD(STRING);    -- PDF parser UDF

-- ---- Drop stage ----
DROP STAGE IF EXISTS RAW.CHIPMUNK_PDF_INT_STAGE;                 -- Internal stage for PDFs


-- ============================================================
-- 2) STAGE + DIRECTORY + STAGE STREAM 
-- ============================================================

CREATE STAGE IF NOT EXISTS RAW.CHIPMUNK_PDF_INT_STAGE;           -- Create internal stage for PDF uploads
ALTER STAGE RAW.CHIPMUNK_PDF_INT_STAGE SET DIRECTORY = (ENABLE = TRUE); -- Enable DIRECTORY() listing
CREATE OR REPLACE STREAM RAW.CHIPMUNK_PDF_STAGE_STREAM
  ON STAGE RAW.CHIPMUNK_PDF_INT_STAGE;                           -- Track new files added to the stage
ALTER STAGE RAW.CHIPMUNK_PDF_INT_STAGE REFRESH;                  -- Force directory to refresh (detect files)

-- ---- Checks  ----
SELECT COUNT(*) AS pdfs
FROM DIRECTORY(@RAW.CHIPMUNK_PDF_INT_STAGE)
WHERE RELATIVE_PATH ILIKE '%.pdf';                               -- Count PDFs found in stage

SELECT *
FROM RAW.CHIPMUNK_PDF_STAGE_STREAM
WHERE METADATA$ACTION = 'INSERT'
ORDER BY RELATIVE_PATH;                                          -- Show newly detected PDFs via stream


-- ============================================================
-- 3) TABLES + TABLE STREAM
-- ============================================================

-- RAW: stores file inventory + scoped URL + timestamps (NO parsing in T1)
CREATE OR REPLACE TRANSIENT TABLE RAW.CHIPMUNK_PDF_RAW_FILE_CATALOG (
  file_name     STRING,                                          -- filename only
  relative_path STRING,                                          -- full stage path
  file_url      STRING,                                          -- stage-provided file URL
  scoped_url    STRING,                                          -- scoped URL for SnowflakeFile.open()
  size          NUMBER,                                          -- bytes
  last_modified TIMESTAMP_LTZ,                                   -- last modified timestamp from directory
  extracted     VARIANT,                                         -- optional storage for parsed payload (unused in this design)
  insert_ts     TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()         -- load timestamp
);

-- PROCESSED: stores parsed results for analytics + joining
CREATE OR REPLACE TRANSIENT TABLE RAW.CHIPMUNK_PDF_PROCESSED_FILE_CATALOG (
  pdf_id         STRING,                                         -- ID derived from filename
  file_name      STRING,                                         -- filename only
  relative_path  STRING,                                         -- full stage path
  file_url       STRING,                                         -- stage-provided file URL
  last_modified  TIMESTAMP_LTZ,                                  -- last modified timestamp
  pdf_metadata   VARIANT,                                        -- extracted metadata from PDF
  page_count     NUMBER,                                         -- page count
  extracted_text STRING,                                         -- extracted text content
  insert_ts      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()        -- load timestamp
);

-- The table stream captures newly inserted RAW metadata rows so T2 can incrementally parse only new PDFs, preventing reprocessing and duplicate loads while enabling an event-driven task pipeline.
CREATE OR REPLACE STREAM RAW.CHIPMUNK_PDF_STAGE_STREAM_T2
  ON TABLE RAW.CHIPMUNK_PDF_RAW_FILE_CATALOG;


-- ============================================================
-- 4) PYTHON UDF (PARSE PDF METADATA + FULL TEXT)
-- ============================================================

CREATE OR REPLACE FUNCTION RAW.CHIPMUNK_GET_PDF_PAYLOAD(FILE_URL STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
PACKAGES = ('snowflake-snowpark-python','PyPDF2')
HANDLER = 'main'
AS
$$
from snowflake.snowpark.files import SnowflakeFile
from PyPDF2 import PdfReader

def main(file_url: str):
    with SnowflakeFile.open(file_url, 'rb') as f:
        reader = PdfReader(f)

        meta_obj = {}
        try:
            m = reader.metadata
            if m:
                for k, v in m.items():
                    meta_obj[str(k)] = None if v is None else str(v)
        except Exception:
            meta_obj = {}

        pages = len(reader.pages)
        text_parts = []
        for i in range(pages):
            try:
                text_parts.append(reader.pages[i].extract_text() or "")
            except Exception:
                text_parts.append("")
        full_text = "\n".join(text_parts)

    return {
        "metadata": meta_obj,
        "page_count": pages,
        "chars": len(full_text),
        "text": full_text
    }
$$;

-- ---- UDF quick test  ----
SELECT
  RELATIVE_PATH,
  RAW.CHIPMUNK_GET_PDF_PAYLOAD(
    BUILD_SCOPED_FILE_URL(@RAW.CHIPMUNK_PDF_INT_STAGE, RELATIVE_PATH)
  ) AS parsed
FROM DIRECTORY(@RAW.CHIPMUNK_PDF_INT_STAGE)
WHERE RELATIVE_PATH ILIKE '%.pdf'
LIMIT 2;



-- ============================================================
-- 5) TASK DAG (T1 -> T2 -> T3)
-- ============================================================

-- ============================================================
-- T1 (serverless): refresh stage + insert RAW METADATA ONLY
-- Purpose:
--   (1) Refresh the internal stage directory so Snowflake "sees" newly uploaded PDFs
--   (2) Insert ONLY NEW PDF file metadata (not parsed text) into the RAW catalog table
-- Why this task exists:
--   - Separates "file detection + metadata logging" (T1) from "PDF parsing" (T2)
--   - Uses the STAGE STREAM to process only newly added files (incremental pipeline)
-- ============================================================

CREATE OR REPLACE TASK RAW.CHIPMUNK_T1
SCHEDULE = '1 minute'                               -- Run this task automatically every minute
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'Medium' -- Use Snowflake-managed serverless compute (no warehouse                                                           needed)
AS
BEGIN                                               -- Begin multi-statement task block (allows multiple SQL                                                            statements)
  -- --------------------------------------------------------
  -- Step 1: Refresh the stage directory metadata
  -- Why:
  --   DIRECTORY(@stage) reads from the stage's directory listing.
  --   REFRESH forces Snowflake to scan the stage and update that listing
  --   so newly uploaded PDFs become visible to DIRECTORY().
  -- --------------------------------------------------------
  ALTER STAGE RAW.CHIPMUNK_PDF_INT_STAGE REFRESH;

  -- --------------------------------------------------------
  -- Step 2: Insert RAW metadata for NEW PDF files only
  -- How "new files" are detected:
  --   - RAW.CHIPMUNK_PDF_STAGE_STREAM is a STREAM ON STAGE.
  --   - When new files appear in the stage, the stream records an INSERT event.
  --   - We JOIN DIRECTORY(@stage) with the stream to pick only those inserted files.
  -- What we store:
  --   - file_name      : just the filename (no folders)
  --   - relative_path  : full path inside the stage
  --   - file_url       : stage-provided file reference
  --   - scoped_url     : secure URL used by SnowflakeFile.open() in the Python UDF (T2)
  --   - size           : file size in bytes
  --   - last_modified  : file last modified timestamp
  --   - extracted      : NULL here because parsing happens in T2 (processed layer)
  -- --------------------------------------------------------
  INSERT INTO RAW.CHIPMUNK_PDF_RAW_FILE_CATALOG
    (file_name, relative_path, file_url, scoped_url, size, last_modified, extracted)
  SELECT
    SPLIT_PART(d.relative_path, '/', -1) AS file_name,  -- Extract only the filename portion from the full path                                                             (e.g., "folder/a.pdf" -> "a.pdf")
    d.relative_path,    -- Store the full relative path inside the stage (used for traceability)
    d.file_url,    -- Store the stage file URL (useful for debugging / lineage)
    BUILD_SCOPED_FILE_URL(@RAW.CHIPMUNK_PDF_INT_STAGE, d.relative_path) AS scoped_url,    -- Build a scoped URL                                                             for secure file access by the Python UDF in Task T2
    d.size,
    d.last_modified::TIMESTAMP_LTZ, -- Cast last_modified into TIMESTAMP_LTZ to match the RAW table column type
    NULL::VARIANT AS extracted
  FROM DIRECTORY(@RAW.CHIPMUNK_PDF_INT_STAGE) d    -- DIRECTORY() returns one row per file in the stage
  JOIN RAW.CHIPMUNK_PDF_STAGE_STREAM s             -- Stage stream contains change events for the stage
    ON d.relative_path = s.relative_path           -- Match the directory row to the stream event by path
  WHERE s.METADATA$ACTION = 'INSERT'               -- Only handle newly added files (not deletes/updates)
    AND d.relative_path ILIKE '%.pdf'              -- Process only PDFs (ignore non-PDF files)
    
-- ------------------------------------------------------
-- Safety check: prevents duplicates if:
--   - the task reruns, or
--   - stage stream events replay, or
--   - REFRESH causes the same file to be detected again
-- We treat (relative_path + last_modified) as a unique version of the file.
-- ------------------------------------------------------
    AND NOT EXISTS (
      SELECT 1
      FROM RAW.CHIPMUNK_PDF_RAW_FILE_CATALOG r
      WHERE r.relative_path = d.relative_path
        AND r.last_modified = d.last_modified::TIMESTAMP_LTZ
    );
END;

-- ============================================================
-- TASK: RAW.CHIPMUNK_T2
-- Purpose:
--   Parse newly detected PDFs using the Python UDF (CHIPMUNK_GET_PDF_PAYLOAD)
--   and insert the extracted results into the PROCESSED table.
--
-- Why this task exists:
--   - T1 only records RAW file metadata (cheap, fast, incremental)
--   - T2 performs the expensive work (PDF text extraction) only for new files
--   - Uses a TABLE STREAM as an incremental "to-do list" of new RAW rows
-- ============================================================

CREATE OR REPLACE TASK RAW.CHIPMUNK_T2
WAREHOUSE = CHIPMUNK_WH
AFTER RAW.CHIPMUNK_T1
WHEN SYSTEM$STREAM_HAS_DATA('RAW.CHIPMUNK_PDF_STAGE_STREAM_T2') -- Guardrail: run only if stream has new change records
AS
BEGIN
  -- ----------------------------------------------------------
  -- Insert parsed PDF outputs into the PROCESSED catalog table
  -- PROCESSED table stores:
  --   - pdf_id         : identifier derived from filename
  --   - pdf_metadata   : metadata extracted from PDF
  --   - page_count     : number of pages
  --   - extracted_text : full extracted text content
  -- ----------------------------------------------------------
  
  INSERT INTO RAW.CHIPMUNK_PDF_PROCESSED_FILE_CATALOG
    (pdf_id, file_name, relative_path, file_url, last_modified, pdf_metadata, page_count, extracted_text)
  SELECT
    REGEXP_REPLACE(SPLIT_PART(relative_path, '/', -1), '\\.pdf$', '', 1, 0, 'i') AS pdf_id, -- Extract in this format: "E-TV002_Manual-1.pdf" -> "E-TV002_Manual-1"
    
    -- Carry forward metadata columns from RAW
    file_name,
    relative_path,
    file_url,
    last_modified,
    
    -- Extract structured fields from the UDF payload (VARIANT)
    payload:metadata                          AS pdf_metadata,
    TRY_TO_NUMBER(payload:page_count::STRING) AS page_count,
    payload:text::STRING                      AS extracted_text
  FROM (
    -- --------------------------------------------------------
    -- Inner query: get ONLY the new RAW rows from the table stream
    -- and run the UDF on those files.
    --
    -- Why use the table stream:
    --   The stream tracks NEW INSERTS into RAW.CHIPMUNK_PDF_RAW_FILE_CATALOG
    --   so we do not re-parse old PDFs each time the task runs.
    -- --------------------------------------------------------
    
    SELECT
      r.file_name,
      r.relative_path,
      r.file_url,
      r.last_modified,
      RAW.CHIPMUNK_GET_PDF_PAYLOAD(r.scoped_url) AS payload -- to access file securely
      FROM RAW.CHIPMUNK_PDF_STAGE_STREAM_T2 st
    JOIN RAW.CHIPMUNK_PDF_RAW_FILE_CATALOG r
      ON r.relative_path = st.relative_path
     AND r.last_modified = st.last_modified
    WHERE st.METADATA$ACTION = 'INSERT' -- only handle newly inserted RAW rows
  ) x
  -- ----------------------------------------------------------
  -- Safety: prevent duplicates if task reruns / retries
  -- We treat (relative_path + last_modified) as the unique file version key.
  -- If that file-version already exists in PROCESSED, skip insert.
  -- ----------------------------------------------------------
  
  WHERE NOT EXISTS (
    SELECT 1
    FROM RAW.CHIPMUNK_PDF_PROCESSED_FILE_CATALOG p
    WHERE p.relative_path = x.relative_path
      AND p.last_modified = x.last_modified
  );
END;

-- ============================================================
-- TASK: RAW.CHIPMUNK_T3
-- Purpose:
--   Build the final MODELED-layer views after PDF parsing is completed.
--
-- What T3 produces:
--   1) MODELED.CHIPMUNK_FINAL_INTEGRATED_PART   (uses PARTITIONED external table)
--   2) MODELED.CHIPMUNK_FINAL_INTEGRATED_NOPART (uses NON-PARTITIONED external table)
--
-- Why we build TWO views:
--   - The only difference is the external table used for the Part B stats.
--   - This lets you run the same query against both views and screenshot Query Profile
--     to prove partition pruning improves performance.
--
-- Why T3 is serverless:
--   - It only creates/updates views (no heavy compute like Python parsing),
--     so Snowflake-managed compute is sufficient.
-- ============================================================

CREATE OR REPLACE TASK RAW.CHIPMUNK_T3
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'Medium'
AFTER RAW.CHIPMUNK_T2
AS
BEGIN
  -- Ensure MODELED schema exists before creating views inside it
  CREATE SCHEMA IF NOT EXISTS MODELED;

  -- ----------------------------------------------------------
  -- (A) FINAL VIEW USING PARTITIONED EXTERNAL TABLE
  -- Expected: better pruning + less data scanned in Query Profile
  -- ----------------------------------------------------------
  CREATE OR REPLACE VIEW MODELED.CHIPMUNK_FINAL_INTEGRATED_PART AS
  WITH partb_stats AS (
    SELECT
      year,
      quarter,
      AVG(arr_delay)        AS avg_arr_delay,
      AVG(cancelled)::FLOAT AS cancel_rate
    FROM RAW.EXTERNAL_ONTIME_ASG_CHIPMUNK_PART -- PARTITIONED external table (Part B QN 1)
    WHERE year = 2019
      AND quarter = 3
    GROUP BY year, quarter
  )
    -- Final integrated output combines curated product info + PDF text + external stats
  SELECT
    -- --------------------------
    -- Part A curated columns
    -- These come from your curated dataset and describe the product + review info.
    -- If join fails, these columns will appear as NULL.
    -- --------------------------
    c.PRODUCT_ID,
    c.BRAND,
    c.MODEL_NAME,
    c.CATEGORY,
    c.REVIEW_DATE,
    YEAR(c.REVIEW_DATE)  AS review_year,
    MONTH(c.REVIEW_DATE) AS review_month,
    DAY(c.REVIEW_DATE)   AS review_day,
    c.RATING,
    c.REVIEW_TEXT,
    c.REVIEWER_LOCATION_NAME,
    c.REVIEWER_GEO_POINT,

    -- --------------------------
    -- PDF processed columns (from T2)
    -- This is the parsed PDF output stored in PROCESSED table.
    -- --------------------------
    p.pdf_id,
    p.file_name,
    p.relative_path,
    p.file_url,
    p.last_modified,
    p.pdf_metadata,
    p.page_count,
    p.extracted_text,

    -- --------------------------
    -- External table stats (Part B)
    -- These are appended to every row via a cross join (ON 1=1)
    -- because partb_stats is a single aggregated row (2019, Q3).
    -- --------------------------
    s.avg_arr_delay,
    s.cancel_rate
  FROM RAW.CHIPMUNK_PDF_PROCESSED_FILE_CATALOG p
  LEFT JOIN DLW_GROUP1_DB.GROUP1_ASG.CURATED_PRODUCT_FEEDBACK c
    ON c.PRODUCT_ID = SPLIT_PART(p.pdf_id, '_', 1) -- we take substring before '_' so as to match product IDs
  LEFT JOIN partb_stats s
    ON 1 = 1;

  -- ----------------------------------------------------------
  -- (B) FINAL VIEW USING NON-PARTITIONED EXTERNAL TABLE
  -- Expected: worse pruning + more data scanned in Query Profile
  -- ----------------------------------------------------------
  CREATE OR REPLACE VIEW MODELED.CHIPMUNK_FINAL_INTEGRATED_NOPART AS
  WITH partb_stats AS (
    SELECT
      year,
      quarter,
      AVG(arr_delay)        AS avg_arr_delay,
      AVG(cancelled)::FLOAT AS cancel_rate
    FROM RAW.EXTERNAL_ONTIME_ASG_CHIPMUNK_NOPART
    WHERE year = 2019
      AND quarter = 3
    GROUP BY year, quarter
  )
  SELECT
  
    -- Curated columns (same as PART view)
    c.PRODUCT_ID,
    c.BRAND,
    c.MODEL_NAME,
    c.CATEGORY,
    c.REVIEW_DATE,
    YEAR(c.REVIEW_DATE)  AS review_year,
    MONTH(c.REVIEW_DATE) AS review_month,
    DAY(c.REVIEW_DATE)   AS review_day,
    c.RATING,
    c.REVIEW_TEXT,
    c.REVIEWER_LOCATION_NAME,
    c.REVIEWER_GEO_POINT,
    
    -- Processed PDF columns (same as PART view)
    p.pdf_id,
    p.file_name,
    p.relative_path,
    p.file_url,
    p.last_modified,
    p.pdf_metadata,
    p.page_count,
    p.extracted_text,
    
    -- External stats from NOPART table
    s.avg_arr_delay,
    s.cancel_rate
  FROM RAW.CHIPMUNK_PDF_PROCESSED_FILE_CATALOG p
  LEFT JOIN DLW_GROUP1_DB.GROUP1_ASG.CURATED_PRODUCT_FEEDBACK c
    ON c.PRODUCT_ID = SPLIT_PART(p.pdf_id, '_', 1)
  LEFT JOIN partb_stats s
    ON 1 = 1;

END;


-- ============================================================
-- 6) RUN / TEST (task history + tables)
-- ============================================================

-- Resume ALL tasks 
ALTER TASK RAW.CHIPMUNK_T2 RESUME;
ALTER TASK RAW.CHIPMUNK_T3 RESUME;

-- Kick off the DAG by executing T1 once
EXECUTE TASK RAW.CHIPMUNK_T1;

-- Confirm tasks exist
SHOW TASKS LIKE 'CHIPMUNK%';

-- Check raw & processed tables
SELECT * FROM RAW.CHIPMUNK_PDF_RAW_FILE_CATALOG ORDER BY insert_ts DESC; -- T1
SELECT * FROM RAW.CHIPMUNK_PDF_PROCESSED_FILE_CATALOG ORDER BY insert_ts DESC; -- T2

-- Task history 
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
  SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP()),
  RESULT_LIMIT => 50
))
WHERE NAME ILIKE '%CHIPMUNK_T%'
ORDER BY COMPLETED_TIME DESC;


-- Row count validation 
SELECT COUNT(*) AS raw_rows FROM RAW.CHIPMUNK_PDF_RAW_FILE_CATALOG;
SELECT COUNT(*) AS processed_rows FROM RAW.CHIPMUNK_PDF_PROCESSED_FILE_CATALOG;


-- ============================================================
-- 7) PARTITION PERFORMANCE TEST 
-- ============================================================

ALTER SESSION SET USE_CACHED_RESULT = FALSE;
-- Query A: WITH partitions
SELECT
  COUNT(*) AS rows_cnt,
FROM MODELED.CHIPMUNK_FINAL_INTEGRATED_PART;

-- Query B: WITHOUT partitions
SELECT
  COUNT(*) AS rows_cnt,
FROM MODELED.CHIPMUNK_FINAL_INTEGRATED_NOPART;

-- filter on date columns
SELECT *
FROM MODELED.CHIPMUNK_FINAL_INTEGRATED_PART
WHERE review_year IN (2024, 2025)
ORDER BY REVIEW_DATE DESC

    
-- ============================================================
-- 9) CLEANUP (avoid credits)
-- ============================================================
ALTER TASK RAW.CHIPMUNK_T3 SUSPEND;
ALTER TASK RAW.CHIPMUNK_T2 SUSPEND;
ALTER TASK RAW.CHIPMUNK_T1 SUSPEND;

ALTER SESSION UNSET QUERY_TAG;
ALTER WAREHOUSE CHIPMUNK_WH SUSPEND;
-- check status
SHOW WAREHOUSES LIKE 'CHIPMUNK_WH';