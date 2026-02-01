# Data-Lake-and-Warehouse-Assignment
# Data Lake and Warehouse Assignment - Snowflake Data Engineering & Retail Analytics Project
---

## 🏗️ Project Overview
This repository contains a comprehensive data engineering solution implemented in **Snowflake**. The project demonstrates a full ELT (Extract, Load, Transform) pipeline integrating structured, semi-structured (JSON), and unstructured (PDF) data to support retail performance analytics and customer sentiment insights.

---

## 🛠️ Detailed Project Contributions

### Part A: Multi-Domain Data Integration & Curated Pipeline
In this group phase, I contributed to the design of a layered data architecture (RAW → STG → CURATED) to integrate fragmented business domains.

* **Domain Orchestration:** Ingested and integrated five key domains: Product Master, Sales Transactions, Customer Reviews, Media Metadata, and Returns/Issues.
* **Advanced JSON Flattening:** Implemented an 8-level JSON hierarchy flattening for Product Master records, which was 60% above the base requirement.
* **Curated Dataset Design:** Built the `CURATED_PRODUCT_FEEDBACK` table at the "Review-Grain" (1 row per review event).
* **Pre-aggregation Logic:** Utilized Common Table Expressions (CTEs) to aggregate media URLs and return metrics before joining, preventing row duplication and maintaining data integrity.
* **Geospatial & Data Quality:** Integrated coordinates for supplier HQs, stores, and customers using `ST_POINT()` for mapping analysis.
* **Null Handling:** Applied `COALESCE` functions to ensure reporting-friendly outputs, such as labeling missing media as "NO MEDIA".

### Part B Question 1: Data Lake Optimization & Partitioning
This individual component focused on performance tuning and storage management within a Snowflake Data Lake.

* **Stored Procedure (SP) Development:** Created a JavaScript Stored Procedure, `ASG_UNLOAD_ONTIME_REPORTING_CSV`, to automate the unloading of flight data into partitioned stage folders (Year/Quarter/Month).
* **External Table Partitioning:** Defined an external table with a `PARTITION BY` function to enable metadata-level pruning.
* **Performance Benchmarking:** Compared query execution profiles with and without partitioning.
    * **Without Partition:** 39s execution, 2.77 GB scanned.
    * **With Partition:** 11s execution, 836.74 MB scanned.
* **Materialized Views:** Implemented Materialized Views to reduce aggregation overhead, achieving an execution time of just 576 milliseconds.

### Part B Question 2: Unstructured Data Automation (Task DAG)
I developed an automated pipeline to ingest and parse unstructured PDF documents.

* **Python UDF Development:** Built a Python UDF (`RAW.CHIPMUNK_GET_PDF_PAYLOAD`) using the `PyPDF2` library and `SnowflakeFile` to extract text and metadata from PDFs.
* **Task DAG Orchestration:** Designed a three-stage automated workflow:
    * **T1 (Metadata):** Detects new PDF uploads and records metadata in a RAW catalog.
    * **T2 (Parsing):** Triggered by **Streams** on new data, this task runs the Python UDF to extract content.
    * **T3 (Modeling):** Consolidates parsed text with curated retail data for final reporting.
* **Incremental Loading:** Implemented **Table Streams** to ensure only newly arrived files are processed, optimizing compute resource usage.

---

## 📈 Key Technical Skills Demonstrated
* **Snowflake ELT:** Staging, `COPY INTO`, and `LATERAL FLATTEN()`.
* **Optimization:** Partition pruning, Materialized Views, and Warehouse cost control.
* **Programming:** SQL, JavaScript (Stored Procedures), and Python (Snowpark UDFs).
* **Automation:** Task DAGs, Change Data Capture (CDC) with Streams, and Serverless tasks.

---

## ⚠️ Challenges & Solutions
* **ID Suffix Mismatch:** Resolved join issues between PDFs and Product IDs by using `SPLIT_PART()` to handle ID suffixes like `_Manual-1`.
* **Stream Consumption:** Fixed issues where streams appeared empty by ensuring stage refreshes occurred after stream creation.
* **Duplicate Prevention:** Added `NOT EXISTS` checks on `relative_path` and `last_modified` fields to prevent duplicate records during task reruns.
