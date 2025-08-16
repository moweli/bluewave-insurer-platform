# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake Table Setup for UK Insurance Claims
# MAGIC This notebook creates the medallion architecture (Bronze, Silver, Gold) for the insurance claims streaming pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from delta.tables import DeltaTable
import json

# Initialize Spark session with Delta Lake
spark = SparkSession.builder \
    .appName("InsuranceClaimsTableSetup") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Database configuration
BRONZE_DB = "insurance_bronze"
SILVER_DB = "insurance_silver"
GOLD_DB = "insurance_gold"

# Storage paths
STORAGE_ACCOUNT = "bluewaveplatformsa"
CONTAINER = "delta-lake"
BRONZE_PATH = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
SILVER_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
GOLD_PATH = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Databases

# COMMAND ----------

# Create databases
spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_DB}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DB}")

print(f"✅ Created databases: {BRONZE_DB}, {SILVER_DB}, {GOLD_DB}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer Tables - Raw Data

# COMMAND ----------

# Bronze Claims Schema - Raw ingestion from Event Hub
bronze_claims_schema = StructType([
    StructField("claim_id", StringType(), False),
    StructField("policy_number", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("claim_amount", DecimalType(10, 2), True),
    StructField("claim_date", TimestampType(), True),
    StructField("incident_date", TimestampType(), True),
    StructField("incident_location", StringType(), True),
    StructField("incident_postcode", StringType(), True),
    StructField("claim_type", StringType(), True),
    StructField("claim_status", StringType(), True),
    StructField("fraud_score", DoubleType(), True),
    StructField("description", StringType(), True),
    StructField("_raw_data", StringType(), True),
    StructField("_ingestion_timestamp", TimestampType(), False),
    StructField("_event_hub_offset", LongType(), True),
    StructField("_event_hub_partition", IntegerType(), True),
    StructField("_event_hub_sequence", LongType(), True),
    StructField("_data_source", StringType(), True)
])

# Create Bronze Claims table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_DB}.claims_raw (
    claim_id STRING NOT NULL,
    policy_number STRING,
    customer_id STRING,
    claim_amount DECIMAL(10,2),
    claim_date TIMESTAMP,
    incident_date TIMESTAMP,
    incident_location STRING,
    incident_postcode STRING,
    claim_type STRING,
    claim_status STRING,
    fraud_score DOUBLE,
    description STRING,
    _raw_data STRING,
    _ingestion_timestamp TIMESTAMP NOT NULL,
    _event_hub_offset BIGINT,
    _event_hub_partition INT,
    _event_hub_sequence BIGINT,
    _data_source STRING
)
USING DELTA
PARTITIONED BY (DATE(claim_date))
LOCATION '{BRONZE_PATH}claims_raw'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.dataSkippingNumIndexedCols' = '32',
    'delta.deletedFileRetentionDuration' = 'interval 30 days',
    'delta.logRetentionDuration' = 'interval 60 days'
)
""")

print("✅ Created Bronze claims_raw table")

# COMMAND ----------

# Bronze Policies Schema
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_DB}.policies_raw (
    policy_number STRING NOT NULL,
    customer_id STRING,
    policy_type STRING,
    start_date DATE,
    end_date DATE,
    premium_amount DECIMAL(10,2),
    coverage_limit DECIMAL(12,2),
    deductible DECIMAL(10,2),
    risk_score DOUBLE,
    underwriter STRING,
    _ingestion_timestamp TIMESTAMP NOT NULL,
    _raw_data STRING
)
USING DELTA
PARTITIONED BY (policy_type)
LOCATION '{BRONZE_PATH}policies_raw'
""")

print("✅ Created Bronze policies_raw table")

# COMMAND ----------

# Bronze Customers Schema
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_DB}.customers_raw (
    customer_id STRING NOT NULL,
    first_name STRING,
    last_name STRING,
    date_of_birth DATE,
    email STRING,
    phone STRING,
    address_line1 STRING,
    address_line2 STRING,
    city STRING,
    postcode STRING,
    country STRING,
    registration_date DATE,
    customer_segment STRING,
    lifetime_value DECIMAL(12,2),
    _ingestion_timestamp TIMESTAMP NOT NULL,
    _raw_data STRING
)
USING DELTA
LOCATION '{BRONZE_PATH}customers_raw'
""")

print("✅ Created Bronze customers_raw table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer Tables - Cleaned and Enriched

# COMMAND ----------

# Silver Claims - Cleaned and validated
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_DB}.claims (
    claim_id STRING NOT NULL,
    policy_number STRING NOT NULL,
    customer_id STRING NOT NULL,
    claim_amount DECIMAL(10,2) NOT NULL,
    claim_amount_gbp DECIMAL(10,2) NOT NULL,
    claim_date TIMESTAMP NOT NULL,
    incident_date TIMESTAMP NOT NULL,
    incident_location STRING,
    incident_postcode STRING,
    postcode_district STRING,
    claim_type STRING NOT NULL,
    claim_status STRING NOT NULL,
    fraud_score DOUBLE,
    fraud_indicators ARRAY<STRING>,
    is_high_risk BOOLEAN,
    requires_manual_review BOOLEAN,
    days_to_claim INT,
    claim_to_premium_ratio DOUBLE,
    customer_age_at_claim INT,
    is_weekend_claim BOOLEAN,
    is_bank_holiday BOOLEAN,
    weather_event_correlation STRING,
    processing_timestamp TIMESTAMP,
    data_quality_score DOUBLE,
    _silver_timestamp TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (DATE(claim_date), claim_type)
LOCATION '{SILVER_PATH}claims'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.columnMapping.mode' = 'name'
)
""")

print("✅ Created Silver claims table")

# COMMAND ----------

# Silver Policies - Enriched with calculated fields
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_DB}.policies (
    policy_number STRING NOT NULL,
    customer_id STRING NOT NULL,
    policy_type STRING NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    policy_duration_days INT,
    premium_amount DECIMAL(10,2) NOT NULL,
    premium_monthly DECIMAL(10,2),
    coverage_limit DECIMAL(12,2) NOT NULL,
    deductible DECIMAL(10,2),
    risk_score DOUBLE,
    risk_category STRING,
    underwriter STRING,
    is_active BOOLEAN,
    days_until_renewal INT,
    claims_count INT,
    total_claims_amount DECIMAL(12,2),
    loss_ratio DOUBLE,
    profitability_score DOUBLE,
    _silver_timestamp TIMESTAMP NOT NULL,
    _scd_start_date TIMESTAMP,
    _scd_end_date TIMESTAMP,
    _scd_is_current BOOLEAN
)
USING DELTA
PARTITIONED BY (policy_type)
LOCATION '{SILVER_PATH}policies'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
)
""")

print("✅ Created Silver policies table")

# COMMAND ----------

# Silver Customers - Enriched and masked PII
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_DB}.customers (
    customer_id STRING NOT NULL,
    customer_hash STRING NOT NULL,
    age_band STRING,
    postcode_district STRING,
    city STRING,
    country STRING,
    registration_year INT,
    customer_segment STRING,
    lifetime_value DECIMAL(12,2),
    lifetime_value_band STRING,
    total_policies INT,
    active_policies INT,
    total_claims INT,
    total_claims_amount DECIMAL(12,2),
    average_claim_amount DECIMAL(10,2),
    fraud_risk_score DOUBLE,
    churn_risk_score DOUBLE,
    is_vulnerable_customer BOOLEAN,
    gdpr_consent BOOLEAN,
    gdpr_consent_date DATE,
    data_retention_date DATE,
    _silver_timestamp TIMESTAMP NOT NULL
)
USING DELTA
LOCATION '{SILVER_PATH}customers'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.deletedFileRetentionDuration' = 'interval 7 days'
)
""")

print("✅ Created Silver customers table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer Tables - Business Aggregations

# COMMAND ----------

# Gold - Daily Claims Summary
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_DB}.daily_claims_summary (
    summary_date DATE NOT NULL,
    claim_type STRING NOT NULL,
    total_claims INT,
    total_amount DECIMAL(12,2),
    average_amount DECIMAL(10,2),
    median_amount DECIMAL(10,2),
    fraud_detected_count INT,
    fraud_detected_amount DECIMAL(12,2),
    high_risk_count INT,
    manual_review_count INT,
    auto_approved_count INT,
    processing_time_avg_ms BIGINT,
    processing_time_p95_ms BIGINT,
    _gold_timestamp TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (summary_date)
LOCATION '{GOLD_PATH}daily_claims_summary'
""")

print("✅ Created Gold daily_claims_summary table")

# COMMAND ----------

# Gold - Customer Risk Profiles
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_DB}.customer_risk_profiles (
    customer_id STRING NOT NULL,
    risk_segment STRING NOT NULL,
    fraud_risk_score DOUBLE,
    claims_frequency_score DOUBLE,
    claims_severity_score DOUBLE,
    payment_behavior_score DOUBLE,
    overall_risk_rating STRING,
    recommended_premium_adjustment DOUBLE,
    retention_probability DOUBLE,
    lifetime_value_prediction DECIMAL(12,2),
    last_updated TIMESTAMP NOT NULL,
    _gold_timestamp TIMESTAMP NOT NULL
)
USING DELTA
LOCATION '{GOLD_PATH}customer_risk_profiles'
""")

print("✅ Created Gold customer_risk_profiles table")

# COMMAND ----------

# Gold - Fraud Detection Metrics
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_DB}.fraud_metrics (
    metric_date DATE NOT NULL,
    metric_hour INT,
    total_claims_evaluated INT,
    fraud_alerts_generated INT,
    confirmed_fraud_cases INT,
    false_positive_count INT,
    false_negative_count INT,
    precision_score DOUBLE,
    recall_score DOUBLE,
    f1_score DOUBLE,
    total_fraud_prevented_gbp DECIMAL(12,2),
    average_detection_time_ms BIGINT,
    top_fraud_patterns ARRAY<STRING>,
    high_risk_postcodes ARRAY<STRING>,
    _gold_timestamp TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (metric_date)
LOCATION '{GOLD_PATH}fraud_metrics'
""")

print("✅ Created Gold fraud_metrics table")

# COMMAND ----------

# Gold - Operational KPIs
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_DB}.operational_kpis (
    kpi_date DATE NOT NULL,
    kpi_name STRING NOT NULL,
    kpi_value DOUBLE,
    kpi_target DOUBLE,
    kpi_status STRING,
    variance_from_target DOUBLE,
    trend_7_day STRING,
    trend_30_day STRING,
    _gold_timestamp TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (kpi_date)
LOCATION '{GOLD_PATH}operational_kpis'
""")

print("✅ Created Gold operational_kpis table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Dimension Tables

# COMMAND ----------

# UK Postcode Risk Dimension
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_DB}.dim_postcode_risk (
    postcode_district STRING NOT NULL,
    region STRING,
    flood_risk_level STRING,
    crime_risk_level STRING,
    average_property_value DECIMAL(12,2),
    population_density STRING,
    urban_rural_classification STRING,
    historical_claim_frequency DOUBLE,
    historical_claim_severity DOUBLE,
    risk_score DOUBLE,
    last_updated DATE
)
USING DELTA
LOCATION '{SILVER_PATH}dim_postcode_risk'
""")

print("✅ Created dimension table: dim_postcode_risk")

# COMMAND ----------

# Insurance Product Dimension
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_DB}.dim_insurance_products (
    product_code STRING NOT NULL,
    product_name STRING,
    product_type STRING,
    product_category STRING,
    min_coverage DECIMAL(10,2),
    max_coverage DECIMAL(12,2),
    base_premium DECIMAL(10,2),
    risk_factors ARRAY<STRING>,
    excluded_conditions ARRAY<STRING>,
    regulatory_requirements ARRAY<STRING>,
    is_active BOOLEAN,
    launch_date DATE,
    discontinue_date DATE
)
USING DELTA
LOCATION '{SILVER_PATH}dim_insurance_products'
""")

print("✅ Created dimension table: dim_insurance_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Views for Easy Access

# COMMAND ----------

# Create useful views
spark.sql(f"""
CREATE OR REPLACE VIEW {GOLD_DB}.v_recent_high_value_claims AS
SELECT 
    c.claim_id,
    c.claim_date,
    c.customer_id,
    c.claim_amount_gbp,
    c.claim_type,
    c.fraud_score,
    c.fraud_indicators,
    cust.customer_segment,
    cust.lifetime_value
FROM {SILVER_DB}.claims c
JOIN {SILVER_DB}.customers cust ON c.customer_id = cust.customer_id
WHERE c.claim_date >= current_date() - INTERVAL 7 DAYS
    AND c.claim_amount_gbp > 10000
ORDER BY c.claim_amount_gbp DESC
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {GOLD_DB}.v_fraud_investigation_queue AS
SELECT 
    c.claim_id,
    c.claim_date,
    c.customer_id,
    c.claim_amount_gbp,
    c.fraud_score,
    c.fraud_indicators,
    c.incident_postcode,
    p.risk_score as postcode_risk_score
FROM {SILVER_DB}.claims c
LEFT JOIN {SILVER_DB}.dim_postcode_risk p ON c.postcode_district = p.postcode_district
WHERE c.requires_manual_review = true
    OR c.fraud_score > 0.7
ORDER BY c.fraud_score DESC, c.claim_amount_gbp DESC
""")

print("✅ Created views for reporting")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Optimization Settings

# COMMAND ----------

# Optimize tables for streaming workloads
tables_to_optimize = [
    f"{BRONZE_DB}.claims_raw",
    f"{SILVER_DB}.claims",
    f"{SILVER_DB}.policies",
    f"{GOLD_DB}.daily_claims_summary"
]

for table in tables_to_optimize:
    try:
        spark.sql(f"OPTIMIZE {table}")
        print(f"✅ Optimized {table}")
    except:
        print(f"⚠️ Table {table} not yet populated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Display created tables
print("\n📊 Database Summary:")
print("=" * 50)

for db in [BRONZE_DB, SILVER_DB, GOLD_DB]:
    tables = spark.sql(f"SHOW TABLES IN {db}").collect()
    print(f"\n{db}:")
    for table in tables:
        print(f"  - {table.tableName}")

print("\n✅ All tables created successfully!")