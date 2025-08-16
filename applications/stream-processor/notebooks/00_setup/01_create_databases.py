# Databricks notebook source
# MAGIC %md
# MAGIC # Create Database Schemas
# MAGIC Cost-optimized setup for demo environment

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create databases if not exist
# MAGIC CREATE DATABASE IF NOT EXISTS bronze COMMENT 'Raw data layer';
# MAGIC CREATE DATABASE IF NOT EXISTS silver COMMENT 'Cleaned and enriched data';
# MAGIC CREATE DATABASE IF NOT EXISTS gold COMMENT 'Business-ready aggregations';
# MAGIC CREATE DATABASE IF NOT EXISTS gdpr COMMENT 'GDPR compliance data';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show databases
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create bronze claims table
# MAGIC CREATE TABLE IF NOT EXISTS bronze.claims_raw (
# MAGIC     claim_id STRING,
# MAGIC     policy_number STRING,
# MAGIC     customer_id STRING,
# MAGIC     customer_name STRING,
# MAGIC     customer_email STRING,
# MAGIC     customer_phone STRING,
# MAGIC     claim_amount DECIMAL(10,2),
# MAGIC     claim_date TIMESTAMP,
# MAGIC     claim_type STRING,
# MAGIC     incident_location STRING,
# MAGIC     postcode STRING,
# MAGIC     fraud_score DOUBLE,
# MAGIC     ingestion_time TIMESTAMP,
# MAGIC     offset LONG,
# MAGIC     partition_id INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (DATE(claim_date))
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create silver claims table
# MAGIC CREATE TABLE IF NOT EXISTS silver.claims_processed (
# MAGIC     claim_id STRING,
# MAGIC     policy_number STRING,
# MAGIC     customer_id STRING,
# MAGIC     customer_name STRING,
# MAGIC     customer_email STRING,
# MAGIC     customer_phone STRING,
# MAGIC     customer_address STRING,
# MAGIC     claim_amount DECIMAL(10,2),
# MAGIC     claim_date TIMESTAMP,
# MAGIC     claim_type STRING,
# MAGIC     claim_status STRING,
# MAGIC     incident_location STRING,
# MAGIC     incident_description STRING,
# MAGIC     claim_notes STRING,
# MAGIC     postcode STRING,
# MAGIC     uk_region STRING,
# MAGIC     fraud_score DOUBLE,
# MAGIC     high_value_claim BOOLEAN,
# MAGIC     suspicious_timing BOOLEAN,
# MAGIC     is_suspicious BOOLEAN,
# MAGIC     risk_level STRING,
# MAGIC     processed_time TIMESTAMP,
# MAGIC     processing_date DATE
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (processing_date)
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.deletedFileRetentionDuration' = 'interval 1 days'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create GDPR PII classified table
# MAGIC CREATE TABLE IF NOT EXISTS gdpr.pii_classified_claims (
# MAGIC     claim_id STRING,
# MAGIC     customer_id STRING,
# MAGIC     pii_scan_timestamp TIMESTAMP,
# MAGIC     notes_pii STRING,
# MAGIC     description_pii STRING,
# MAGIC     address_pii STRING,
# MAGIC     combined_sensitivity STRING,
# MAGIC     contains_pii BOOLEAN,
# MAGIC     gdpr_risk_score DOUBLE,
# MAGIC     processing_date DATE
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (processing_date);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create GDPR audit log table
# MAGIC CREATE TABLE IF NOT EXISTS gdpr.audit_log (
# MAGIC     user_id STRING,
# MAGIC     action STRING,
# MAGIC     resource STRING,
# MAGIC     timestamp TIMESTAMP,
# MAGIC     access_level STRING,
# MAGIC     data_categories STRING,
# MAGIC     purpose STRING,
# MAGIC     legal_basis STRING,
# MAGIC     ip_address STRING,
# MAGIC     success BOOLEAN,
# MAGIC     details STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (DATE(timestamp))
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.logRetentionDuration' = 'interval 90 days',
# MAGIC     'delta.deletedFileRetentionDuration' = 'interval 7 days'
# MAGIC );

# COMMAND ----------

print("✅ All databases and tables created successfully")
print("💰 Tables optimized for cost with:")
print("   - Auto-compaction enabled")
print("   - Minimal retention periods")
print("   - Partitioning for efficient queries")