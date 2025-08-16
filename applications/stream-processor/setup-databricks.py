#!/usr/bin/env python3
"""
Databricks Setup Script for BlueWave Insurance Platform
Sets up notebooks, cluster, and configurations for demo environment
"""

import os
import json
import time
import base64
from pathlib import Path

print("""
================================================
🚀 DATABRICKS SETUP FOR BLUEWAVE DEMO
================================================
""")

# Configuration
WORKSPACE_URL = os.getenv("DATABRICKS_HOST", "https://adb-xxx.azuredatabricks.net")
TOKEN = os.getenv("DATABRICKS_TOKEN", "")

print(f"Workspace URL: {WORKSPACE_URL}")

if not TOKEN:
    print("""
⚠️  DATABRICKS TOKEN NOT SET!

To get your token:
1. Go to your Databricks workspace
2. Click on your username in the top right
3. Select 'User Settings'
4. Go to 'Access Tokens' tab
5. Generate new token
6. Set environment variable:
   export DATABRICKS_TOKEN="dapi..."
""")
    exit(1)

# Cost-optimized cluster configuration
CLUSTER_CONFIG = {
    "cluster_name": "demo-streaming-cluster",
    "spark_version": "13.3.x-scala2.12",
    "node_type_id": "Standard_F4s",  # 4 cores, 8GB RAM, ~$0.20/hour
    "driver_node_type_id": "Standard_F4s",
    "num_workers": 0,  # Single node for demo
    "autotermination_minutes": 10,  # Auto-terminate after 10 mins idle
    "spark_conf": {
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "spark.databricks.delta.autoCompact.enabled": "true",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.databricks.cluster.profile": "singleNode",
        "spark.master": "local[*]"
    },
    "azure_attributes": {
        "availability": "SPOT_AZURE",  # Use spot instances for cost savings
        "first_on_demand": 0,
        "spot_bid_max_price": -1  # Use default spot pricing
    },
    "custom_tags": {
        "Environment": "demo",
        "AutoTerminate": "true",
        "CostOptimized": "true",
        "Purpose": "streaming-demo"
    },
    "enable_elastic_disk": False,
    "enable_local_disk_encryption": False
}

# Notebook content to create
NOTEBOOKS = {
    "00_setup/create_tables": """# Databricks notebook source
# MAGIC %md
# MAGIC # Create Delta Tables for Insurance Claims

# COMMAND ----------

# Create databases
spark.sql("CREATE DATABASE IF NOT EXISTS insurance_bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS insurance_silver")
spark.sql("CREATE DATABASE IF NOT EXISTS insurance_gold")

# COMMAND ----------

# Bronze layer - raw claims
spark.sql('''
CREATE TABLE IF NOT EXISTS insurance_bronze.claims_raw (
    claim_id STRING,
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
    _ingestion_timestamp TIMESTAMP,
    _event_hub_offset BIGINT,
    _event_hub_partition INT,
    _data_source STRING
)
USING DELTA
PARTITIONED BY (DATE(claim_date))
LOCATION '/mnt/bronze/claims_raw'
''')

print("✅ Bronze tables created")

# COMMAND ----------

# Silver layer - validated claims
spark.sql('''
CREATE TABLE IF NOT EXISTS insurance_silver.claims (
    claim_id STRING,
    policy_number STRING,
    customer_id STRING,
    claim_amount DECIMAL(10,2),
    claim_amount_gbp DECIMAL(10,2),
    claim_date TIMESTAMP,
    incident_date TIMESTAMP,
    incident_location STRING,
    incident_postcode STRING,
    postcode_district STRING,
    claim_type STRING,
    claim_status STRING,
    fraud_score DOUBLE,
    fraud_indicators ARRAY<STRING>,
    is_high_risk BOOLEAN,
    requires_manual_review BOOLEAN,
    days_to_claim INT,
    processing_status STRING,
    data_quality_score DOUBLE,
    _silver_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (DATE(claim_date), claim_type)
LOCATION '/mnt/silver/claims'
''')

print("✅ Silver tables created")

# COMMAND ----------

# Gold layer - aggregations
spark.sql('''
CREATE TABLE IF NOT EXISTS insurance_gold.daily_claims_summary (
    summary_date DATE,
    claim_type STRING,
    total_claims INT,
    total_amount DECIMAL(12,2),
    average_amount DECIMAL(10,2),
    fraud_detected_count INT,
    fraud_detected_amount DECIMAL(12,2),
    auto_approved_count INT,
    manual_review_count INT,
    processing_time_avg_ms BIGINT,
    _gold_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (summary_date)
LOCATION '/mnt/gold/daily_claims_summary'
''')

print("✅ Gold tables created")

# COMMAND ----------

print("✅ All tables created successfully!")
""",

    "01_bronze/event_hub_ingestion": """# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Event Hub Ingestion

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

# Event Hub configuration
CONNECTION_STRING = dbutils.secrets.get(scope="bluewave-demo", key="eventhub-connection-string")
EVENT_HUB_NAME = "claims-realtime"

ehConf = {
    'eventhubs.connectionString': CONNECTION_STRING,
    'eventhubs.consumerGroup': '$Default',
    'maxEventsPerTrigger': 100  # Process 100 events per batch for demo
}

# COMMAND ----------

# Read from Event Hub
raw_stream = spark \\
    .readStream \\
    .format("eventhubs") \\
    .options(**ehConf) \\
    .load()

# Parse JSON
parsed_stream = raw_stream.select(
    get_json_object(col("body").cast("string"), "$.claim_id").alias("claim_id"),
    get_json_object(col("body").cast("string"), "$.policy_number").alias("policy_number"),
    get_json_object(col("body").cast("string"), "$.customer_id").alias("customer_id"),
    get_json_object(col("body").cast("string"), "$.claim_amount").cast("decimal(10,2)").alias("claim_amount"),
    get_json_object(col("body").cast("string"), "$.claim_date").cast("timestamp").alias("claim_date"),
    get_json_object(col("body").cast("string"), "$.incident_date").cast("timestamp").alias("incident_date"),
    get_json_object(col("body").cast("string"), "$.incident_location").alias("incident_location"),
    get_json_object(col("body").cast("string"), "$.incident_postcode").alias("incident_postcode"),
    get_json_object(col("body").cast("string"), "$.claim_type").alias("claim_type"),
    get_json_object(col("body").cast("string"), "$.claim_status").alias("claim_status"),
    get_json_object(col("body").cast("string"), "$.fraud_score").cast("double").alias("fraud_score"),
    get_json_object(col("body").cast("string"), "$.description").alias("description"),
    col("body").cast("string").alias("_raw_data"),
    current_timestamp().alias("_ingestion_timestamp"),
    col("offset").alias("_event_hub_offset"),
    col("partition").alias("_event_hub_partition"),
    lit("event_hub").alias("_data_source")
)

# COMMAND ----------

# Write to Bronze Delta table
bronze_query = parsed_stream.writeStream \\
    .outputMode("append") \\
    .format("delta") \\
    .option("checkpointLocation", "/mnt/checkpoints/bronze/claims") \\
    .trigger(processingTime='10 seconds') \\
    .table("insurance_bronze.claims_raw")

print("✅ Started streaming to Bronze layer")

# COMMAND ----------

# Display streaming metrics
display(spark.sql("SELECT COUNT(*) as total_claims FROM insurance_bronze.claims_raw"))
""",

    "02_silver/business_transformations": """# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Business Transformations

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Read from Bronze
bronze_stream = spark.readStream \\
    .format("delta") \\
    .table("insurance_bronze.claims_raw")

# COMMAND ----------

# Apply business transformations
silver_stream = bronze_stream \\
    .withColumn("claim_amount_gbp", col("claim_amount")) \\
    .withColumn("days_to_claim", datediff(col("claim_date"), col("incident_date"))) \\
    .withColumn("postcode_district", regexp_extract(col("incident_postcode"), r'^([A-Z]{1,2}\\d{1,2})', 1)) \\
    .withColumn("is_high_risk", col("fraud_score") > 0.7) \\
    .withColumn("requires_manual_review", 
        (col("fraud_score") > 0.5) | (col("claim_amount") > 10000)) \\
    .withColumn("processing_status",
        when(col("fraud_score") > 0.7, "FRAUD_INVESTIGATION")
        .when(col("claim_amount") > 25000, "MANUAL_REVIEW")
        .otherwise("AUTO_APPROVED")) \\
    .withColumn("fraud_indicators",
        array_remove(array(
            when(col("days_to_claim") < 30, lit("QUICK_CLAIM")).otherwise(lit(None)),
            when(col("claim_amount") > 50000, lit("HIGH_VALUE")).otherwise(lit(None))
        ), None)) \\
    .withColumn("data_quality_score", lit(0.95)) \\
    .withColumn("_silver_timestamp", current_timestamp())

# COMMAND ----------

# Write to Silver Delta table
silver_query = silver_stream.writeStream \\
    .outputMode("append") \\
    .format("delta") \\
    .option("checkpointLocation", "/mnt/checkpoints/silver/claims") \\
    .trigger(processingTime='30 seconds') \\
    .table("insurance_silver.claims")

print("✅ Started streaming to Silver layer")

# COMMAND ----------

# Display processing metrics
display(spark.sql('''
SELECT 
    processing_status,
    COUNT(*) as count,
    AVG(claim_amount_gbp) as avg_amount
FROM insurance_silver.claims
GROUP BY processing_status
'''))
""",

    "03_gold/aggregations": """# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Business Aggregations

# COMMAND ----------

from pyspark.sql.functions import *

# Read from Silver
silver_stream = spark.readStream \\
    .format("delta") \\
    .table("insurance_silver.claims") \\
    .withWatermark("_silver_timestamp", "10 minutes")

# COMMAND ----------

# Create daily summary
daily_summary = silver_stream \\
    .withColumn("summary_date", to_date(col("claim_date"))) \\
    .groupBy("summary_date", "claim_type") \\
    .agg(
        count("*").alias("total_claims"),
        sum("claim_amount_gbp").alias("total_amount"),
        avg("claim_amount_gbp").alias("average_amount"),
        sum(when(col("is_high_risk"), 1).otherwise(0)).alias("fraud_detected_count"),
        sum(when(col("is_high_risk"), col("claim_amount_gbp")).otherwise(0)).alias("fraud_detected_amount"),
        sum(when(col("processing_status") == "AUTO_APPROVED", 1).otherwise(0)).alias("auto_approved_count"),
        sum(when(col("requires_manual_review"), 1).otherwise(0)).alias("manual_review_count")
    ) \\
    .withColumn("_gold_timestamp", current_timestamp())

# COMMAND ----------

# Write to Gold Delta table
gold_query = daily_summary.writeStream \\
    .outputMode("complete") \\
    .format("delta") \\
    .option("checkpointLocation", "/mnt/checkpoints/gold/daily_summary") \\
    .trigger(processingTime='1 minute') \\
    .table("insurance_gold.daily_claims_summary")

print("✅ Started aggregations to Gold layer")

# COMMAND ----------

# Display KPIs
display(spark.sql('''
SELECT 
    summary_date,
    SUM(total_claims) as total_claims,
    SUM(total_amount) as total_value,
    AVG(average_amount) as avg_claim,
    SUM(fraud_detected_count) as fraud_cases,
    SUM(auto_approved_count) * 100.0 / SUM(total_claims) as auto_approval_rate
FROM insurance_gold.daily_claims_summary
WHERE summary_date >= current_date() - 7
GROUP BY summary_date
ORDER BY summary_date DESC
'''))
"""
}

# Create notebooks directory structure
notebooks_dir = Path("databricks_notebooks")
notebooks_dir.mkdir(exist_ok=True)

print("\n📁 Creating notebook files...")
for notebook_path, content in NOTEBOOKS.items():
    file_path = notebooks_dir / f"{notebook_path}.py"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(content)
    print(f"   ✓ Created {notebook_path}.py")

print(f"""
================================================
✅ SETUP COMPLETE!
================================================

📁 Notebooks created in: {notebooks_dir}

🚀 NEXT STEPS:
-------------
1. Upload notebooks to Databricks:
   - Go to {WORKSPACE_URL}
   - Create a folder: /Users/<your-email>/bluewave-demo
   - Import the notebooks from {notebooks_dir}

2. Create cluster manually:
   - Name: demo-streaming-cluster
   - Mode: Single Node
   - Node type: Standard_F4s
   - Auto-terminate: 10 minutes
   - Spot instances: Enabled

3. Add Event Hub secret:
   - Create secret scope: bluewave-demo
   - Add secret: eventhub-connection-string

4. Run notebooks in order:
   - 00_setup/create_tables
   - 01_bronze/event_hub_ingestion
   - 02_silver/business_transformations
   - 03_gold/aggregations

💰 COST ESTIMATE:
----------------
- Cluster: ~$0.20/hour (with spot)
- Storage: ~$0.02/GB/month
- Total for 2-hour demo: ~$0.50

⚠️  IMPORTANT:
--------------
- ALWAYS terminate cluster after demo
- Delete checkpoint folders to reset
- Use cached data when possible
""")

# Create a simple cluster creation script
cluster_script = """
# Databricks CLI commands to create cluster
databricks clusters create --json '{
    "cluster_name": "demo-streaming-cluster",
    "spark_version": "13.3.x-scala2.12",
    "node_type_id": "Standard_F4s",
    "num_workers": 0,
    "autotermination_minutes": 10,
    "spark_conf": {
        "spark.databricks.cluster.profile": "singleNode",
        "spark.master": "local[*]"
    }
}'
"""

(notebooks_dir / "create_cluster.sh").write_text(cluster_script)
print("\n📝 Cluster creation script saved to: databricks_notebooks/create_cluster.sh")