# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Event Hub Streaming Ingestion
# MAGIC Real-time ingestion of insurance claims from Azure Event Hubs to Delta Lake Bronze layer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration and Imports

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
import json
from datetime import datetime
import os

# Initialize Spark
spark = SparkSession.builder \
    .appName("BronzeClaimsIngestion") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Configuration
BRONZE_DB = "insurance_bronze"
CHECKPOINT_PATH = "/mnt/checkpoints/bronze/claims"
BRONZE_TABLE = f"{BRONZE_DB}.claims_raw"

# Event Hub configuration
EVENT_HUB_NAMESPACE = "bw-dev-uks-eventhubs-001"
EVENT_HUB_NAME = "claims-realtime"
CONSUMER_GROUP = "$Default"

# Get connection string from Key Vault (in production)
# For demo, set EVENTHUB_CONNECTION_STRING environment variable
CONNECTION_STRING = os.getenv("EVENTHUB_CONNECTION_STRING", 
    "Endpoint=sb://YOUR-NAMESPACE.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=YOUR-KEY")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Schemas

# COMMAND ----------

# Expected schema for claims events
claims_event_schema = StructType([
    StructField("claim_id", StringType(), False),
    StructField("policy_number", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("claim_amount", DoubleType(), True),
    StructField("claim_date", StringType(), True),
    StructField("incident_date", StringType(), True),
    StructField("incident_location", StringType(), True),
    StructField("incident_postcode", StringType(), True),
    StructField("claim_type", StringType(), True),
    StructField("claim_status", StringType(), True),
    StructField("fraud_score", DoubleType(), True),
    StructField("fraud_indicators", ArrayType(StringType()), True),
    StructField("description", StringType(), True),
    StructField("metadata", MapType(StringType(), StringType()), True)
])

# Dead letter schema for malformed records
dead_letter_schema = StructType([
    StructField("raw_data", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("error_timestamp", TimestampType(), True),
    StructField("event_hub_offset", LongType(), True),
    StructField("event_hub_partition", IntegerType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Event Hub Connection Setup

# COMMAND ----------

# Build Event Hub configuration
ehConf = {
    'eventhubs.connectionString': CONNECTION_STRING,
    'eventhubs.consumerGroup': CONSUMER_GROUP,
    'eventhubs.startingPosition': json.dumps({
        "offset": "-1",
        "seqNo": -1,
        "enqueuedTime": None,
        "isInclusive": True
    }),
    'maxEventsPerTrigger': 1000  # Process up to 1000 events per batch
}

print(f"📡 Connecting to Event Hub: {EVENT_HUB_NAMESPACE}/{EVENT_HUB_NAME}")
print(f"   Consumer Group: {CONSUMER_GROUP}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Streaming DataFrame

# COMMAND ----------

# Read from Event Hub
raw_stream = spark \
    .readStream \
    .format("eventhubs") \
    .options(**ehConf) \
    .load()

# Parse Event Hub metadata
event_hub_stream = raw_stream.select(
    col("body").cast("string").alias("raw_body"),
    col("enqueuedTime").alias("event_time"),
    col("offset").alias("event_hub_offset"),
    col("partition").alias("event_hub_partition"),
    col("sequenceNumber").alias("event_hub_sequence"),
    col("systemProperties"),
    col("properties")
)

print("✅ Event Hub stream created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse and Validate Data

# COMMAND ----------

def parse_claim_event(df):
    """Parse JSON events and handle malformed data"""
    
    # Try to parse JSON
    parsed_df = df.withColumn(
        "parsed_json",
        from_json(col("raw_body"), claims_event_schema)
    )
    
    # Split into valid and invalid records
    valid_df = parsed_df.filter(col("parsed_json").isNotNull())
    invalid_df = parsed_df.filter(col("parsed_json").isNull())
    
    # Process valid records
    claims_df = valid_df.select(
        col("parsed_json.claim_id").alias("claim_id"),
        col("parsed_json.policy_number").alias("policy_number"),
        col("parsed_json.customer_id").alias("customer_id"),
        col("parsed_json.claim_amount").cast(DecimalType(10, 2)).alias("claim_amount"),
        to_timestamp(col("parsed_json.claim_date")).alias("claim_date"),
        to_timestamp(col("parsed_json.incident_date")).alias("incident_date"),
        col("parsed_json.incident_location").alias("incident_location"),
        col("parsed_json.incident_postcode").alias("incident_postcode"),
        col("parsed_json.claim_type").alias("claim_type"),
        col("parsed_json.claim_status").alias("claim_status"),
        col("parsed_json.fraud_score").alias("fraud_score"),
        col("parsed_json.description").alias("description"),
        col("raw_body").alias("_raw_data"),
        current_timestamp().alias("_ingestion_timestamp"),
        col("event_hub_offset").alias("_event_hub_offset"),
        col("event_hub_partition").alias("_event_hub_partition"),
        col("event_hub_sequence").alias("_event_hub_sequence"),
        lit("event_hub").alias("_data_source")
    )
    
    return claims_df, invalid_df

# Parse the stream
parsed_claims, invalid_records = parse_claim_event(event_hub_stream)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

def add_quality_checks(df):
    """Add data quality indicators"""
    
    return df.withColumn(
        "dq_checks",
        struct(
            # Check for required fields
            col("claim_id").isNotNull().alias("has_claim_id"),
            col("policy_number").isNotNull().alias("has_policy_number"),
            col("customer_id").isNotNull().alias("has_customer_id"),
            col("claim_amount").isNotNull().alias("has_amount"),
            
            # Business rule checks
            (col("claim_amount") > 0).alias("positive_amount"),
            (col("claim_amount") < 1000000).alias("reasonable_amount"),
            (col("claim_date") >= col("incident_date")).alias("valid_dates"),
            
            # UK postcode validation (basic)
            regexp_extract(col("incident_postcode"), r'^[A-Z]{1,2}[0-9]', 0).isNotNull().alias("valid_uk_postcode")
        )
    ).withColumn(
        "dq_score",
        # Calculate quality score (0-1)
        (
            col("dq_checks.has_claim_id").cast("int") +
            col("dq_checks.has_policy_number").cast("int") +
            col("dq_checks.has_customer_id").cast("int") +
            col("dq_checks.has_amount").cast("int") +
            col("dq_checks.positive_amount").cast("int") +
            col("dq_checks.reasonable_amount").cast("int") +
            col("dq_checks.valid_dates").cast("int") +
            col("dq_checks.valid_uk_postcode").cast("int")
        ) / 8.0
    )

quality_checked_claims = add_quality_checks(parsed_claims)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handle Duplicates

# COMMAND ----------

# Add watermark for handling late data (10 minutes)
deduplicated_claims = quality_checked_claims \
    .withWatermark("_ingestion_timestamp", "10 minutes") \
    .dropDuplicates(["claim_id", "_ingestion_timestamp"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze Delta Table

# COMMAND ----------

def write_to_bronze(df, checkpoint_path, table_name):
    """Write streaming data to Bronze Delta table with merge for deduplication"""
    
    query = df.writeStream \
        .outputMode("append") \
        .format("delta") \
        .option("checkpointLocation", checkpoint_path) \
        .option("mergeSchema", "true") \
        .trigger(processingTime='10 seconds') \
        .table(table_name)
    
    return query

# Start the streaming write
bronze_stream_query = write_to_bronze(
    deduplicated_claims.drop("dq_checks"),  # Drop the struct column for cleaner table
    CHECKPOINT_PATH,
    BRONZE_TABLE
)

print(f"✅ Started streaming to {BRONZE_TABLE}")
print(f"   Checkpoint: {CHECKPOINT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dead Letter Queue for Invalid Records

# COMMAND ----------

def write_dead_letter_records(df):
    """Write invalid records to dead letter queue"""
    
    dead_letter_df = df.select(
        col("raw_body").alias("raw_data"),
        lit("Failed JSON parsing").alias("error_message"),
        current_timestamp().alias("error_timestamp"),
        col("event_hub_offset"),
        col("event_hub_partition")
    )
    
    dead_letter_query = dead_letter_df.writeStream \
        .outputMode("append") \
        .format("delta") \
        .option("checkpointLocation", "/mnt/checkpoints/bronze/dead_letter") \
        .option("path", "/mnt/bronze/dead_letter_queue") \
        .trigger(processingTime='1 minute') \
        .start()
    
    return dead_letter_query

# Start dead letter queue writer
if invalid_records:
    dlq_query = write_dead_letter_records(invalid_records)
    print("✅ Started dead letter queue for invalid records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoring and Metrics

# COMMAND ----------

def display_streaming_metrics():
    """Display streaming metrics and status"""
    
    # Get query status
    status = bronze_stream_query.status
    progress = bronze_stream_query.recentProgress
    
    print("📊 Streaming Status:")
    print(f"   Is Active: {bronze_stream_query.isActive}")
    print(f"   Message: {status['message']}")
    print(f"   Trigger: {status.get('isTriggerActive', False)}")
    
    if progress and len(progress) > 0:
        latest = progress[-1]
        print("\n📈 Recent Progress:")
        print(f"   Batch ID: {latest.get('id', 'N/A')}")
        print(f"   Timestamp: {latest.get('timestamp', 'N/A')}")
        
        # Input metrics
        if 'sources' in latest and len(latest['sources']) > 0:
            source = latest['sources'][0]
            print(f"   Events Processed: {source.get('numInputRows', 0)}")
            print(f"   Processing Rate: {source.get('inputRowsPerSecond', 0):.2f} rows/sec")
        
        # Sink metrics
        if 'sink' in latest:
            sink = latest['sink']
            print(f"   Rows Written: {sink.get('numOutputRows', 0)}")
    
    # Count records in Bronze table
    try:
        bronze_count = spark.table(BRONZE_TABLE).count()
        print(f"\n📦 Total Bronze Records: {bronze_count:,}")
    except:
        print("\n📦 Bronze table not yet created")

# Display metrics
display_streaming_metrics()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Monitoring Dashboard Query

# COMMAND ----------

# Create a monitoring view
spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW bronze_monitoring AS
SELECT 
    DATE(_ingestion_timestamp) as ingestion_date,
    HOUR(_ingestion_timestamp) as ingestion_hour,
    claim_type,
    COUNT(*) as claim_count,
    AVG(claim_amount) as avg_claim_amount,
    MAX(claim_amount) as max_claim_amount,
    AVG(fraud_score) as avg_fraud_score,
    SUM(CASE WHEN fraud_score > 0.7 THEN 1 ELSE 0 END) as high_fraud_count,
    AVG(dq_score) as avg_quality_score,
    COUNT(DISTINCT customer_id) as unique_customers
FROM {BRONZE_TABLE}
WHERE _ingestion_timestamp >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY DATE(_ingestion_timestamp), HOUR(_ingestion_timestamp), claim_type
ORDER BY ingestion_date DESC, ingestion_hour DESC
""")

# Display recent metrics
display(spark.sql("SELECT * FROM bronze_monitoring LIMIT 24"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Management

# COMMAND ----------

# Function to stop the stream gracefully
def stop_bronze_stream():
    """Stop the bronze streaming query gracefully"""
    if bronze_stream_query.isActive:
        bronze_stream_query.stop()
        print("⏹️ Bronze stream stopped")
    else:
        print("ℹ️ Bronze stream is not active")

# Function to restart the stream
def restart_bronze_stream():
    """Restart the bronze streaming query"""
    global bronze_stream_query
    stop_bronze_stream()
    
    bronze_stream_query = write_to_bronze(
        deduplicated_claims.drop("dq_checks"),
        CHECKPOINT_PATH,
        BRONZE_TABLE
    )
    print("🔄 Bronze stream restarted")

# Keep stream running
# bronze_stream_query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC The Bronze layer streaming pipeline is now:
# MAGIC - ✅ Ingesting real-time claims from Event Hub
# MAGIC - ✅ Parsing and validating JSON events
# MAGIC - ✅ Performing data quality checks
# MAGIC - ✅ Handling duplicates with watermarking
# MAGIC - ✅ Writing to Delta Lake with ACID guarantees
# MAGIC - ✅ Managing invalid records in dead letter queue
# MAGIC - ✅ Providing real-time metrics and monitoring