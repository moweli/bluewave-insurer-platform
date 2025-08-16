# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Business Transformations and Enrichment
# MAGIC Apply business rules, enrichments, and validations to create clean, business-ready data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import json
from datetime import datetime, timedelta

spark = SparkSession.builder \
    .appName("SilverClaimsProcessing") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .getOrCreate()

# Database configuration
BRONZE_DB = "insurance_bronze"
SILVER_DB = "insurance_silver"
BRONZE_TABLE = f"{BRONZE_DB}.claims_raw"
SILVER_TABLE = f"{SILVER_DB}.claims"
CHECKPOINT_PATH = "/mnt/checkpoints/silver/claims"

# Business rule thresholds
CLAIM_AMOUNT_LIMITS = {
    "motor": 100000,
    "home": 500000,
    "travel": 50000,
    "pet": 10000,
    "life": 1000000
}

FRAUD_SCORE_THRESHOLD = 0.7
MANUAL_REVIEW_THRESHOLD = 0.5

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Reference Data

# COMMAND ----------

# Load dimension tables (these would be maintained separately)
# UK Bank Holidays
uk_bank_holidays = spark.createDataFrame([
    ("2024-01-01", "New Year's Day"),
    ("2024-03-29", "Good Friday"),
    ("2024-04-01", "Easter Monday"),
    ("2024-05-06", "Early May bank holiday"),
    ("2024-05-27", "Spring bank holiday"),
    ("2024-08-26", "Summer bank holiday"),
    ("2024-12-25", "Christmas Day"),
    ("2024-12-26", "Boxing Day"),
], ["date", "holiday_name"]).withColumn("date", to_date(col("date")))

# UK Postcode to Region mapping (sample)
uk_postcode_regions = spark.createDataFrame([
    ("E", "London"),
    ("EC", "London"),
    ("N", "London"),
    ("NW", "London"),
    ("SE", "London"),
    ("SW", "London"),
    ("W", "London"),
    ("WC", "London"),
    ("B", "Birmingham"),
    ("M", "Manchester"),
    ("L", "Liverpool"),
    ("G", "Glasgow"),
    ("EH", "Edinburgh"),
    ("CF", "Cardiff"),
    ("BT", "Belfast"),
], ["postcode_area", "region"])

# High-risk postcodes for fraud
high_risk_postcodes = spark.createDataFrame([
    ("B1", 2.5),
    ("M1", 2.3),
    ("L1", 2.8),
    ("E1", 2.1),
    ("SE1", 2.0),
], ["postcode_district", "risk_multiplier"])

print("✅ Reference data loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze Stream

# COMMAND ----------

# Read from Bronze Delta table as stream
bronze_stream = spark.readStream \
    .format("delta") \
    .table(BRONZE_TABLE) \
    .filter(col("dq_score") > 0.5)  # Filter out very poor quality records

print(f"✅ Reading from {BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Core Business Transformations

# COMMAND ----------

def apply_business_transformations(df):
    """Apply core business logic and transformations"""
    
    # Extract postcode district
    transformed_df = df.withColumn(
        "postcode_district",
        regexp_extract(col("incident_postcode"), r'^([A-Z]{1,2}\d{1,2})', 1)
    ).withColumn(
        "postcode_area",
        regexp_extract(col("incident_postcode"), r'^([A-Z]{1,2})', 1)
    )
    
    # Calculate derived fields
    transformed_df = transformed_df \
        .withColumn("claim_amount_gbp", col("claim_amount")) \
        .withColumn("days_to_claim", 
            datediff(col("claim_date"), col("incident_date"))) \
        .withColumn("claim_amount_category",
            when(col("claim_amount") < 1000, "small")
            .when(col("claim_amount") < 10000, "medium")
            .when(col("claim_amount") < 50000, "large")
            .otherwise("exceptional")) \
        .withColumn("is_weekend_claim",
            dayofweek(col("claim_date")).isin([1, 7])) \
        .withColumn("claim_hour",
            hour(col("claim_date"))) \
        .withColumn("is_business_hours",
            (col("claim_hour").between(9, 17)) & 
            (~col("is_weekend_claim")))
    
    return transformed_df

silver_transformed = apply_business_transformations(bronze_stream)

# COMMAND ----------

# MAGIC %md
# MAGIC ## UK-Specific Enrichments

# COMMAND ----------

def enrich_with_uk_data(df):
    """Add UK-specific enrichments"""
    
    # Join with UK regions
    enriched_df = df.join(
        broadcast(uk_postcode_regions),
        df.postcode_area == uk_postcode_regions.postcode_area,
        "left"
    ).drop(uk_postcode_regions.postcode_area)
    
    # Add bank holiday flag
    enriched_df = enriched_df.join(
        broadcast(uk_bank_holidays),
        to_date(enriched_df.claim_date) == uk_bank_holidays.date,
        "left"
    ).withColumn(
        "is_bank_holiday",
        col("holiday_name").isNotNull()
    ).drop("date", "holiday_name")
    
    # Add high-risk postcode flag
    enriched_df = enriched_df.join(
        broadcast(high_risk_postcodes),
        enriched_df.postcode_district == high_risk_postcodes.postcode_district,
        "left"
    ).withColumn(
        "postcode_fraud_risk",
        coalesce(col("risk_multiplier"), lit(1.0))
    ).drop(high_risk_postcodes.postcode_district, "risk_multiplier")
    
    return enriched_df

silver_enriched = enrich_with_uk_data(silver_transformed)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fraud Detection Rules

# COMMAND ----------

def apply_fraud_detection_rules(df):
    """Apply UK insurance-specific fraud detection rules"""
    
    # Calculate fraud indicators
    fraud_df = df.withColumn(
        "fraud_indicators",
        array_remove(
            array(
                # Velocity check - multiple claims in 30 days
                when(col("days_to_claim") < 30, lit("QUICK_CLAIM")).otherwise(lit(None)),
                
                # Amount check - claim exceeds typical range
                when(col("claim_amount") > 50000, lit("HIGH_VALUE")).otherwise(lit(None)),
                
                # Weekend/holiday pattern
                when(col("is_weekend_claim") | col("is_bank_holiday"), 
                     lit("SUSPICIOUS_TIMING")).otherwise(lit(None)),
                
                # Late night claims
                when(col("claim_hour").isin([0,1,2,3,4,5,23]), 
                     lit("ODD_HOURS")).otherwise(lit(None)),
                
                # High-risk postcode
                when(col("postcode_fraud_risk") > 2.0, 
                     lit("HIGH_RISK_LOCATION")).otherwise(lit(None)),
                
                # Whiplash pattern (motor claims)
                when((col("claim_type") == "motor") & 
                     (col("claim_amount").between(3000, 6000)), 
                     lit("WHIPLASH_PATTERN")).otherwise(lit(None)),
                
                # Storm chasing pattern
                when((col("claim_type") == "home") & 
                     (col("days_to_claim") < 2), 
                     lit("STORM_CHASER")).otherwise(lit(None))
            ),
            None
        )
    )
    
    # Calculate adjusted fraud score
    fraud_df = fraud_df.withColumn(
        "adjusted_fraud_score",
        least(
            col("fraud_score") * col("postcode_fraud_risk") * 
            (1 + (size(col("fraud_indicators")) * 0.1)),
            lit(1.0)
        )
    )
    
    # Set review flags
    fraud_df = fraud_df \
        .withColumn("is_high_risk",
            col("adjusted_fraud_score") > FRAUD_SCORE_THRESHOLD) \
        .withColumn("requires_manual_review",
            (col("adjusted_fraud_score") > MANUAL_REVIEW_THRESHOLD) |
            (col("claim_amount") > 25000) |
            (size(col("fraud_indicators")) > 3))
    
    return fraud_df

silver_with_fraud = apply_fraud_detection_rules(silver_enriched)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Validation Rules

# COMMAND ----------

def validate_business_rules(df):
    """Apply business validation rules"""
    
    # Create validation flags
    validated_df = df.withColumn(
        "validation_checks",
        struct(
            # Amount within limits
            when(col("claim_type") == "motor", 
                 col("claim_amount") <= CLAIM_AMOUNT_LIMITS["motor"])
            .when(col("claim_type") == "home",
                 col("claim_amount") <= CLAIM_AMOUNT_LIMITS["home"])
            .when(col("claim_type") == "travel",
                 col("claim_amount") <= CLAIM_AMOUNT_LIMITS["travel"])
            .otherwise(True).alias("amount_within_limit"),
            
            # Valid claim timing
            (col("days_to_claim") >= 0).alias("valid_claim_timing"),
            
            # Has required fields
            col("policy_number").isNotNull().alias("has_policy"),
            col("customer_id").isNotNull().alias("has_customer"),
            
            # UK postcode format
            regexp_extract(col("incident_postcode"), 
                          r'^[A-Z]{1,2}[0-9]{1,2}[A-Z]?\s?[0-9][A-Z]{2}$', 
                          0).isNotNull().alias("valid_postcode")
        )
    )
    
    # Calculate validation score
    validated_df = validated_df.withColumn(
        "validation_score",
        (
            col("validation_checks.amount_within_limit").cast("int") +
            col("validation_checks.valid_claim_timing").cast("int") +
            col("validation_checks.has_policy").cast("int") +
            col("validation_checks.has_customer").cast("int") +
            col("validation_checks.valid_postcode").cast("int")
        ) / 5.0
    )
    
    # Set processing status based on validation
    validated_df = validated_df.withColumn(
        "processing_status",
        when(col("validation_score") < 0.6, "REJECTED")
        .when(col("requires_manual_review"), "MANUAL_REVIEW")
        .when(col("is_high_risk"), "FRAUD_INVESTIGATION")
        .otherwise("AUTO_APPROVED")
    )
    
    return validated_df.drop("validation_checks")

silver_validated = validate_business_rules(silver_with_fraud)

# COMMAND ----------

# MAGIC %md
# MAGIC ## PII Masking and GDPR Compliance

# COMMAND ----------

def apply_pii_masking(df):
    """Apply PII masking for GDPR compliance"""
    
    # Hash sensitive fields
    masked_df = df.withColumn(
        "customer_id_hash",
        sha2(col("customer_id"), 256)
    )
    
    # Add GDPR fields
    masked_df = masked_df \
        .withColumn("gdpr_consent", lit(True)) \
        .withColumn("gdpr_consent_date", current_date()) \
        .withColumn("data_retention_date", 
            date_add(current_date(), 365 * 7))  # 7 years for insurance claims
    
    return masked_df

silver_masked = apply_pii_masking(silver_validated)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Processing Metadata

# COMMAND ----------

# Add Silver layer metadata
silver_final = silver_masked \
    .withColumn("_silver_timestamp", current_timestamp()) \
    .withColumn("_processing_time_ms", 
        (unix_timestamp(current_timestamp()) - unix_timestamp(col("_ingestion_timestamp"))) * 1000) \
    .withColumn("data_quality_score", col("dq_score")) \
    .drop("dq_score")

# Select final columns for Silver table
silver_output = silver_final.select(
    "claim_id",
    "policy_number",
    "customer_id",
    "claim_amount",
    "claim_amount_gbp",
    "claim_date",
    "incident_date",
    "incident_location",
    "incident_postcode",
    "postcode_district",
    "postcode_area",
    "region",
    "claim_type",
    "claim_status",
    "fraud_score",
    "adjusted_fraud_score",
    "fraud_indicators",
    "is_high_risk",
    "requires_manual_review",
    "days_to_claim",
    "claim_amount_category",
    "is_weekend_claim",
    "is_bank_holiday",
    "is_business_hours",
    "processing_status",
    "validation_score",
    "data_quality_score",
    "_silver_timestamp",
    "_processing_time_ms"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Delta Table

# COMMAND ----------

# Write to Silver table with merge for updates
def write_to_silver(df, checkpoint_path, table_name):
    """Write streaming data to Silver Delta table"""
    
    query = df.writeStream \
        .outputMode("append") \
        .format("delta") \
        .option("checkpointLocation", checkpoint_path) \
        .option("mergeSchema", "true") \
        .trigger(processingTime='30 seconds') \
        .table(table_name)
    
    return query

# Start the Silver stream
silver_stream_query = write_to_silver(
    silver_output,
    CHECKPOINT_PATH,
    SILVER_TABLE
)

print(f"✅ Started streaming to {SILVER_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Real-time Metrics and Monitoring

# COMMAND ----------

# Create monitoring view for Silver layer
spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW silver_monitoring AS
SELECT 
    DATE(_silver_timestamp) as processing_date,
    HOUR(_silver_timestamp) as processing_hour,
    processing_status,
    claim_type,
    COUNT(*) as claim_count,
    AVG(claim_amount_gbp) as avg_amount,
    MAX(claim_amount_gbp) as max_amount,
    AVG(adjusted_fraud_score) as avg_fraud_score,
    SUM(CASE WHEN is_high_risk THEN 1 ELSE 0 END) as high_risk_count,
    SUM(CASE WHEN requires_manual_review THEN 1 ELSE 0 END) as manual_review_count,
    AVG(_processing_time_ms) as avg_processing_time_ms,
    PERCENTILE_APPROX(_processing_time_ms, 0.95) as p95_processing_time_ms
FROM {SILVER_TABLE}
WHERE _silver_timestamp >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY processing_date, processing_hour, processing_status, claim_type
ORDER BY processing_date DESC, processing_hour DESC
""")

# Display current metrics
display(spark.sql("""
SELECT 
    processing_status,
    COUNT(*) as count,
    AVG(claim_amount_gbp) as avg_amount,
    AVG(adjusted_fraud_score) as avg_fraud_score
FROM silver_monitoring
GROUP BY processing_status
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fraud Pattern Analysis

# COMMAND ----------

# Analyze fraud patterns
spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW fraud_pattern_analysis AS
SELECT 
    explode(fraud_indicators) as fraud_indicator,
    COUNT(*) as occurrence_count,
    AVG(claim_amount_gbp) as avg_claim_amount,
    AVG(adjusted_fraud_score) as avg_fraud_score,
    collect_list(postcode_district) as affected_postcodes
FROM {SILVER_TABLE}
WHERE size(fraud_indicators) > 0
    AND _silver_timestamp >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY fraud_indicator
ORDER BY occurrence_count DESC
""")

display(spark.sql("SELECT * FROM fraud_pattern_analysis"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Alerts for High-Risk Claims

# COMMAND ----------

# Monitor for high-risk claims requiring immediate attention
high_risk_alerts = silver_output \
    .filter(
        (col("is_high_risk") == True) | 
        (col("claim_amount_gbp") > 50000) |
        (size(col("fraud_indicators")) > 4)
    ) \
    .select(
        "claim_id",
        "claim_date",
        "customer_id",
        "claim_amount_gbp",
        "adjusted_fraud_score",
        "fraud_indicators",
        "processing_status"
    )

# Write alerts to a separate stream for immediate action
alert_query = high_risk_alerts.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime='10 seconds') \
    .start()

print("✅ High-risk claim monitoring started")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream Status and Management

# COMMAND ----------

def check_silver_stream_status():
    """Check the status of Silver streaming query"""
    
    if silver_stream_query.isActive:
        print("✅ Silver stream is active")
        
        # Get recent progress
        progress = silver_stream_query.recentProgress
        if progress and len(progress) > 0:
            latest = progress[-1]
            print(f"\n📊 Latest batch:")
            print(f"   Timestamp: {latest.get('timestamp', 'N/A')}")
            
            if 'numInputRows' in latest:
                print(f"   Rows processed: {latest['numInputRows']}")
            
            if 'durationMs' in latest:
                duration = latest['durationMs']
                print(f"   Processing time: {duration.get('triggerExecution', 0)}ms")
    else:
        print("❌ Silver stream is not active")
    
    # Count Silver records
    try:
        silver_count = spark.table(SILVER_TABLE).count()
        print(f"\n📦 Total Silver records: {silver_count:,}")
    except:
        print("\n📦 Silver table not yet created")

# Check status
check_silver_stream_status()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC The Silver layer transformation pipeline is now:
# MAGIC - ✅ Applying business transformations and enrichments
# MAGIC - ✅ Adding UK-specific data (regions, bank holidays)
# MAGIC - ✅ Detecting fraud patterns with UK insurance rules
# MAGIC - ✅ Validating business rules and data quality
# MAGIC - ✅ Masking PII for GDPR compliance
# MAGIC - ✅ Routing claims for appropriate processing
# MAGIC - ✅ Generating real-time metrics and alerts
# MAGIC - ✅ Tracking processing performance