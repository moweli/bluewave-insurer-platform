# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Business Aggregations and KPIs
# MAGIC Generate real-time business metrics, KPIs, and aggregated views for reporting

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration and Setup

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from delta.tables import DeltaTable
import json

spark = SparkSession.builder \
    .appName("GoldAggregations") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Database configuration
SILVER_DB = "insurance_silver"
GOLD_DB = "insurance_gold"
SILVER_TABLE = f"{SILVER_DB}.claims"

# Gold table paths
DAILY_SUMMARY_TABLE = f"{GOLD_DB}.daily_claims_summary"
FRAUD_METRICS_TABLE = f"{GOLD_DB}.fraud_metrics"
OPERATIONAL_KPIS_TABLE = f"{GOLD_DB}.operational_kpis"
CUSTOMER_RISK_TABLE = f"{GOLD_DB}.customer_risk_profiles"

# Checkpoint paths
CHECKPOINT_BASE = "/mnt/checkpoints/gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Silver Stream

# COMMAND ----------

# Read from Silver Delta table as stream with watermark
silver_stream = spark.readStream \
    .format("delta") \
    .table(SILVER_TABLE) \
    .withWatermark("_silver_timestamp", "10 minutes")

print(f"✅ Reading from {SILVER_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily Claims Summary Aggregation

# COMMAND ----------

def create_daily_summary(df):
    """Create daily claims summary with key metrics"""
    
    daily_summary = df \
        .withColumn("summary_date", to_date(col("claim_date"))) \
        .groupBy("summary_date", "claim_type") \
        .agg(
            count("*").alias("total_claims"),
            sum("claim_amount_gbp").alias("total_amount"),
            avg("claim_amount_gbp").alias("average_amount"),
            expr("percentile_approx(claim_amount_gbp, 0.5)").alias("median_amount"),
            min("claim_amount_gbp").alias("min_amount"),
            max("claim_amount_gbp").alias("max_amount"),
            stddev("claim_amount_gbp").alias("stddev_amount"),
            
            # Fraud metrics
            sum(when(col("is_high_risk"), 1).otherwise(0)).alias("fraud_detected_count"),
            sum(when(col("is_high_risk"), col("claim_amount_gbp")).otherwise(0)).alias("fraud_detected_amount"),
            avg("adjusted_fraud_score").alias("avg_fraud_score"),
            
            # Processing metrics
            sum(when(col("requires_manual_review"), 1).otherwise(0)).alias("manual_review_count"),
            sum(when(col("processing_status") == "AUTO_APPROVED", 1).otherwise(0)).alias("auto_approved_count"),
            sum(when(col("processing_status") == "REJECTED", 1).otherwise(0)).alias("rejected_count"),
            
            # Performance metrics
            avg("_processing_time_ms").alias("processing_time_avg_ms"),
            expr("percentile_approx(_processing_time_ms, 0.95)").alias("processing_time_p95_ms"),
            expr("percentile_approx(_processing_time_ms, 0.99)").alias("processing_time_p99_ms"),
            
            # Quality metrics
            avg("data_quality_score").alias("avg_data_quality"),
            avg("validation_score").alias("avg_validation_score")
        ) \
        .withColumn("_gold_timestamp", current_timestamp()) \
        .withColumn("fraud_rate", col("fraud_detected_count") / col("total_claims")) \
        .withColumn("auto_approval_rate", col("auto_approved_count") / col("total_claims"))
    
    return daily_summary

# Create daily summary stream
daily_summary_stream = create_daily_summary(silver_stream)

# Write daily summary to Gold
daily_summary_query = daily_summary_stream.writeStream \
    .outputMode("complete") \
    .format("delta") \
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/daily_summary") \
    .trigger(processingTime='1 minute') \
    .table(DAILY_SUMMARY_TABLE)

print(f"✅ Started daily summary aggregation to {DAILY_SUMMARY_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hourly Fraud Metrics

# COMMAND ----------

def create_fraud_metrics(df):
    """Create detailed fraud detection metrics"""
    
    fraud_metrics = df \
        .withColumn("metric_date", to_date(col("claim_date"))) \
        .withColumn("metric_hour", hour(col("claim_date"))) \
        .groupBy("metric_date", "metric_hour") \
        .agg(
            # Volume metrics
            count("*").alias("total_claims_evaluated"),
            sum(when(col("adjusted_fraud_score") > 0.5, 1).otherwise(0)).alias("fraud_alerts_generated"),
            sum(when(col("is_high_risk"), 1).otherwise(0)).alias("high_risk_flagged"),
            sum(when(col("processing_status") == "FRAUD_INVESTIGATION", 1).otherwise(0)).alias("sent_to_investigation"),
            
            # Score distributions
            avg("fraud_score").alias("avg_original_fraud_score"),
            avg("adjusted_fraud_score").alias("avg_adjusted_fraud_score"),
            max("adjusted_fraud_score").alias("max_fraud_score"),
            expr("percentile_approx(adjusted_fraud_score, 0.9)").alias("p90_fraud_score"),
            
            # Financial impact
            sum(when(col("is_high_risk"), col("claim_amount_gbp")).otherwise(0)).alias("total_fraud_prevented_gbp"),
            avg(when(col("is_high_risk"), col("claim_amount_gbp")).otherwise(None)).alias("avg_fraud_amount_gbp"),
            
            # Pattern analysis
            collect_set(when(size(col("fraud_indicators")) > 0, col("fraud_indicators")[0]).otherwise(None)).alias("top_fraud_patterns"),
            collect_set(when(col("is_high_risk"), col("postcode_district")).otherwise(None)).alias("high_risk_postcodes"),
            
            # Performance
            avg(when(col("is_high_risk"), col("_processing_time_ms")).otherwise(None)).alias("avg_detection_time_ms")
        ) \
        .withColumn("_gold_timestamp", current_timestamp()) \
        .withColumn("fraud_detection_rate", 
            col("high_risk_flagged") / col("total_claims_evaluated")) \
        .withColumn("alert_to_investigation_rate",
            when(col("fraud_alerts_generated") > 0, 
                 col("sent_to_investigation") / col("fraud_alerts_generated"))
            .otherwise(0))
    
    return fraud_metrics

# Create fraud metrics stream
fraud_metrics_stream = create_fraud_metrics(silver_stream)

# Write fraud metrics to Gold
fraud_metrics_query = fraud_metrics_stream.writeStream \
    .outputMode("complete") \
    .format("delta") \
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/fraud_metrics") \
    .trigger(processingTime='2 minutes') \
    .table(FRAUD_METRICS_TABLE)

print(f"✅ Started fraud metrics aggregation to {FRAUD_METRICS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Operational KPIs

# COMMAND ----------

def calculate_operational_kpis(df):
    """Calculate key operational KPIs"""
    
    # Define KPI targets
    kpi_targets = {
        "avg_processing_time_ms": 200,
        "auto_approval_rate": 0.75,
        "fraud_detection_rate": 0.035,
        "data_quality_score": 0.9,
        "claims_per_hour": 100
    }
    
    kpi_stream = df \
        .withColumn("kpi_date", to_date(col("claim_date"))) \
        .groupBy("kpi_date") \
        .agg(
            # Processing efficiency
            avg("_processing_time_ms").alias("avg_processing_time_ms"),
            expr("percentile_approx(_processing_time_ms, 0.95)").alias("p95_processing_time_ms"),
            
            # Automation metrics
            (sum(when(col("processing_status") == "AUTO_APPROVED", 1).otherwise(0)) / count("*")).alias("auto_approval_rate"),
            (sum(when(col("requires_manual_review"), 1).otherwise(0)) / count("*")).alias("manual_review_rate"),
            
            # Fraud effectiveness
            (sum(when(col("is_high_risk"), 1).otherwise(0)) / count("*")).alias("fraud_detection_rate"),
            avg("adjusted_fraud_score").alias("avg_fraud_confidence"),
            
            # Quality metrics
            avg("data_quality_score").alias("data_quality_score"),
            avg("validation_score").alias("validation_score"),
            
            # Volume metrics
            count("*").alias("total_claims_processed"),
            (count("*") / 24.0).alias("claims_per_hour"),
            
            # Financial metrics
            sum("claim_amount_gbp").alias("total_claim_value"),
            avg("claim_amount_gbp").alias("avg_claim_value")
        )
    
    # Reshape to KPI format
    kpi_unpivoted = kpi_stream.select(
        col("kpi_date"),
        explode(
            array(
                struct(lit("avg_processing_time_ms").alias("kpi_name"), 
                       col("avg_processing_time_ms").alias("kpi_value")),
                struct(lit("auto_approval_rate").alias("kpi_name"), 
                       col("auto_approval_rate").alias("kpi_value")),
                struct(lit("fraud_detection_rate").alias("kpi_name"), 
                       col("fraud_detection_rate").alias("kpi_value")),
                struct(lit("data_quality_score").alias("kpi_name"), 
                       col("data_quality_score").alias("kpi_value")),
                struct(lit("claims_per_hour").alias("kpi_name"), 
                       col("claims_per_hour").alias("kpi_value"))
            )
        ).alias("kpi")
    ).select(
        col("kpi_date"),
        col("kpi.kpi_name"),
        col("kpi.kpi_value")
    )
    
    # Add targets and calculate variance
    kpi_final = kpi_unpivoted \
        .withColumn("kpi_target",
            when(col("kpi_name") == "avg_processing_time_ms", kpi_targets["avg_processing_time_ms"])
            .when(col("kpi_name") == "auto_approval_rate", kpi_targets["auto_approval_rate"])
            .when(col("kpi_name") == "fraud_detection_rate", kpi_targets["fraud_detection_rate"])
            .when(col("kpi_name") == "data_quality_score", kpi_targets["data_quality_score"])
            .when(col("kpi_name") == "claims_per_hour", kpi_targets["claims_per_hour"])
            .otherwise(None)) \
        .withColumn("variance_from_target",
            when(col("kpi_target").isNotNull(), 
                 ((col("kpi_value") - col("kpi_target")) / col("kpi_target")) * 100)
            .otherwise(None)) \
        .withColumn("kpi_status",
            when(col("variance_from_target").isNull(), "NO_TARGET")
            .when(col("kpi_name") == "avg_processing_time_ms",
                when(col("kpi_value") <= col("kpi_target"), "GREEN")
                .when(col("kpi_value") <= col("kpi_target") * 1.2, "AMBER")
                .otherwise("RED"))
            .when(col("kpi_value") >= col("kpi_target"), "GREEN")
            .when(col("kpi_value") >= col("kpi_target") * 0.9, "AMBER")
            .otherwise("RED")) \
        .withColumn("_gold_timestamp", current_timestamp())
    
    return kpi_final

# Create KPI stream
kpi_stream = calculate_operational_kpis(silver_stream)

# Write KPIs to Gold
kpi_query = kpi_stream.writeStream \
    .outputMode("complete") \
    .format("delta") \
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/operational_kpis") \
    .trigger(processingTime='5 minutes') \
    .table(OPERATIONAL_KPIS_TABLE)

print(f"✅ Started KPI calculation to {OPERATIONAL_KPIS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer Risk Profiles (Windowed Aggregation)

# COMMAND ----------

def create_customer_risk_profiles(df):
    """Create customer risk profiles using windowed aggregations"""
    
    # Window for customer history (last 90 days)
    customer_window = Window.partitionBy("customer_id").orderBy("claim_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    customer_profiles = df \
        .withColumn("claims_count", count("*").over(customer_window)) \
        .withColumn("total_claimed", sum("claim_amount_gbp").over(customer_window)) \
        .withColumn("avg_claim_amount", avg("claim_amount_gbp").over(customer_window)) \
        .withColumn("max_fraud_score", max("adjusted_fraud_score").over(customer_window)) \
        .withColumn("fraud_flags_count", sum(when(col("is_high_risk"), 1).otherwise(0)).over(customer_window)) \
        .groupBy("customer_id") \
        .agg(
            max("claims_count").alias("total_claims"),
            max("total_claimed").alias("total_claimed_amount"),
            max("avg_claim_amount").alias("avg_claim_amount"),
            max("max_fraud_score").alias("fraud_risk_score"),
            max("fraud_flags_count").alias("fraud_incidents"),
            
            # Calculate frequency score (claims per month)
            (max("claims_count") / 3.0).alias("claims_frequency_score"),
            
            # Calculate severity score (average claim vs overall average)
            max("avg_claim_amount").alias("claims_severity_score"),
            
            last("claim_date").alias("last_claim_date"),
            current_timestamp().alias("last_updated")
        ) \
        .withColumn("risk_segment",
            when((col("fraud_risk_score") > 0.7) | (col("fraud_incidents") > 2), "HIGH_RISK")
            .when((col("fraud_risk_score") > 0.4) | (col("claims_frequency_score") > 5), "MEDIUM_RISK")
            .otherwise("LOW_RISK")) \
        .withColumn("overall_risk_rating",
            when(col("risk_segment") == "HIGH_RISK", "DECLINE")
            .when(col("risk_segment") == "MEDIUM_RISK", "REVIEW")
            .otherwise("ACCEPT")) \
        .withColumn("recommended_premium_adjustment",
            when(col("risk_segment") == "HIGH_RISK", 1.5)
            .when(col("risk_segment") == "MEDIUM_RISK", 1.2)
            .otherwise(1.0)) \
        .withColumn("_gold_timestamp", current_timestamp())
    
    return customer_profiles

# Create customer risk profile stream
customer_risk_stream = create_customer_risk_profiles(silver_stream)

# Write customer profiles to Gold
customer_risk_query = customer_risk_stream.writeStream \
    .outputMode("complete") \
    .format("delta") \
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/customer_risk") \
    .trigger(processingTime='10 minutes') \
    .table(CUSTOMER_RISK_TABLE)

print(f"✅ Started customer risk profiling to {CUSTOMER_RISK_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Real-time Dashboard Queries

# COMMAND ----------

# Create materialized views for dashboards
spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW realtime_dashboard AS
SELECT 
    current_timestamp() as refresh_time,
    
    -- Current day metrics
    (SELECT COUNT(*) FROM {SILVER_TABLE} 
     WHERE DATE(claim_date) = current_date()) as claims_today,
    
    (SELECT SUM(claim_amount_gbp) FROM {SILVER_TABLE} 
     WHERE DATE(claim_date) = current_date()) as total_value_today,
    
    (SELECT AVG(adjusted_fraud_score) FROM {SILVER_TABLE} 
     WHERE DATE(claim_date) = current_date()) as avg_fraud_score_today,
    
    -- Processing metrics
    (SELECT AVG(_processing_time_ms) FROM {SILVER_TABLE} 
     WHERE _silver_timestamp >= current_timestamp() - INTERVAL 1 HOUR) as avg_processing_time_1h,
    
    -- Fraud metrics
    (SELECT COUNT(*) FROM {SILVER_TABLE} 
     WHERE is_high_risk = true 
     AND DATE(claim_date) = current_date()) as fraud_detected_today,
    
    -- Queue sizes
    (SELECT COUNT(*) FROM {SILVER_TABLE} 
     WHERE requires_manual_review = true 
     AND processing_status = 'MANUAL_REVIEW') as manual_review_queue,
    
    (SELECT COUNT(*) FROM {SILVER_TABLE} 
     WHERE processing_status = 'FRAUD_INVESTIGATION') as fraud_investigation_queue
""")

# Display dashboard
display(spark.sql("SELECT * FROM realtime_dashboard"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top Claims Analysis

# COMMAND ----------

# Create view for top claims requiring attention
spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW top_claims_attention AS
WITH RankedClaims AS (
    SELECT 
        claim_id,
        customer_id,
        claim_date,
        claim_type,
        claim_amount_gbp,
        adjusted_fraud_score,
        fraud_indicators,
        processing_status,
        ROW_NUMBER() OVER (ORDER BY claim_amount_gbp DESC) as amount_rank,
        ROW_NUMBER() OVER (ORDER BY adjusted_fraud_score DESC) as fraud_rank
    FROM {SILVER_TABLE}
    WHERE claim_date >= current_date() - INTERVAL 7 DAYS
)
SELECT 
    'HIGH_VALUE' as category,
    claim_id,
    customer_id,
    claim_date,
    claim_type,
    claim_amount_gbp,
    adjusted_fraud_score,
    processing_status
FROM RankedClaims
WHERE amount_rank <= 10

UNION ALL

SELECT 
    'HIGH_FRAUD_RISK' as category,
    claim_id,
    customer_id,
    claim_date,
    claim_type,
    claim_amount_gbp,
    adjusted_fraud_score,
    processing_status
FROM RankedClaims
WHERE fraud_rank <= 10

ORDER BY category, claim_amount_gbp DESC
""")

display(spark.sql("SELECT * FROM top_claims_attention"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regional Analysis

# COMMAND ----------

# Regional performance metrics
spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW regional_metrics AS
SELECT 
    region,
    COUNT(*) as claim_count,
    SUM(claim_amount_gbp) as total_claimed,
    AVG(claim_amount_gbp) as avg_claim_amount,
    AVG(adjusted_fraud_score) as avg_fraud_score,
    SUM(CASE WHEN is_high_risk THEN 1 ELSE 0 END) as fraud_cases,
    SUM(CASE WHEN is_high_risk THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as fraud_rate_percent,
    AVG(_processing_time_ms) as avg_processing_time,
    COUNT(DISTINCT customer_id) as unique_customers
FROM {SILVER_TABLE}
WHERE claim_date >= current_date() - INTERVAL 30 DAYS
    AND region IS NOT NULL
GROUP BY region
ORDER BY total_claimed DESC
""")

display(spark.sql("SELECT * FROM regional_metrics"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream Health Monitoring

# COMMAND ----------

def monitor_gold_streams():
    """Monitor all Gold layer streaming queries"""
    
    streams = {
        "Daily Summary": daily_summary_query,
        "Fraud Metrics": fraud_metrics_query,
        "Operational KPIs": kpi_query,
        "Customer Risk": customer_risk_query
    }
    
    print("📊 Gold Layer Stream Status:")
    print("=" * 50)
    
    for name, query in streams.items():
        if query.isActive:
            print(f"✅ {name}: Active")
            progress = query.recentProgress
            if progress and len(progress) > 0:
                latest = progress[-1]
                if 'numInputRows' in latest:
                    print(f"   Last batch: {latest['numInputRows']} rows")
        else:
            print(f"❌ {name}: Inactive")
    
    print("\n📈 Gold Table Record Counts:")
    for table in [DAILY_SUMMARY_TABLE, FRAUD_METRICS_TABLE, OPERATIONAL_KPIS_TABLE, CUSTOMER_RISK_TABLE]:
        try:
            count = spark.table(table).count()
            print(f"   {table}: {count:,} records")
        except:
            print(f"   {table}: Not yet created")

# Monitor streams
monitor_gold_streams()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Executive Dashboard View

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {GOLD_DB}.v_executive_dashboard AS
WITH CurrentMetrics AS (
    SELECT 
        MAX(kpi_value) FILTER (WHERE kpi_name = 'claims_per_hour') as claims_per_hour,
        MAX(kpi_value) FILTER (WHERE kpi_name = 'auto_approval_rate') as auto_approval_rate,
        MAX(kpi_value) FILTER (WHERE kpi_name = 'fraud_detection_rate') as fraud_detection_rate,
        MAX(kpi_value) FILTER (WHERE kpi_name = 'avg_processing_time_ms') as avg_processing_time_ms
    FROM {OPERATIONAL_KPIS_TABLE}
    WHERE kpi_date = current_date()
),
DailySummary AS (
    SELECT 
        SUM(total_claims) as total_claims_today,
        SUM(total_amount) as total_amount_today,
        SUM(fraud_detected_amount) as fraud_prevented_today,
        AVG(fraud_rate) as avg_fraud_rate
    FROM {DAILY_SUMMARY_TABLE}
    WHERE summary_date = current_date()
)
SELECT 
    current_timestamp() as dashboard_timestamp,
    cm.claims_per_hour,
    cm.auto_approval_rate * 100 as auto_approval_percent,
    cm.fraud_detection_rate * 100 as fraud_detection_percent,
    cm.avg_processing_time_ms,
    ds.total_claims_today,
    ds.total_amount_today,
    ds.fraud_prevented_today,
    ds.avg_fraud_rate * 100 as fraud_rate_percent
FROM CurrentMetrics cm
CROSS JOIN DailySummary ds
""")

print("✅ Created executive dashboard view")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC The Gold layer aggregation pipeline is now:
# MAGIC - ✅ Generating daily claims summaries
# MAGIC - ✅ Calculating fraud detection metrics
# MAGIC - ✅ Tracking operational KPIs against targets
# MAGIC - ✅ Building customer risk profiles
# MAGIC - ✅ Providing real-time dashboard views
# MAGIC - ✅ Analyzing regional performance
# MAGIC - ✅ Identifying top claims for attention
# MAGIC - ✅ Creating executive-level reporting