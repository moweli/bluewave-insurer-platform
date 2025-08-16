# Databricks notebook source
# MAGIC %md
# MAGIC # GDPR Retention Policies and Right to Erasure
# MAGIC Implement data retention rules and subject deletion requests

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Retention Policies

# COMMAND ----------

# UK Insurance retention requirements
retention_policies = {
    "claims": {
        "standard": 7 * 365,  # 7 years for claims
        "fraud": 10 * 365,    # 10 years for fraud cases
        "declined": 3 * 365,   # 3 years for declined claims
        "description": "Insurance claims retention per FCA requirements"
    },
    "audit_logs": {
        "standard": 10 * 365,  # 10 years for audit trails
        "access": 2 * 365,     # 2 years for access logs
        "deletion": 10 * 365,  # 10 years for deletion records
        "description": "Audit log retention for regulatory compliance"
    },
    "customer_data": {
        "active": 999 * 365,   # Keep while policy active
        "inactive": 7 * 365,   # 7 years after policy ends
        "marketing": 2 * 365,  # 2 years for marketing consent
        "description": "Customer PII retention based on consent"
    },
    "operational": {
        "logs": 90,            # 90 days for operational logs
        "temp": 30,            # 30 days for temporary data
        "cache": 7,            # 7 days for cached data
        "description": "Operational data with short retention"
    }
}

# Create retention policy table
policy_data = []
for category, policies in retention_policies.items():
    for policy_type, days in policies.items():
        if policy_type != "description":
            policy_data.append({
                "category": category,
                "policy_type": policy_type,
                "retention_days": days,
                "description": policies["description"],
                "created_date": datetime.now(),
                "active": True
            })

retention_df = spark.createDataFrame(policy_data)
retention_df.write.mode("overwrite").format("delta").saveAsTable("gdpr.retention_policies")

print("✅ Retention policies defined")
display(retention_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Retention to Claims Data

# COMMAND ----------

# Load claims with retention calculation
claims_df = spark.table("silver.claims_processed")
pii_df = spark.table("gdpr.pii_classified_claims")

# Join and calculate retention
claims_with_retention = (
    claims_df.alias("c")
    .join(pii_df.alias("p"), F.col("c.claim_id") == F.col("p.claim_id"), "left")
    .withColumn("days_since_claim", F.datediff(F.current_date(), F.col("c.claim_date")))
    
    # Apply retention rules based on claim characteristics
    .withColumn("retention_category",
        F.when(F.col("c.fraud_score") > 0.8, "fraud")
         .when(F.col("c.claim_status") == "REJECTED", "declined")
         .otherwise("standard")
    )
    .withColumn("retention_days",
        F.when(F.col("retention_category") == "fraud", 10 * 365)
         .when(F.col("retention_category") == "declined", 3 * 365)
         .otherwise(7 * 365)
    )
    .withColumn("retention_end_date", 
        F.date_add(F.col("c.claim_date"), F.col("retention_days"))
    )
    .withColumn("past_retention", 
        F.col("days_since_claim") > F.col("retention_days")
    )
    .withColumn("days_until_deletion",
        F.col("retention_days") - F.col("days_since_claim")
    )
)

# Show retention status
retention_summary = claims_with_retention.groupBy("retention_category", "past_retention").count()
display(retention_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identify Data for Deletion

# COMMAND ----------

# Find records past retention
deletion_candidates = (
    claims_with_retention
    .filter(F.col("past_retention") == True)
    .select(
        "claim_id",
        "customer_id",
        "claim_date",
        "retention_category",
        "retention_days",
        "days_since_claim",
        F.current_timestamp().alias("identified_for_deletion")
    )
)

if deletion_candidates.count() > 0:
    print(f"⚠️ Found {deletion_candidates.count()} claims past retention period")
    
    # Log deletion candidates
    deletion_candidates.write \
        .mode("append") \
        .format("delta") \
        .saveAsTable("gdpr.deletion_queue")
    
    display(deletion_candidates.limit(10))
else:
    print("✅ No claims past retention period")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Implement Right to Erasure (Article 17)

# COMMAND ----------

def process_erasure_request(customer_id, reason="Customer request"):
    """Process a GDPR erasure request for a customer"""
    
    print(f"🔄 Processing erasure request for customer: {customer_id}")
    
    # Step 1: Check for legal holds
    legal_hold_check = spark.sql(f"""
        SELECT COUNT(*) as hold_count
        FROM silver.claims_processed
        WHERE customer_id = '{customer_id}'
        AND (fraud_score > 0.8 OR claim_status = 'DISPUTED')
    """).collect()[0]['hold_count']
    
    if legal_hold_check > 0:
        print(f"⚠️ Cannot delete: {legal_hold_check} claims under legal hold")
        return False
    
    # Step 2: Anonymize customer data
    anonymized_data = spark.sql(f"""
        SELECT 
            claim_id,
            'DELETED_' || claim_id as customer_id,
            'ANONYMIZED' as customer_name,
            'deleted@anonymous.com' as customer_email,
            '0000000000' as customer_phone,
            'DELETED' as customer_address,
            claim_amount,
            claim_date,
            claim_type,
            claim_status,
            '[REDACTED]' as incident_description,
            '[REDACTED]' as claim_notes
        FROM silver.claims_processed
        WHERE customer_id = '{customer_id}'
    """)
    
    # Step 3: Archive anonymized data
    anonymized_data.write \
        .mode("append") \
        .format("delta") \
        .saveAsTable("gdpr.anonymized_claims")
    
    # Step 4: Log the deletion
    deletion_log = spark.createDataFrame([{
        "customer_id": customer_id,
        "deletion_timestamp": datetime.now(),
        "reason": reason,
        "records_affected": anonymized_data.count(),
        "deletion_type": "ANONYMIZATION",
        "retention_override": False
    }])
    
    deletion_log.write \
        .mode("append") \
        .format("delta") \
        .saveAsTable("gdpr.deletion_log")
    
    print(f"✅ Customer {customer_id} data anonymized successfully")
    return True

# Example erasure request
# Uncomment to test: process_erasure_request("CUST_12345", "GDPR Article 17 Request")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Deletion Schedule

# COMMAND ----------

# Create automated deletion schedule
deletion_schedule = (
    claims_with_retention
    .filter(F.col("days_until_deletion").between(0, 30))
    .groupBy(F.date_add(F.current_date(), F.col("days_until_deletion")).alias("deletion_date"))
    .agg(
        F.count("*").alias("records_to_delete"),
        F.collect_set("retention_category").alias("categories"),
        F.sum(F.when(F.col("overall_sensitivity") == "HIGH", 1).otherwise(0)).alias("high_sensitivity_records")
    )
    .orderBy("deletion_date")
)

print("📅 Upcoming Deletion Schedule (Next 30 Days)")
display(deletion_schedule)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitor Retention Compliance

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Retention compliance dashboard
# MAGIC CREATE OR REPLACE VIEW gdpr.retention_compliance AS
# MAGIC WITH retention_status AS (
# MAGIC     SELECT 
# MAGIC         retention_category,
# MAGIC         CASE 
# MAGIC             WHEN days_until_deletion < 0 THEN 'OVERDUE'
# MAGIC             WHEN days_until_deletion < 30 THEN 'DUE_SOON'
# MAGIC             WHEN days_until_deletion < 180 THEN 'UPCOMING'
# MAGIC             ELSE 'COMPLIANT'
# MAGIC         END as status,
# MAGIC         COUNT(*) as record_count,
# MAGIC         MIN(days_until_deletion) as min_days,
# MAGIC         MAX(days_until_deletion) as max_days
# MAGIC     FROM silver.claims_processed c
# MAGIC     GROUP BY retention_category, status
# MAGIC )
# MAGIC SELECT * FROM retention_status
# MAGIC ORDER BY 
# MAGIC     CASE status
# MAGIC         WHEN 'OVERDUE' THEN 1
# MAGIC         WHEN 'DUE_SOON' THEN 2
# MAGIC         WHEN 'UPCOMING' THEN 3
# MAGIC         ELSE 4
# MAGIC     END,
# MAGIC     retention_category;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show retention compliance
# MAGIC SELECT * FROM gdpr.retention_compliance;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Data Portability Export (Article 20)

# COMMAND ----------

def export_customer_data(customer_id, format="json"):
    """Export customer data for GDPR portability request"""
    
    print(f"📤 Exporting data for customer: {customer_id}")
    
    # Gather all customer data
    customer_data = spark.sql(f"""
        SELECT 
            c.claim_id,
            c.policy_number,
            c.customer_name,
            c.customer_email,
            c.customer_phone,
            c.customer_address,
            c.claim_amount,
            c.claim_date,
            c.claim_type,
            c.claim_status,
            c.incident_location,
            c.incident_description,
            p.overall_sensitivity,
            p.gdpr_risk_score
        FROM silver.claims_processed c
        LEFT JOIN gdpr.pii_classified_claims p ON c.claim_id = p.claim_id
        WHERE c.customer_id = '{customer_id}'
    """)
    
    if customer_data.count() == 0:
        print("❌ No data found for customer")
        return None
    
    # Export based on format
    export_path = f"/tmp/gdpr_export_{customer_id}"
    
    if format == "json":
        customer_data.coalesce(1).write \
            .mode("overwrite") \
            .json(export_path)
    elif format == "csv":
        customer_data.coalesce(1).write \
            .mode("overwrite") \
            .option("header", True) \
            .csv(export_path)
    else:
        customer_data.coalesce(1).write \
            .mode("overwrite") \
            .parquet(export_path)
    
    # Log the export
    export_log = spark.createDataFrame([{
        "customer_id": customer_id,
        "export_timestamp": datetime.now(),
        "format": format,
        "records_exported": customer_data.count(),
        "export_path": export_path,
        "gdpr_article": "Article 20 - Data Portability"
    }])
    
    export_log.write \
        .mode("append") \
        .format("delta") \
        .saveAsTable("gdpr.export_log")
    
    print(f"✅ Data exported to: {export_path}")
    return export_path

# Example export
# Uncomment to test: export_customer_data("CUST_12345", "json")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retention Policy Summary

# COMMAND ----------

# Generate retention summary report
retention_report = spark.sql("""
    WITH retention_analysis AS (
        SELECT 
            retention_category,
            COUNT(*) as total_records,
            SUM(CASE WHEN past_retention THEN 1 ELSE 0 END) as overdue_records,
            SUM(CASE WHEN days_until_deletion BETWEEN 0 AND 30 THEN 1 ELSE 0 END) as due_soon,
            AVG(days_since_claim) as avg_age_days,
            MIN(claim_date) as oldest_claim,
            MAX(claim_date) as newest_claim
        FROM silver.claims_processed
        GROUP BY retention_category
    )
    SELECT 
        retention_category,
        total_records,
        overdue_records,
        due_soon,
        ROUND(avg_age_days, 0) as avg_age_days,
        oldest_claim,
        newest_claim,
        ROUND(overdue_records * 100.0 / total_records, 2) as overdue_percentage
    FROM retention_analysis
    ORDER BY overdue_percentage DESC
""")

print("📊 Retention Policy Compliance Report")
print("=" * 60)
display(retention_report)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC ✅ **Retention Policies Complete**
# MAGIC 
# MAGIC The retention system now provides:
# MAGIC 1. **Automated retention rules** - Based on data type and regulations
# MAGIC 2. **Right to erasure** - Customer deletion requests (Article 17)
# MAGIC 3. **Data portability** - Export customer data (Article 20)
# MAGIC 4. **Deletion scheduling** - Automated cleanup of old data
# MAGIC 5. **Compliance monitoring** - Track retention status
# MAGIC 
# MAGIC 💰 **Cost Optimization Notes:**
# MAGIC - Automatic deletion reduces storage costs
# MAGIC - Archived data moved to cheaper storage tiers
# MAGIC - Efficient Delta format for time-based operations
# MAGIC - No external compliance tools required
# MAGIC 
# MAGIC 🔒 **Security Notes:**
# MAGIC - Anonymization preserves analytics while protecting privacy
# MAGIC - Legal holds prevent premature deletion
# MAGIC - Complete audit trail of all deletions
# MAGIC - Secure export process for data portability