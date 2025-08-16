# Databricks notebook source
# MAGIC %md
# MAGIC # PII Detection for GDPR Compliance
# MAGIC Detect and classify PII in insurance claims data using UK-specific patterns

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
import json
import re

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define UK-specific PII Patterns

# COMMAND ----------

# UK PII Detection Patterns
uk_pii_patterns = {
    # High Sensitivity
    'ni_number': {
        'pattern': r'\b[A-Z]{2}\d{6}[A-Z]\b',
        'sensitivity': 'HIGH',
        'category': 'Government_ID',
        'name': 'National Insurance Number'
    },
    'nhs_number': {
        'pattern': r'\b\d{3}\s?\d{3}\s?\d{4}\b',
        'sensitivity': 'HIGH', 
        'category': 'Healthcare_ID',
        'name': 'NHS Number'
    },
    'driving_license': {
        'pattern': r'\b[A-Z]{2,5}\d{6}[A-Z0-9]{3}\b',
        'sensitivity': 'HIGH',
        'category': 'Government_ID',
        'name': 'UK Driving License'
    },
    
    # Medium Sensitivity
    'email': {
        'pattern': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
        'sensitivity': 'MEDIUM',
        'category': 'Contact_Info',
        'name': 'Email Address'
    },
    'uk_phone': {
        'pattern': r'\b(?:0[1-9]\d{8,9}|\+44\s?\d{10}|07\d{9})\b',
        'sensitivity': 'MEDIUM',
        'category': 'Contact_Info',
        'name': 'UK Phone Number'
    },
    
    # Financial
    'bank_account': {
        'pattern': r'\b\d{2}-\d{2}-\d{2}-\d{8}\b',
        'sensitivity': 'HIGH',
        'category': 'Financial',
        'name': 'UK Bank Account'
    },
    'credit_card': {
        'pattern': r'\b(?:\d{4}[\s-]?){3}\d{4}\b',
        'sensitivity': 'HIGH',
        'category': 'Financial',
        'name': 'Credit Card Number'
    },
    
    # Location
    'postcode': {
        'pattern': r'\b[A-Z]{1,2}\d{1,2}[A-Z]?\s?\d[A-Z]{2}\b',
        'sensitivity': 'LOW',
        'category': 'Location',
        'name': 'UK Postcode'
    },
    
    # Personal
    'person_name': {
        'pattern': r'\b[A-Z][a-z]+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?\b',
        'sensitivity': 'MEDIUM',
        'category': 'Personal',
        'name': 'Person Name'
    }
}

print("✅ Defined", len(uk_pii_patterns), "UK-specific PII patterns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create PII Detection UDF

# COMMAND ----------

def detect_pii(text):
    """Detect PII in text and return findings"""
    if not text:
        return json.dumps({'findings': [], 'sensitivity': 'NONE', 'categories': []})
    
    findings = []
    categories = set()
    max_sensitivity = 'NONE'
    sensitivity_order = {'NONE': 0, 'LOW': 1, 'MEDIUM': 2, 'HIGH': 3}
    
    for pii_type, config in uk_pii_patterns.items():
        matches = re.finditer(config['pattern'], str(text), re.IGNORECASE)
        for match in matches:
            findings.append({
                'type': pii_type,
                'category': config['category'],
                'sensitivity': config['sensitivity']
            })
            categories.add(config['category'])
            
            # Update max sensitivity
            if sensitivity_order[config['sensitivity']] > sensitivity_order[max_sensitivity]:
                max_sensitivity = config['sensitivity']
    
    # Limit findings to reduce storage
    findings = findings[:10]
    
    return json.dumps({
        'findings': findings,
        'sensitivity': max_sensitivity,
        'categories': list(categories),
        'pii_count': len(findings)
    })

# Register UDF
detect_pii_udf = F.udf(detect_pii, StringType())

print("✅ PII detection UDF created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Claims Data

# COMMAND ----------

# Load sample claims
claims_df = spark.table("silver.claims_processed")
print(f"📊 Loaded {claims_df.count()} claims for PII scanning")

# Sample for demo (scan 10% to save costs)
sample_df = claims_df.sample(0.1, seed=42)
print(f"💰 Cost optimization: Scanning 10% sample ({sample_df.count()} records)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Detect PII in Claims

# COMMAND ----------

# Scan for PII in multiple fields
pii_scanned_df = (
    sample_df
    .withColumn("scan_timestamp", F.current_timestamp())
    
    # Scan text fields for PII
    .withColumn("customer_name_pii", detect_pii_udf(F.col("customer_name")))
    .withColumn("customer_email_pii", detect_pii_udf(F.col("customer_email")))
    .withColumn("customer_phone_pii", detect_pii_udf(F.col("customer_phone")))
    .withColumn("customer_address_pii", detect_pii_udf(F.col("customer_address")))
    .withColumn("incident_description_pii", detect_pii_udf(F.col("incident_description")))
    .withColumn("claim_notes_pii", detect_pii_udf(F.col("claim_notes")))
    
    # Extract sensitivity levels
    .withColumn("name_sensitivity", F.get_json_object(F.col("customer_name_pii"), "$.sensitivity"))
    .withColumn("email_sensitivity", F.get_json_object(F.col("customer_email_pii"), "$.sensitivity"))
    .withColumn("phone_sensitivity", F.get_json_object(F.col("customer_phone_pii"), "$.sensitivity"))
    .withColumn("address_sensitivity", F.get_json_object(F.col("customer_address_pii"), "$.sensitivity"))
    .withColumn("description_sensitivity", F.get_json_object(F.col("incident_description_pii"), "$.sensitivity"))
    .withColumn("notes_sensitivity", F.get_json_object(F.col("claim_notes_pii"), "$.sensitivity"))
    
    # Calculate overall sensitivity
    .withColumn("overall_sensitivity",
        F.when(
            (F.col("name_sensitivity") == "HIGH") | 
            (F.col("email_sensitivity") == "HIGH") |
            (F.col("phone_sensitivity") == "HIGH") |
            (F.col("address_sensitivity") == "HIGH") |
            (F.col("description_sensitivity") == "HIGH") |
            (F.col("notes_sensitivity") == "HIGH"), "HIGH"
        ).when(
            (F.col("name_sensitivity") == "MEDIUM") | 
            (F.col("email_sensitivity") == "MEDIUM") |
            (F.col("phone_sensitivity") == "MEDIUM") |
            (F.col("address_sensitivity") == "MEDIUM") |
            (F.col("description_sensitivity") == "MEDIUM") |
            (F.col("notes_sensitivity") == "MEDIUM"), "MEDIUM"
        ).when(
            (F.col("name_sensitivity") == "LOW") | 
            (F.col("email_sensitivity") == "LOW") |
            (F.col("phone_sensitivity") == "LOW") |
            (F.col("address_sensitivity") == "LOW") |
            (F.col("description_sensitivity") == "LOW") |
            (F.col("notes_sensitivity") == "LOW"), "LOW"
        ).otherwise("NONE")
    )
    
    # Flag records containing PII
    .withColumn("contains_pii", F.col("overall_sensitivity") != "NONE")
    
    # Calculate GDPR risk score
    .withColumn("gdpr_risk_score",
        F.when(F.col("overall_sensitivity") == "HIGH", 1.0)
         .when(F.col("overall_sensitivity") == "MEDIUM", 0.5)
         .when(F.col("overall_sensitivity") == "LOW", 0.2)
         .otherwise(0.0)
    )
)

# Display sample results
display(pii_scanned_df.select(
    "claim_id",
    "customer_name",
    "overall_sensitivity",
    "contains_pii",
    "gdpr_risk_score"
).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save PII Classification Results

# COMMAND ----------

# Select relevant columns for storage
pii_classified_df = pii_scanned_df.select(
    "claim_id",
    "customer_id",
    "scan_timestamp",
    "claim_notes_pii",
    "incident_description_pii",
    "customer_address_pii",
    "overall_sensitivity",
    "contains_pii",
    "gdpr_risk_score",
    "processing_date"
)

# Write to GDPR database
pii_classified_df.write \
    .mode("overwrite") \
    .format("delta") \
    .partitionBy("processing_date") \
    .saveAsTable("gdpr.pii_classified_claims")

print("✅ PII classification results saved to gdpr.pii_classified_claims")

# COMMAND ----------

# MAGIC %md
# MAGIC ## PII Detection Summary

# COMMAND ----------

# Calculate statistics
stats = pii_scanned_df.agg(
    F.count("*").alias("total_records"),
    F.sum(F.when(F.col("contains_pii"), 1).otherwise(0)).alias("records_with_pii"),
    F.sum(F.when(F.col("overall_sensitivity") == "HIGH", 1).otherwise(0)).alias("high_sensitivity"),
    F.sum(F.when(F.col("overall_sensitivity") == "MEDIUM", 1).otherwise(0)).alias("medium_sensitivity"),
    F.sum(F.when(F.col("overall_sensitivity") == "LOW", 1).otherwise(0)).alias("low_sensitivity"),
    F.avg("gdpr_risk_score").alias("avg_risk_score")
).collect()[0]

print("📊 PII Detection Summary")
print("=" * 50)
print(f"Total Records Scanned: {stats['total_records']}")
print(f"Records with PII: {stats['records_with_pii']} ({stats['records_with_pii']/stats['total_records']*100:.1f}%)")
print(f"High Sensitivity: {stats['high_sensitivity']}")
print(f"Medium Sensitivity: {stats['medium_sensitivity']}")
print(f"Low Sensitivity: {stats['low_sensitivity']}")
print(f"Average GDPR Risk Score: {stats['avg_risk_score']:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Detailed PII Categories Found

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze PII categories found
# MAGIC SELECT 
# MAGIC     overall_sensitivity,
# MAGIC     COUNT(*) as count,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
# MAGIC FROM gdpr.pii_classified_claims
# MAGIC GROUP BY overall_sensitivity
# MAGIC ORDER BY 
# MAGIC     CASE overall_sensitivity
# MAGIC         WHEN 'HIGH' THEN 1
# MAGIC         WHEN 'MEDIUM' THEN 2
# MAGIC         WHEN 'LOW' THEN 3
# MAGIC         WHEN 'NONE' THEN 4
# MAGIC     END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC ✅ **PII Detection Complete**
# MAGIC 
# MAGIC The PII classification is now ready for:
# MAGIC 1. **Dynamic Data Masking** - Apply masking based on sensitivity levels
# MAGIC 2. **Audit Logging** - Track access to sensitive data
# MAGIC 3. **Retention Policies** - Apply GDPR-compliant retention rules
# MAGIC 4. **Subject Access Requests** - Export customer data on request
# MAGIC 
# MAGIC 💰 **Cost Optimization Notes:**
# MAGIC - Only scanned 10% sample for demo
# MAGIC - Used regex patterns instead of external APIs
# MAGIC - Limited findings storage to reduce Delta table size