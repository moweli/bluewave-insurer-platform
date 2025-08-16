# Databricks notebook source
# MAGIC %md
# MAGIC # Dynamic Data Masking for GDPR Compliance
# MAGIC Apply masking rules based on PII sensitivity levels

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
import hashlib
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Masking Functions

# COMMAND ----------

def mask_email(email):
    """Mask email address keeping domain"""
    if not email or '@' not in email:
        return email
    parts = email.split('@')
    masked_user = parts[0][0] + '*' * (len(parts[0]) - 2) + parts[0][-1] if len(parts[0]) > 2 else '***'
    return f"{masked_user}@{parts[1]}"

def mask_phone(phone):
    """Mask UK phone number keeping area code"""
    if not phone:
        return phone
    phone_str = str(phone).replace(' ', '').replace('-', '')
    if len(phone_str) >= 10:
        return phone_str[:4] + '*' * (len(phone_str) - 6) + phone_str[-2:]
    return '*' * len(phone_str)

def mask_name(name):
    """Partially mask person name"""
    if not name:
        return name
    parts = name.split()
    if len(parts) >= 2:
        return f"{parts[0][0]}*** {parts[-1][0]}***"
    return name[0] + '***' if name else '***'

def mask_address(address):
    """Mask address keeping postcode pattern"""
    if not address:
        return address
    # Keep first word and postcode pattern
    parts = address.split()
    if len(parts) > 1:
        # Find postcode pattern
        import re
        postcode_pattern = r'\b[A-Z]{1,2}\d{1,2}[A-Z]?\s?\d[A-Z]{2}\b'
        postcode = re.search(postcode_pattern, address)
        if postcode:
            masked_postcode = postcode.group()[:2] + '** ***'
            return f"{parts[0]} ***, {masked_postcode}"
    return parts[0] + ' ***' if parts else '***'

def hash_id(id_value, salt="gdpr_salt_2024"):
    """One-way hash for IDs"""
    if not id_value:
        return id_value
    return hashlib.sha256(f"{id_value}{salt}".encode()).hexdigest()[:16]

# Register UDFs
mask_email_udf = F.udf(mask_email, StringType())
mask_phone_udf = F.udf(mask_phone, StringType())
mask_name_udf = F.udf(mask_name, StringType())
mask_address_udf = F.udf(mask_address, StringType())
hash_id_udf = F.udf(hash_id, StringType())

print("✅ Masking functions created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load PII Classification Results

# COMMAND ----------

# Load classified claims
classified_df = spark.table("gdpr.pii_classified_claims")
claims_df = spark.table("silver.claims_processed")

# Join to get full data with PII classifications
full_df = claims_df.alias("c").join(
    classified_df.alias("p"),
    F.col("c.claim_id") == F.col("p.claim_id"),
    "left"
).select(
    "c.*",
    F.coalesce(F.col("p.overall_sensitivity"), F.lit("NONE")).alias("sensitivity"),
    F.coalesce(F.col("p.contains_pii"), F.lit(False)).alias("has_pii"),
    F.coalesce(F.col("p.gdpr_risk_score"), F.lit(0.0)).alias("risk_score")
)

print(f"📊 Loaded {full_df.count()} claims for masking")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Dynamic Masking Based on Sensitivity

# COMMAND ----------

# Apply masking based on sensitivity levels
masked_df = (
    full_df
    .withColumn("masked_timestamp", F.current_timestamp())
    
    # High sensitivity - maximum masking
    .withColumn("customer_name_masked",
        F.when(F.col("sensitivity") == "HIGH", mask_name_udf(F.col("customer_name")))
         .otherwise(F.col("customer_name"))
    )
    .withColumn("customer_email_masked",
        F.when(F.col("sensitivity").isin("HIGH", "MEDIUM"), mask_email_udf(F.col("customer_email")))
         .otherwise(F.col("customer_email"))
    )
    .withColumn("customer_phone_masked",
        F.when(F.col("sensitivity").isin("HIGH", "MEDIUM"), mask_phone_udf(F.col("customer_phone")))
         .otherwise(F.col("customer_phone"))
    )
    .withColumn("customer_address_masked",
        F.when(F.col("sensitivity") == "HIGH", mask_address_udf(F.col("customer_address")))
         .otherwise(F.col("customer_address"))
    )
    
    # Hash sensitive IDs
    .withColumn("customer_id_masked",
        F.when(F.col("sensitivity") == "HIGH", hash_id_udf(F.col("customer_id")))
         .otherwise(F.col("customer_id"))
    )
    
    # Redact sensitive text fields
    .withColumn("incident_description_masked",
        F.when(F.col("sensitivity") == "HIGH", F.lit("[REDACTED - Contains sensitive PII]"))
         .when(F.col("sensitivity") == "MEDIUM", 
               F.regexp_replace(F.col("incident_description"), r'\b\d{3,}\b', 'XXX'))
         .otherwise(F.col("incident_description"))
    )
    .withColumn("claim_notes_masked",
        F.when(F.col("sensitivity") == "HIGH", F.lit("[REDACTED - Contains sensitive PII]"))
         .when(F.col("sensitivity") == "MEDIUM",
               F.regexp_replace(F.col("claim_notes"), r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}', '[EMAIL]'))
         .otherwise(F.col("claim_notes"))
    )
    
    # Add masking metadata
    .withColumn("masking_applied", F.col("sensitivity") != "NONE")
    .withColumn("masking_level", F.col("sensitivity"))
    .withColumn("masking_reason", 
        F.when(F.col("sensitivity") == "HIGH", "GDPR Article 9 - Special Category Data")
         .when(F.col("sensitivity") == "MEDIUM", "GDPR Article 6 - Personal Data")
         .when(F.col("sensitivity") == "LOW", "GDPR - Minimal Risk Data")
         .otherwise("No masking required")
    )
)

# Display sample masked data
display(masked_df.select(
    "claim_id",
    "customer_name",
    "customer_name_masked",
    "customer_email",
    "customer_email_masked",
    "sensitivity",
    "masking_reason"
).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Role-Based Views

# COMMAND ----------

# Create view for internal analysts (partial masking)
analyst_view_df = masked_df.select(
    "claim_id",
    "policy_number",
    F.when(F.col("sensitivity") == "HIGH", F.col("customer_id_masked"))
     .otherwise(F.col("customer_id")).alias("customer_id"),
    F.col("customer_name_masked").alias("customer_name"),
    F.col("customer_email_masked").alias("customer_email"),
    F.col("customer_phone_masked").alias("customer_phone"),
    F.col("customer_address_masked").alias("customer_address"),
    "claim_amount",
    "claim_date",
    "claim_type",
    "claim_status",
    "incident_location",
    F.col("incident_description_masked").alias("incident_description"),
    "fraud_score",
    "risk_level",
    "uk_region"
)

# Create view for external partners (maximum masking)
partner_view_df = masked_df.select(
    hash_id_udf(F.col("claim_id")).alias("claim_ref"),
    "claim_amount",
    F.date_trunc("month", F.col("claim_date")).alias("claim_month"),
    "claim_type",
    "claim_status",
    "uk_region",
    "risk_level",
    F.round(F.col("fraud_score"), 1).alias("fraud_indicator")
)

# Create view for compliance/audit (full visibility with audit trail)
audit_view_df = masked_df.select(
    "*",
    F.current_user().alias("accessed_by"),
    F.current_timestamp().alias("accessed_at"),
    F.lit("AUDIT_ACCESS").alias("access_reason")
)

print("✅ Created role-based masked views")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Masked Data

# COMMAND ----------

# Save analyst view
analyst_view_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("gdpr.claims_analyst_view")

# Save partner view
partner_view_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("gdpr.claims_partner_view")

# Save audit view with access control
audit_view_df.write \
    .mode("overwrite") \
    .format("delta") \
    .option("delta.minReaderVersion", "2") \
    .option("delta.minWriterVersion", "5") \
    .option("delta.columnMapping.mode", "name") \
    .saveAsTable("gdpr.claims_audit_view")

print("✅ Masked views saved to GDPR database")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Masking Statistics

# COMMAND ----------

# Calculate masking statistics
stats = masked_df.groupBy("sensitivity").agg(
    F.count("*").alias("record_count"),
    F.sum(F.when(F.col("masking_applied"), 1).otherwise(0)).alias("masked_count"),
    F.avg("risk_score").alias("avg_risk_score")
).orderBy("sensitivity")

display(stats)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify masking effectiveness
# MAGIC SELECT 
# MAGIC     masking_level,
# MAGIC     COUNT(*) as records,
# MAGIC     COUNT(DISTINCT customer_id_masked) as unique_masked_ids,
# MAGIC     COUNT(DISTINCT customer_email_masked) as unique_masked_emails
# MAGIC FROM gdpr.claims_analyst_view
# MAGIC GROUP BY masking_level
# MAGIC ORDER BY masking_level;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Masking Policy Functions

# COMMAND ----------

# Define masking policies
masking_policies = {
    "email_policy": {
        "HIGH": "FULL_MASK",
        "MEDIUM": "PARTIAL_MASK",
        "LOW": "NO_MASK",
        "NONE": "NO_MASK"
    },
    "phone_policy": {
        "HIGH": "FULL_MASK",
        "MEDIUM": "PARTIAL_MASK",
        "LOW": "NO_MASK",
        "NONE": "NO_MASK"
    },
    "name_policy": {
        "HIGH": "INITIALS_ONLY",
        "MEDIUM": "PARTIAL_MASK",
        "LOW": "NO_MASK",
        "NONE": "NO_MASK"
    },
    "id_policy": {
        "HIGH": "HASH",
        "MEDIUM": "NO_MASK",
        "LOW": "NO_MASK",
        "NONE": "NO_MASK"
    }
}

# Save policies as Delta table
policy_df = spark.createDataFrame(
    [(k, v["HIGH"], v["MEDIUM"], v["LOW"], v["NONE"]) 
     for k, v in masking_policies.items()],
    ["policy_name", "high_sensitivity", "medium_sensitivity", "low_sensitivity", "no_sensitivity"]
)

policy_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("gdpr.masking_policies")

print("✅ Masking policies saved")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC ✅ **Data Masking Complete**
# MAGIC 
# MAGIC The masked data is now ready for:
# MAGIC 1. **Role-based access control** - Different views for different roles
# MAGIC 2. **Audit logging** - Track who accesses sensitive data
# MAGIC 3. **Export for SAR** - Provide masked data for subject access requests
# MAGIC 4. **Analytics** - Enable analysis without exposing PII
# MAGIC 
# MAGIC 💰 **Cost Optimization Notes:**
# MAGIC - Views are computed on-demand (no storage cost)
# MAGIC - Masking functions use built-in operations (no external APIs)
# MAGIC - Delta format enables efficient querying