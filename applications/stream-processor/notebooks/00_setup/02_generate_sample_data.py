# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Sample Insurance Claims Data
# MAGIC Creates realistic UK insurance data for testing GDPR compliance

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random
import string

# COMMAND ----------

# Generate sample claims with PII
def generate_sample_claims(num_records=1000):
    """Generate UK insurance claims with realistic PII"""
    
    # UK-specific data
    uk_names = ["James Smith", "Emma Johnson", "Oliver Williams", "Sophia Brown", "William Jones", 
                 "Isabella Taylor", "George Davies", "Charlotte Wilson", "Harry Evans", "Amelia Thomas"]
    
    uk_postcodes = ["SW1A 1AA", "E1 6AN", "M1 1AE", "B1 1AA", "L1 0AA", 
                     "G1 1AA", "EH1 1AA", "CF10 1AA", "BT1 1AA", "NR1 1AA"]
    
    uk_cities = ["London", "Manchester", "Birmingham", "Liverpool", "Glasgow", 
                 "Edinburgh", "Cardiff", "Belfast", "Norwich", "Bristol"]
    
    claim_types = ["motor_collision", "home_flood", "home_burglary", "travel_cancellation", 
                   "motor_theft", "home_storm", "personal_injury", "motor_windscreen"]
    
    data = []
    
    for i in range(num_records):
        # Generate realistic UK data
        customer_name = random.choice(uk_names)
        customer_id = f"CUST_{random.randint(10000, 99999)}"
        
        # Email with potential PII
        email_user = customer_name.lower().replace(" ", ".") + str(random.randint(1, 99))
        customer_email = f"{email_user}@{random.choice(['gmail.com', 'outlook.com', 'yahoo.co.uk', 'btinternet.com'])}"
        
        # UK phone number
        customer_phone = f"07{random.randint(100000000, 999999999)}"
        
        # Address with postcode
        house_number = random.randint(1, 200)
        street_names = ["High Street", "Church Road", "Main Street", "Park Road", "Victoria Road"]
        customer_address = f"{house_number} {random.choice(street_names)}, {random.choice(uk_cities)}"
        
        # Claim details
        claim_id = f"CLM_{datetime.now().strftime('%Y%m%d')}_{random.randint(1000, 9999)}"
        policy_number = f"POL_{random.randint(100000, 999999)}"
        
        # Claim amount - realistic distribution
        if random.random() < 0.7:  # 70% small claims
            claim_amount = round(random.uniform(100, 2000), 2)
        elif random.random() < 0.95:  # 25% medium claims
            claim_amount = round(random.uniform(2000, 10000), 2)
        else:  # 5% large claims
            claim_amount = round(random.uniform(10000, 50000), 2)
        
        # Dates
        claim_date = datetime.now() - timedelta(days=random.randint(0, 30))
        
        # Incident description with potential PII
        incident_descriptions = [
            f"Collision at {random.choice(uk_cities)} roundabout. Other driver was {random.choice(uk_names)}.",
            f"Water damage from burst pipe. Neighbor {random.choice(uk_names)} at flat {random.randint(1,20)} also affected.",
            f"Stolen from {customer_address}. Police report filed with PC {random.randint(1000,9999)}.",
            f"Medical emergency while visiting {random.choice(uk_cities)}. Treated by Dr. {random.choice(['Smith', 'Jones', 'Wilson'])}.",
            f"Storm damage to property. Insurance assessor {random.choice(uk_names)} visited on {claim_date.strftime('%Y-%m-%d')}."
        ]
        
        # Notes with PII
        claim_notes = f"Customer contact: {customer_phone}. Email: {customer_email}. " + random.choice(incident_descriptions)
        
        # Fraud score
        fraud_score = round(random.random() * 0.3 if random.random() > 0.035 else random.uniform(0.7, 0.95), 2)
        
        record = {
            'claim_id': claim_id,
            'policy_number': policy_number,
            'customer_id': customer_id,
            'customer_name': customer_name,
            'customer_email': customer_email,
            'customer_phone': customer_phone,
            'customer_address': customer_address,
            'claim_amount': claim_amount,
            'claim_date': claim_date,
            'claim_type': random.choice(claim_types),
            'claim_status': random.choice(['OPEN', 'PROCESSING', 'SETTLED', 'REJECTED']),
            'incident_location': f"{random.choice(uk_cities)}, {random.choice(uk_postcodes)}",
            'incident_description': random.choice(incident_descriptions),
            'claim_notes': claim_notes,
            'postcode': random.choice(uk_postcodes),
            'fraud_score': fraud_score
        }
        
        data.append(record)
    
    return spark.createDataFrame(data)

# COMMAND ----------

# Generate sample data
print("🔄 Generating sample claims data...")
sample_claims_df = generate_sample_claims(1000)

print(f"✅ Generated {sample_claims_df.count()} sample claims")
display(sample_claims_df.limit(5))

# COMMAND ----------

# Add processing columns for silver layer
silver_claims_df = (
    sample_claims_df
    .withColumn("uk_region", 
        F.when(F.col("postcode").startswith("SW"), "London")
         .when(F.col("postcode").startswith("E"), "London")
         .when(F.col("postcode").startswith("M"), "Manchester")
         .when(F.col("postcode").startswith("B"), "Birmingham")
         .when(F.col("postcode").startswith("L"), "Liverpool")
         .when(F.col("postcode").startswith("G"), "Glasgow")
         .otherwise("Other")
    )
    .withColumn("high_value_claim", F.col("claim_amount") > 10000)
    .withColumn("suspicious_timing", F.hour("claim_date").between(0, 6))
    .withColumn("is_suspicious", F.col("fraud_score") > 0.7)
    .withColumn("risk_level",
        F.when(F.col("fraud_score") > 0.8, "HIGH")
         .when(F.col("fraud_score") > 0.5, "MEDIUM")
         .when(F.col("fraud_score") > 0.3, "LOW")
         .otherwise("NONE")
    )
    .withColumn("processed_time", F.current_timestamp())
    .withColumn("processing_date", F.current_date())
)

# COMMAND ----------

# Write to silver table
print("💾 Writing to silver.claims_processed...")
silver_claims_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("silver.claims_processed")

print("✅ Sample data loaded to silver layer")

# COMMAND ----------

# Verify data
display(spark.sql("""
    SELECT 
        COUNT(*) as total_claims,
        COUNT(DISTINCT customer_id) as unique_customers,
        AVG(claim_amount) as avg_claim_amount,
        SUM(CASE WHEN is_suspicious THEN 1 ELSE 0 END) as suspicious_claims,
        SUM(CASE WHEN high_value_claim THEN 1 ELSE 0 END) as high_value_claims
    FROM silver.claims_processed
"""))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check data distribution
# MAGIC SELECT 
# MAGIC     uk_region,
# MAGIC     claim_type,
# MAGIC     risk_level,
# MAGIC     COUNT(*) as count
# MAGIC FROM silver.claims_processed
# MAGIC GROUP BY uk_region, claim_type, risk_level
# MAGIC ORDER BY count DESC
# MAGIC LIMIT 20;

# COMMAND ----------

print("✅ Sample data ready for GDPR compliance testing")
print("📊 Data contains realistic PII including:")
print("   - Names, emails, phone numbers")
print("   - UK postcodes and addresses")
print("   - Incident descriptions with third-party names")
print("   - Claims notes with contact details")
print("")
print("Ready for Day 4 GDPR implementation!")