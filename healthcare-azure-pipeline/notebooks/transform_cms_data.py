# Databricks notebook source
# notebooks/transform_cms_data.py
# ─────────────────────────────────────────────────────────────
# Healthcare Azure Pipeline — Databricks PySpark Transformation
#
# This notebook runs inside Databricks and:
#   1. Reads raw CMS CSV from ADLS Gen2
#   2. Cleans and transforms the data with PySpark
#   3. Adds derived business metrics
#   4. Writes output as Delta Lake format to ADLS
#   5. Registers the Delta table in Databricks catalog
#
# Parameters (passed by ADF or manually):
#   year         : Dataset year (default: 2023)
#   sample_rows  : If > 0, process only N rows (for testing)
# ─────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## Healthcare ETL Pipeline — CMS Medicare Part D Transformation
# MAGIC **Source:** Azure Data Lake Gen2 (raw zone)
# MAGIC **Output:** Delta Lake (processed zone)

# COMMAND ----------

# Parameters — can be overridden by ADF pipeline
dbutils.widgets.text("year", "2023", "Dataset Year")
dbutils.widgets.text("sample_rows", "0", "Sample Rows (0 = full dataset)")
dbutils.widgets.text("adls_account", "healthcaredlmengqi", "ADLS Account Name")
dbutils.widgets.text("container", "healthcare-data", "ADLS Container")

year         = dbutils.widgets.get("year")
sample_rows  = int(dbutils.widgets.get("sample_rows"))
adls_account = dbutils.widgets.get("adls_account")
container    = dbutils.widgets.get("container")

print(f"Processing year={year}, sample_rows={sample_rows}")

# COMMAND ----------
# MAGIC %md ### Step 1: Configure ADLS Gen2 Access

# COMMAND ----------

# Configure Spark to access ADLS Gen2
# Uses the Databricks cluster's managed identity
spark.conf.set(
    f"fs.azure.account.auth.type.{adls_account}.dfs.core.windows.net",
    "OAuth"
)
spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{adls_account}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)

# Base paths
raw_base       = f"abfss://{container}@{adls_account}.dfs.core.windows.net/raw"
processed_base = f"abfss://{container}@{adls_account}.dfs.core.windows.net/processed"

input_path  = f"{raw_base}/source=cms/year={year}/cms_partd_{year}.csv"
output_path = f"{processed_base}/delta/partd_prescribers/year={year}"

print(f"Input:  {input_path}")
print(f"Output: {output_path}")

# COMMAND ----------
# MAGIC %md ### Step 2: Read Raw Data

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timezone

# Define schema explicitly — never rely on inferred types in production
schema = StructType([
    StructField("Prscrbr_NPI",                   StringType(),  True),
    StructField("Prscrbr_Last_Org_Name",          StringType(),  True),
    StructField("Prscrbr_First_Name",             StringType(),  True),
    StructField("Prscrbr_City",                   StringType(),  True),
    StructField("Prscrbr_State_Abrvtn",           StringType(),  True),
    StructField("Prscrbr_State_FIPS",             StringType(),  True),
    StructField("Prscrbr_Type",                   StringType(),  True),
    StructField("Prscrbr_Type_Src",               StringType(),  True),
    StructField("Brnd_Name",                      StringType(),  True),
    StructField("Gnrc_Name",                      StringType(),  True),
    StructField("Tot_Clms",                       DoubleType(),  True),
    StructField("Tot_30day_Fills",                DoubleType(),  True),
    StructField("Tot_Day_Suply",                  DoubleType(),  True),
    StructField("Tot_Drug_Cst",                   DoubleType(),  True),
    StructField("Tot_Benes",                      DoubleType(),  True),
    StructField("GE65_Sprsn_Flag",                StringType(),  True),
    StructField("GE65_Tot_Clms",                  DoubleType(),  True),
    StructField("GE65_Tot_30day_Fills",           DoubleType(),  True),
    StructField("GE65_Tot_Day_Suply",             DoubleType(),  True),
    StructField("GE65_Tot_Drug_Cst",              DoubleType(),  True),
    StructField("GE65_Tot_Benes",                 DoubleType(),  True),
    StructField("GE65_Bene_Sprsn_Flag",           StringType(),  True),
])

df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .schema(schema)
    .csv(input_path)
)

if sample_rows > 0:
    df_raw = df_raw.limit(sample_rows)
    print(f"Sampling {sample_rows:,} rows")

row_count = df_raw.count()
print(f"Loaded {row_count:,} rows × {len(df_raw.columns)} columns")

# COMMAND ----------
# MAGIC %md ### Step 3: Clean Data

# COMMAND ----------

df_clean = (
    df_raw
    # Standardize text
    .withColumn("Prscrbr_State_Abrvtn", F.upper(F.trim(F.col("Prscrbr_State_Abrvtn"))))
    .withColumn("Prscrbr_City",         F.initcap(F.trim(F.col("Prscrbr_City"))))
    .withColumn("Prscrbr_Type",         F.trim(F.col("Prscrbr_Type")))
    .withColumn("Gnrc_Name",            F.trim(F.col("Gnrc_Name")))
    .withColumn("Brnd_Name",            F.trim(F.col("Brnd_Name")))

    # Handle CMS suppression flags
    .withColumn("GE65_Sprsn_Flag",
                F.when(F.col("GE65_Sprsn_Flag") == "*", "suppressed")
                 .otherwise(F.col("GE65_Sprsn_Flag")))

    # Remove invalid rows
    .filter(F.col("Prscrbr_NPI").isNotNull())
    .filter(F.col("Tot_Clms") > 0)

    # Drop duplicates
    .dropDuplicates(["Prscrbr_NPI", "Gnrc_Name"])
)

clean_count = df_clean.count()
print(f"After cleaning: {clean_count:,} rows ({row_count - clean_count:,} removed)")

# COMMAND ----------
# MAGIC %md ### Step 4: Add Derived Columns & Rename

# COMMAND ----------

df_enriched = (
    df_clean

    # Business metrics
    .withColumn("cost_per_claim",
                F.when(F.col("Tot_Clms") > 0,
                       F.round(F.col("Tot_Drug_Cst") / F.col("Tot_Clms"), 2))
                 .otherwise(F.lit(None).cast(DoubleType())))

    .withColumn("cost_per_beneficiary",
                F.when(F.col("Tot_Benes") > 0,
                       F.round(F.col("Tot_Drug_Cst") / F.col("Tot_Benes"), 2))
                 .otherwise(F.lit(None).cast(DoubleType())))

    .withColumn("senior_claim_rate",
                F.when(
                    (F.col("GE65_Sprsn_Flag") != "suppressed") &
                    (F.col("Tot_Clms") > 0),
                    F.round(F.col("GE65_Tot_Clms") / F.col("Tot_Clms"), 4)
                ).otherwise(F.lit(None).cast(DoubleType())))

    .withColumn("drug_type",
                F.when(
                    F.col("Brnd_Name").isNotNull() & (F.col("Brnd_Name") != ""),
                    "brand"
                ).otherwise("generic"))

    # Rename to snake_case
    .withColumnRenamed("Prscrbr_NPI",          "prescriber_npi")
    .withColumnRenamed("Prscrbr_Last_Org_Name", "prescriber_last_name")
    .withColumnRenamed("Prscrbr_First_Name",    "prescriber_first_name")
    .withColumnRenamed("Prscrbr_City",          "prescriber_city")
    .withColumnRenamed("Prscrbr_State_Abrvtn",  "prescriber_state")
    .withColumnRenamed("Prscrbr_State_FIPS",    "state_fips")
    .withColumnRenamed("Prscrbr_Type",          "prescriber_specialty")
    .withColumnRenamed("Prscrbr_Type_Src",      "specialty_source")
    .withColumnRenamed("Brnd_Name",             "brand_name")
    .withColumnRenamed("Gnrc_Name",             "generic_name")
    .withColumnRenamed("Tot_Clms",              "total_claims")
    .withColumnRenamed("Tot_30day_Fills",        "total_30day_fills")
    .withColumnRenamed("Tot_Day_Suply",         "total_day_supply")
    .withColumnRenamed("Tot_Drug_Cst",          "total_drug_cost")
    .withColumnRenamed("Tot_Benes",             "total_beneficiaries")
    .withColumnRenamed("GE65_Sprsn_Flag",       "senior_suppression_flag")
    .withColumnRenamed("GE65_Tot_Clms",         "senior_total_claims")
    .withColumnRenamed("GE65_Tot_30day_Fills",   "senior_30day_fills")
    .withColumnRenamed("GE65_Tot_Day_Suply",    "senior_day_supply")
    .withColumnRenamed("GE65_Tot_Drug_Cst",     "senior_drug_cost")
    .withColumnRenamed("GE65_Tot_Benes",        "senior_beneficiaries")
    .withColumnRenamed("GE65_Bene_Sprsn_Flag",  "senior_bene_suppression_flag")

    # Pipeline metadata
    .withColumn("data_year",         F.lit(int(year)))
    .withColumn("processed_at",      F.lit(datetime.now(timezone.utc).isoformat()))
    .withColumn("pipeline_version",  F.lit("2.0.0"))
    .withColumn("pipeline_platform", F.lit("azure-databricks"))
)

print(f"Enriched schema: {len(df_enriched.columns)} columns")

# COMMAND ----------
# MAGIC %md ### Step 5: Write Delta Lake Output

# COMMAND ----------

(
    df_enriched
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("prescriber_state")
    .save(output_path)
)

print(f"Delta Lake written to: {output_path}")

# COMMAND ----------
# MAGIC %md ### Step 6: Register Delta Table in Catalog

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS healthcare")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS healthcare.partd_prescribers_{year}
    USING DELTA
    LOCATION '{output_path}'
""")

# Refresh table metadata
spark.sql(f"MSCK REPAIR TABLE healthcare.partd_prescribers_{year}")

print(f"Table registered: healthcare.partd_prescribers_{year}")

# COMMAND ----------
# MAGIC %md ### Step 7: Quick Validation

# COMMAND ----------

final_count = spark.read.format("delta").load(output_path).count()
print(f"\n{'='*50}")
print(f"  TRANSFORMATION COMPLETE")
print(f"  Input rows  : {row_count:,}")
print(f"  Output rows : {final_count:,}")
print(f"  Delta path  : {output_path}")
print(f"  Format      : Delta Lake (ACID, time travel enabled)")
print(f"{'='*50}")

# Return summary for ADF
dbutils.notebook.exit(f'{{"status":"success","input_rows":{row_count},"output_rows":{final_count},"output_path":"{output_path}"}}')
