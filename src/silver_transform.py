# src/silver_transform.py
import os
from pathlib import Path
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
BRONZE_CSV_PATH = str(PROJECT_ROOT / "data" / "bronze" / "egx_raw_latest.csv")
SILVER_DELTA_PATH = str(PROJECT_ROOT / "data" / "silver" / "silver_clean_financials")

if not os.path.exists(BRONZE_CSV_PATH):
    raise FileNotFoundError(f" Bronze file missing at: {BRONZE_CSV_PATH}")

builder = (
    SparkSession.builder.appName("EGX-Silver-Incremental-Cleansing")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df_bronze_raw = (
    spark.read.option("header", True)
    .option("inferSchema", False)
    .csv(BRONZE_CSV_PATH)
)

# 4. Clean & Standardize
df_bronze_clean = (
    df_bronze_raw.withColumnRenamed("Financial Term", "financial_term_raw")
    .withColumnRenamed("Company", "company_raw")
    .withColumnRenamed("Statement Type", "statement_type_raw")
    .withColumnRenamed("End Of Quarter", "period_raw")
    .withColumnRenamed("Amount", "amount_raw")
    .withColumn("company", F.trim(F.col("company_raw")))
    .withColumn("statement_type", F.trim(F.col("statement_type_raw")))
    .withColumn("financial_term", F.trim(F.col("financial_term_raw")))
    .withColumn(
        "helper",
        F.lower(F.concat_ws("_", F.col("statement_type"), F.col("financial_term"))),
    )
    .withColumn(
        "period_date_str",
        F.regexp_extract(F.col("period_raw"), r"([A-Za-z]{3}\s\d{1,2},\s\d{4})", 1),
    )
    .withColumn("end_of_quarter", F.to_date(F.col("period_date_str"), "MMM dd, yyyy"))
    .withColumn(
        "amount",
        F.expr("try_cast(regexp_replace(trim(amount_raw), ',', '') AS DECIMAL(18,4))"),
    )
    .filter(F.col("end_of_quarter").isNotNull() & F.col("amount").isNotNull())
    .withColumn(
        "financial_record_id",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("company"), F.lit("")),
                F.coalesce(F.col("statement_type"), F.lit("")),
                F.coalesce(F.col("financial_term"), F.lit("")),
                F.coalesce(F.col("end_of_quarter").cast("string"), F.lit("")),
            ),
            256,
        ),
    )
    .select(
        "financial_record_id",
        "company",
        "statement_type",
        "financial_term",
        "helper",
        "end_of_quarter",
        "amount",
        F.col("Extracted_At_UTC").alias("extracted_at_utc"),
        F.col("Run_ID").alias("run_id"),
    )
    .dropDuplicates(["financial_record_id"])
)

if DeltaTable.isDeltaTable(spark, SILVER_DELTA_PATH):
    delta_silver = DeltaTable.forPath(spark, SILVER_DELTA_PATH)

    (
        delta_silver.alias("target")
        .merge(
            df_bronze_clean.alias("source"),
            "target.financial_record_id = source.financial_record_id",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    (
        df_bronze_clean.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(SILVER_DELTA_PATH)
    )

spark.stop()