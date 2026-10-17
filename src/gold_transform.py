# src/gold_transform.py
import getpass
import os
import sys
from pathlib import Path
import psycopg2

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Project Paths
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
SILVER_DELTA_PATH = str(PROJECT_ROOT / "data" / "silver" / "silver_clean_financials")

# PostgreSQL Connection Config
JDBC_URL = "jdbc:postgresql://localhost:5432/egx_financial_db"
DB_USER = os.getenv("PGUSER", getpass.getuser())
DB_PASSWORD = os.getenv("PGPASSWORD", "")
DRIVER = "org.postgresql.Driver"


def ensure_gold_tables_and_constraints_exist():
    """Creates Gold schema, tables, and PK/FK constraints if they don't exist."""
    print("🛠️ Ensuring PostgreSQL Schema, Tables, and Foreign Keys exist...")
    conn = psycopg2.connect(
        dbname="egx_financial_db",
        user=DB_USER,
        password=DB_PASSWORD,
        host="localhost",
        port="5432",
    )
    conn.autocommit = True
    cursor = conn.cursor()

    init_sql = """
    CREATE SCHEMA IF NOT EXISTS gold;

    CREATE TABLE IF NOT EXISTS gold."DimStatements" (
        "Statement Name" TEXT,
        "Statement Type" TEXT PRIMARY KEY
    );

    CREATE TABLE IF NOT EXISTS gold."DimAccounts" (
        "Financial Term" TEXT,
        "Helper" TEXT PRIMARY KEY,
        "Statement Type" TEXT
    );

    CREATE TABLE IF NOT EXISTS gold."DimDate" (
        "Start of Quarter" DATE,
        "End of Quarter" DATE PRIMARY KEY,
        "Quarter" INT,
        "Start of Year" DATE,
        "End of Year" DATE,
        "Year" INT
    );

    CREATE TABLE IF NOT EXISTS gold."FactFinancials" (
        "financial_record_id" TEXT PRIMARY KEY,
        "Amount" NUMERIC(18,4),
        "Company" TEXT,
        "End Of Quarter" DATE REFERENCES gold."DimDate" ("End of Quarter"),
        "Financial Term" TEXT,
        "Helper" TEXT REFERENCES gold."DimAccounts" ("Helper"),
        "Statement Type" TEXT REFERENCES gold."DimStatements" ("Statement Type")
    );
    """
    cursor.execute(init_sql)
    cursor.close()
    conn.close()


# 2. Initialize Database DDL
ensure_gold_tables_and_constraints_exist()

# 3. Spark Session
builder = (
    SparkSession.builder.appName("EGX-Gold-Incremental-PostgreSQL")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(
    builder, extra_packages=["org.postgresql:postgresql:42.7.7"]
).getOrCreate()

if not os.path.exists(SILVER_DELTA_PATH):
    print(f"❌ Error: Delta path does not exist at {SILVER_DELTA_PATH}")
    sys.exit(1)

# 4. Load & Filter Silver Delta Data
df_silver_raw = spark.read.format("delta").load(SILVER_DELTA_PATH)
df_silver = df_silver_raw.filter(
    (F.col("end_of_quarter") >= "2020-01-01")
    & (F.month("end_of_quarter").isin([3, 6, 9, 12]))
)


def write_staging_table(df, staging_table_name):
    """Writes Spark DataFrame to a temporary PostgreSQL staging table."""
    (
        df.coalesce(1)
        .write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", f'gold."{staging_table_name}"')
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver", DRIVER)
        .mode("overwrite")
        .save()
    )


# ------------------------------------------------------------
# 5. Build Dimensions and Fact DataFrames
# ------------------------------------------------------------
dim_statements = (
    df_silver.select(F.col("statement_type").alias("Statement Type"))
    .distinct()
    .withColumn(
        "Statement Name",
        F.initcap(F.regexp_replace(F.col("Statement Type"), "-", " ")),
    )
    .select("Statement Name", "Statement Type")
)

dim_accounts = (
    df_silver.select(
        F.col("financial_term").alias("Financial Term"),
        F.col("helper").alias("Helper"),
        F.col("statement_type").alias("Statement Type"),
    )
    .dropDuplicates(["Helper"])
)

dim_date = (
    df_silver.select(F.col("end_of_quarter").alias("End of Quarter"))
    .distinct()
    .withColumn("Start of Quarter", F.trunc("End of Quarter", "quarter"))
    .withColumn("Quarter", F.quarter("End of Quarter"))
    .withColumn("Start of Year", F.trunc("End of Quarter", "year"))
    .withColumn(
        "End of Year",
        F.make_date(F.year("End of Quarter"), F.lit(12), F.lit(31)),
    )
    .withColumn("Year", F.year("End of Quarter"))
    .select(
        "Start of Quarter",
        "End of Quarter",
        "Quarter",
        "Start of Year",
        "End of Year",
        "Year",
    )
)

fact_financials = df_silver.select(
    "financial_record_id",
    F.col("amount").alias("Amount"),
    F.col("company").alias("Company"),
    F.col("end_of_quarter").alias("End Of Quarter"),
    F.col("financial_term").alias("Financial Term"),
    F.col("helper").alias("Helper"),
    F.col("statement_type").alias("Statement Type"),
)

# ------------------------------------------------------------
# 6. Write Staging Tables
# ------------------------------------------------------------
print("📦 Staging Gold tables in PostgreSQL...")
write_staging_table(dim_statements, "stg_DimStatements")
write_staging_table(dim_accounts, "stg_DimAccounts")
write_staging_table(dim_date, "stg_DimDate")
write_staging_table(fact_financials, "stg_FactFinancials")


# ------------------------------------------------------------
# 7. Execute PostgreSQL Incremental Upserts
# ------------------------------------------------------------
def execute_incremental_upserts():
    print("🔄 Upserting staged records into Target Gold Tables...")
    conn = psycopg2.connect(
        dbname="egx_financial_db",
        user=DB_USER,
        password=DB_PASSWORD,
        host="localhost",
        port="5432",
    )
    conn.autocommit = True
    cursor = conn.cursor()

    upsert_sql = """
    -- 1. DimStatements Upsert
    INSERT INTO gold."DimStatements" ("Statement Name", "Statement Type")
    SELECT "Statement Name", "Statement Type" FROM gold."stg_DimStatements"
    ON CONFLICT ("Statement Type") DO UPDATE
    SET "Statement Name" = EXCLUDED."Statement Name";

    -- 2. DimAccounts Upsert
    INSERT INTO gold."DimAccounts" ("Financial Term", "Helper", "Statement Type")
    SELECT "Financial Term", "Helper", "Statement Type" FROM gold."stg_DimAccounts"
    ON CONFLICT ("Helper") DO UPDATE
    SET "Financial Term" = EXCLUDED."Financial Term",
        "Statement Type" = EXCLUDED."Statement Type";

    -- 3. DimDate Upsert
    INSERT INTO gold."DimDate" ("Start of Quarter", "End of Quarter", "Quarter", "Start of Year", "End of Year", "Year")
    SELECT "Start of Quarter", "End of Quarter", "Quarter", "Start of Year", "End of Year", "Year" FROM gold."stg_DimDate"
    ON CONFLICT ("End of Quarter") DO NOTHING;

    -- 4. FactFinancials Upsert
    INSERT INTO gold."FactFinancials" ("financial_record_id", "Amount", "Company", "End Of Quarter", "Financial Term", "Helper", "Statement Type")
    SELECT "financial_record_id", "Amount", "Company", "End Of Quarter", "Financial Term", "Helper", "Statement Type" FROM gold."stg_FactFinancials"
    ON CONFLICT ("financial_record_id") DO UPDATE
    SET "Amount" = EXCLUDED."Amount",
        "Company" = EXCLUDED."Company",
        "End Of Quarter" = EXCLUDED."End Of Quarter",
        "Financial Term" = EXCLUDED."Financial Term",
        "Helper" = EXCLUDED."Helper",
        "Statement Type" = EXCLUDED."Statement Type";

    -- Cleanup Staging Tables
    DROP TABLE IF EXISTS gold."stg_DimStatements";
    DROP TABLE IF EXISTS gold."stg_DimAccounts";
    DROP TABLE IF EXISTS gold."stg_DimDate";
    DROP TABLE IF EXISTS gold."stg_FactFinancials";
    """
    cursor.execute(upsert_sql)
    cursor.close()
    conn.close()
    print("✅ Incremental PostgreSQL Gold Upsert Complete!")


execute_incremental_upserts()

print("\n🎉 Gold Incremental Load Complete & Star Schema Maintained!")
spark.stop()