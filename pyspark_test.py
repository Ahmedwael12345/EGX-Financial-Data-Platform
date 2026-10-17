from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("EGXFinancialDataPlatform")
    .master("local[*]")
    .getOrCreate()
)

data = [
    ("EFID", "Revenue", 1_000_000.0),
    ("SWDY", "Revenue", 2_500_000.0),
    ("EFID", "Net Income", 150_000.0),
]

df = spark.createDataFrame(
    data,
    ["Company", "Financial_Term", "Amount"]
)

df.show()
df.printSchema()

spark.stop()