from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PySparkDataTransformation") \
    .getOrCreate()

# Read input CSV file
df = spark.read.csv("raw_data.csv", header=True, inferSchema=True)

# Data cleaning & transformation
df_clean = (
    df.dropDuplicates()
      .filter(col("value").isNotNull())
      .withColumn("value_double", col("value") * 2)
      .filter(col("value") > 0)
)

# Write cleaned data to Parquet
df_clean.write.mode("overwrite").parquet("output/clean_data.parquet")

spark.stop()
