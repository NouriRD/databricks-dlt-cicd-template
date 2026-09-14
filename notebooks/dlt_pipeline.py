import dlt
from pyspark.sql.functions import *

# 1. BRONZE: Jib l data lkhayba b7al ma hiya
@dlt.table(
  comment="Raw data from customers CSV"
)
def bronze_customers():
  return spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("header", "true") \
    .load("/databricks-datasets/retail-org/customers/")

# 2. SILVER: N9iw l data
@dlt.table(
  comment="Cleaned customers data"
)
def silver_customers():
  df = dlt.read_stream("bronze_customers")
  return df.withColumn("customer_id", col("customer_id").cast("int")) \
           .dropna()

# 3. GOLD: Table finale l BI
@dlt.table(
  comment="Gold table: customers count by country"
)
def gold_customers_by_country():
  df = dlt.read("silver_customers")
  return df.groupBy("country").count()