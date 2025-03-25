from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip



# ✅ Configure Spark with Delta
builder = SparkSession.builder \
    .appName("Delta Data for ML") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# ✅ Create Spark session
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Path to your Delta table
delta_path = "data/delta_output/T1_delta"

# ✅ Load data from Delta
df = spark.read.format("delta").load(delta_path)

# ✅ Show the DataFrame
df.show(5)
