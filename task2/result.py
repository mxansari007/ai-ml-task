# Open a new Python script or Jupyter notebook and run:
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Configure Spark with Delta
builder = SparkSession.builder \
    .appName("Delta Table Viewer") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Path to Delta table
delta_path = "data/delta_output/T1_delta"

# Read Delta table
delta_df = spark.read.format("delta").load(delta_path)

# Show data
delta_df.show(truncate=False)
