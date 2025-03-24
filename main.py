from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col, to_date, to_timestamp, current_date, current_timestamp, array, map_from_arrays, lit

# Configure Spark with Delta
builder = SparkSession.builder \
    .appName("CSV to Delta Kafka") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Read CSV into Spark DataFrame
csv_path = "data/T1.csv"
df = spark.read.csv(csv_path, header=True, inferSchema=True)

# Rename columns to remove invalid characters for Delta
df = df.withColumnRenamed("Date/Time", "DateTime") \
       .withColumnRenamed("LV ActivePower (kW)", "LV_ActivePower_kW") \
       .withColumnRenamed("Wind Speed (m/s)", "Wind_Speed_m_s") \
       .withColumnRenamed("Theoretical_Power_Curve (KWh)", "Theoretical_Power_Curve_KWh") \
       .withColumnRenamed("Wind Direction (°)", "Wind_Direction_deg")

# Create new columns based on requirements
df_transformed = df.withColumn(
    "signal_date", to_date(col("DateTime"), "yyyy-MM-dd")
).withColumn(
    "signal_ts", to_timestamp(col("DateTime"), "yyyy-MM-dd'T'HH:mm:ss")
).withColumn(
    "create_date", current_date()
).withColumn(
    "create_ts", current_timestamp()
)

# Handle duplicate keys policy for map columns
spark.conf.set("spark.sql.mapKeyDedupPolicy", "LAST_WIN")

# Correctly define signal_keys using lit()
signal_keys = array(
    lit("LV_ActivePower_kW"),
    lit("Wind_Speed_m_s"),
    lit("Theoretical_Power_Curve_KWh"),
    lit("Wind_Direction_deg")
)

# Cast values to string and define signal_values
signal_values = array(
    col("LV_ActivePower_kW").cast("string"),
    col("Wind_Speed_m_s").cast("string"),
    col("Theoretical_Power_Curve_KWh").cast("string"),
    col("Wind_Direction_deg").cast("string")
)

# Add 'signals' map to DataFrame
df_transformed = df_transformed.withColumn(
    "signals", map_from_arrays(signal_keys, signal_values)
)

# Select final columns
final_df = df_transformed.select(
    "signal_date",
    "signal_ts",
    "create_date",
    "create_ts",
    "signals"
)

# ✅ Write DataFrame to Delta format
delta_path = "data/delta_output/T1_delta"
final_df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(delta_path)

# ✅ Also write as Parquet for Kafka producer
parquet_path = "data/final_df.parquet"
final_df.write.mode("overwrite").parquet(parquet_path)

print(f"✅ Data successfully written to Delta format at {delta_path} and Parquet at {parquet_path}")
