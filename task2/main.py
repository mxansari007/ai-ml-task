from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import (
    col, to_date, to_timestamp, current_date, current_timestamp, 
    array, map_from_arrays, lit, when, avg, countDistinct, date_format, expr
)

# --------------------------------------
# 🔥 Configure Spark with Delta
# --------------------------------------
builder = SparkSession.builder \
    .appName("CSV to Delta Kafka Analysis") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# --------------------------------------
# 📚 Read CSV into Spark DataFrame
# --------------------------------------
csv_path = "data/T1.csv"
df = spark.read.csv(csv_path, header=True, inferSchema=True)

# --------------------------------------
# ✅ Rename columns to avoid invalid characters for Delta
# --------------------------------------
df = df.withColumnRenamed("Date/Time", "DateTime") \
       .withColumnRenamed("LV ActivePower (kW)", "LV_ActivePower_kW") \
       .withColumnRenamed("Wind Speed (m/s)", "Wind_Speed_m_s") \
       .withColumnRenamed("Theoretical_Power_Curve (KWh)", "Theoretical_Power_Curve_KWh") \
       .withColumnRenamed("Wind Direction (°)", "Wind_Direction_deg")

# --------------------------------------
# 🎯 Create new columns based on requirements
# --------------------------------------
# Correct DateTime Parsing
df_transformed = df.withColumn(
    "signal_date", to_date(col("DateTime"), "dd MM yyyy HH:mm")
).withColumn(
    "signal_ts", to_timestamp(col("DateTime"), "dd MM yyyy HH:mm")
).withColumn(
    "create_date", current_date()
).withColumn(
    "create_ts", current_timestamp()
)

# --------------------------------------
# 🗝️ Handle duplicate map keys
# --------------------------------------
spark.conf.set("spark.sql.mapKeyDedupPolicy", "LAST_WIN")

# Create 'signals' map with proper column names
signal_keys = array(
    lit("LV_ActivePower_kW"),
    lit("Wind_Speed_m_s"),
    lit("Theoretical_Power_Curve_KWh"),
    lit("Wind_Direction_deg")
)

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

# --------------------------------------
# 📊 Task 2: Distinct signal_ts per day
# --------------------------------------
distinct_signal_ts_per_day = df_transformed.groupBy("signal_date") \
    .agg(countDistinct("signal_ts").alias("distinct_signal_ts_count"))

# --------------------------------------
# 📈 Task 3: Average of all signals per hour
# --------------------------------------
avg_signals_per_hour = df_transformed.withColumn(
    "hour", date_format("signal_ts", "yyyy-MM-dd HH:00:00")
).groupBy("hour").agg(
    avg(col("LV_ActivePower_kW")).alias("avg_LV_ActivePower_kW"),
    avg(col("Wind_Speed_m_s")).alias("avg_Wind_Speed_m_s"),
    avg(col("Theoretical_Power_Curve_KWh")).alias("avg_Theoretical_Power_Curve_KWh"),
    avg(col("Wind_Direction_deg")).alias("avg_Wind_Direction_deg")
)

# --------------------------------------
# 🕹️ Task 4: Add `generation_indicator` column
# --------------------------------------
df_transformed = df_transformed.withColumn(
    "generation_indicator",
    when(col("LV_ActivePower_kW") < 200, "Low")
    .when((col("LV_ActivePower_kW") >= 200) & (col("LV_ActivePower_kW") < 600), "Medium")
    .when((col("LV_ActivePower_kW") >= 600) & (col("LV_ActivePower_kW") < 1000), "High")
    .otherwise("Exceptional")
)

# --------------------------------------
# 🔥 Task 5: Define signal mapping JSON
# --------------------------------------
signal_mapping_json = [
    {"sig_name": "LV ActivePower (kW)", "sig_mapping_name": "active_power_average"},
    {"sig_name": "Wind Speed (m/s)", "sig_mapping_name": "wind_speed_average"},
    {"sig_name": "Theoretical_Power_Curve (KWh)", "sig_mapping_name": "theo_power_curve_average"},
    {"sig_name": "Wind Direction (°)", "sig_mapping_name": "wind_direction_average"}
]

# Create a dictionary for mapping
signal_mapping_dict = {
    "LV_ActivePower_kW": "active_power_average",
    "Wind_Speed_m_s": "wind_speed_average",
    "Theoretical_Power_Curve_KWh": "theo_power_curve_average",
    "Wind_Direction_deg": "wind_direction_average"
}

# --------------------------------------
# 🔄 Task 6: Map signal names using dictionary
# --------------------------------------
# Transform the map entries and update signal names dynamically
map_expr = "map_from_entries(transform(map_entries(signals), x -> struct('" \
           + "' || CASE x.key " \
           + " ".join([f"WHEN '{key}' THEN '{value}'" for key, value in signal_mapping_dict.items()]) \
           + " ELSE x.key END, x.value)))"

# Apply the transformation to rename signal names
df_transformed = df_transformed.withColumn(
    "signals", expr(map_expr)
)

# --------------------------------------
# 🎯 Select final columns
# --------------------------------------
final_df = df_transformed.select(
    "signal_date",
    "signal_ts",
    "create_date",
    "create_ts",
    "signals",
    "generation_indicator"
)



distinct_signal_ts_per_day.show()


# Show the results
avg_signals_per_hour.show(truncate=False)




# --------------------------------------
# 📥 Write DataFrame to Delta and Parquet
# --------------------------------------
delta_path = "data/delta_output/T1_delta"
final_df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(delta_path)

parquet_path = "data/final_df.parquet"
final_df.write.mode("overwrite").parquet(parquet_path)

print(f"✅ Data successfully written to Delta format at {delta_path} and Parquet at {parquet_path}")
print("✅ Distinct signal_ts count per day calculated and averages per hour computed.")
