from pyspark.sql.functions import countDistinct, to_date

# Calculate distinct signal_ts datapoints per day
distinct_signal_ts_per_day = final_df.groupBy(to_date(col("signal_ts")).alias("signal_date")) \
    .agg(countDistinct("signal_ts").alias("distinct_signal_ts_count"))

# Show the results
distinct_signal_ts_per_day.show()
