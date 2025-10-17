# batch_analysis.py
# Usage example:
# spark-submit batch_analysis.py \
#   --input hdfs://localhost:9000/Sensors \
#   --out-summary hdfs://localhost:9000/Sensors_summary \
#   --out-outliers hdfs://localhost:9000/Sensors_outliers \
#   --z 3.0 \
#   --per-minute hdfs://localhost:9000/Sensors_minute

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, StringType

def main(a):
    spark = SparkSession.builder.appName("SensorBatchAnalysis").getOrCreate()

    df = spark.read.parquet(a.input)

    required = {"timestamp", "sensor_id", "value"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    df = (df
          .withColumn("value", F.col("value").cast(DoubleType()))
          .withColumn("timestamp_str", F.col("timestamp").cast(StringType()))
          .withColumn("event_ts",
                      F.to_timestamp("timestamp_str", "yyyy-MM-dd'T'HH:mm:ss"))
          )

    # per-sensor global stats
    w = Window.partitionBy("sensor_id")
    enriched = (df
        .withColumn("mean", F.mean("value").over(w))
        .withColumn("stddev", F.stddev_pop("value").over(w))
        .withColumn("zscore",
                    F.when(F.col("stddev") > 0,
                           (F.col("value") - F.col("mean")) / F.col("stddev"))
                     .otherwise(F.lit(0.0)))
        .withColumn("is_outlier", F.abs(F.col("zscore")) > F.lit(a.z))
    )

    summary = (enriched.groupBy("sensor_id")
        .agg(
            F.count("*").alias("count"),
            F.min("value").alias("min"),
            F.expr("percentile(value, 0.05)").alias("p05"),
            F.avg("value").alias("mean"),
            F.stddev_pop("value").alias("stddev"),
            F.expr("percentile(value, 0.95)").alias("p95"),
            F.max("value").alias("max"),
        )
        .orderBy("sensor_id")
    )

    # optional per-minute rollups
    if a.per_minute:
        minute = (df.where(F.col("event_ts").isNotNull())
            .groupBy("sensor_id", F.window("event_ts", "1 minute").alias("win"))
            .agg(
                F.count("*").alias("count"),
                F.avg("value").alias("mean"),
                F.stddev_pop("value").alias("stddev"),
                F.min("value").alias("min"),
                F.max("value").alias("max"),
            )
            .select(
                "sensor_id",
                F.col("win.start").alias("window_start"),
                F.col("win.end").alias("window_end"),
                "count","mean","stddev","min","max"
            )
            .orderBy("sensor_id","window_start")
        )
        minute.write.mode("overwrite").parquet(a.per_minute)

    summary.write.mode("overwrite").parquet(a.out_summary)

    outliers = (enriched
        .where("is_outlier = true")
        .select("timestamp", "event_ts", "sensor_id", "value", "mean", "stddev", "zscore")
    )
    outliers.write.mode("overwrite").parquet(a.out_outliers)

    print("\n=== Summary preview ===")
    summary.show(20, truncate=False)
    print("\n=== Outliers preview ===")
    outliers.show(20, truncate=False)

    spark.stop()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--out-summary", required=True)
    p.add_argument("--out-outliers", required=True)
    p.add_argument("--z", type=float, default=3.0)
    p.add_argument("--per-minute", default="")
    args = p.parse_args()
    main(args)
