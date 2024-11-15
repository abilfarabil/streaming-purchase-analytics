"""
Purchase Aggregator using memory sink
-----------------------------------
Process streaming purchase events using memory sink for simplicity.
"""

import pyspark
import os
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql.functions import (
    from_json,
    col,
    sum as _sum,
    from_unixtime,
    window
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    TimestampType
)

def create_spark_session():
    """Initialize Spark session with minimal configurations."""
    return (
        pyspark.sql.SparkSession.builder
        .appName("PurchaseAggregator")
        .master("local[*]")  # Menggunakan local mode
        .config("spark.sql.shuffle.partitions", 1)
        .config("spark.streaming.stopGracefullyOnShutdown", True)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

def get_schema():
    """Define schema for purchase events."""
    return StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("furniture", StringType(), True),
        StructField("color", StringType(), True),
        StructField("price", IntegerType(), True),
        StructField("ts", LongType(), True),
    ])

def main():
    """Main execution function."""
    # Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Load configuration
    dotenv_path = Path("/opt/app/.env")
    load_dotenv(dotenv_path=dotenv_path)
    kafka_host = os.getenv("KAFKA_HOST")
    kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

    try:
        # Create streaming DataFrame from Kafka
        stream_df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", f"{kafka_host}:9092")
            .option("subscribe", kafka_topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

        # Parse JSON and add timestamp
        parsed_df = (
            stream_df
            .selectExpr("CAST(value AS STRING) as json")
            .select(from_json(col("json"), get_schema()).alias("data"))
            .select("data.*")
            .withColumn(
                "event_time",
                from_unixtime(col("ts")).cast(TimestampType())
            )
        )

        # Create 10-minute window aggregation
        agg_df = (
            parsed_df
            .groupBy(window(col("event_time"), "10 minutes"))
            .agg(_sum("price").alias("total_purchase"))
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("total_purchase")
            )
        )

        # Write to console
        query = (
            agg_df
            .writeStream
            .format("console")
            .outputMode("complete")
            .option("truncate", False)
            .trigger(processingTime="10 seconds")
            .start()
        )

        # Wait for termination
        query.awaitTermination()

    except Exception as e:
        print(f"Error in streaming job: {str(e)}")
        raise
    finally:
        print("Stopping Spark session...")
        spark.stop()

if __name__ == "__main__":
    main()