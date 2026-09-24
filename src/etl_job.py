# Databricks notebook source
"""Clickstream Kitchen ETL job: Extract, Transform, Load click events into Delta Lake."""
from pyspark.sql import DataFrame, SparkSession, functions as F


def extract(spark: SparkSession) -> DataFrame:
    """EXTRACT: grab the raw click tickets (sample data standing in for Kinesis)."""
    rows = [
        ("e1", "u1", "buy", "2026-09-24 10:00:00"),
        ("e1", "u1", "buy", "2026-09-24 10:00:00"),  # duplicate from a network retry
        ("e2", "u2", "buy", "2026-09-24 10:05:00"),
        ("e3", "u1", "signup", "2026-09-24 11:00:00"),
        ("e4", None, "buy", "2026-09-24 11:30:00"),  # junk row: missing user
    ]
    schema = "event_id string, user_id string, button string, event_time string"
    return spark.createDataFrame(rows, schema)


def clean_clicks(df: DataFrame) -> DataFrame:
    """TRANSFORM part 1: throw out junk rows and duplicate clicks."""
    return df.dropna(subset=["event_id", "user_id", "button"]).dropDuplicates(["event_id"])


def daily_click_counts(df: DataFrame) -> DataFrame:
    """TRANSFORM part 2: summarize clicks per button per day."""
    return (
        df.withColumn("event_date", F.to_date("event_time", "yyyy-MM-dd HH:mm:ss"))
        .groupBy("event_date", "button")
        .agg(
            F.count("*").alias("total_clicks"),
            F.countDistinct("user_id").alias("unique_users"),
        )
    )


def load(df: DataFrame, table_name: str) -> None:
    """LOAD: save the finished dish into the pantry as a Delta table."""
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)


TABLE_NAME = "workspace.default.daily_clicks"


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    raw = extract(spark)
    summary = daily_click_counts(clean_clicks(raw))
    load(summary, TABLE_NAME)
    summary.show()


if __name__ == "__main__":
    main()