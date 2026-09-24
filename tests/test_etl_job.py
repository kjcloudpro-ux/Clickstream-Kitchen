"""Unit tests for the Clickstream Kitchen ETL transforms."""
import pytest
from pyspark.sql import SparkSession

from etl_job import clean_clicks, daily_click_counts, extract


@pytest.fixture(scope="session")
def spark():
    """Start a small local Spark session just for testing."""
    session = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()
    yield session
    session.stop()


def test_clean_clicks_removes_duplicates_and_junk(spark):
    raw = extract(spark)  # 5 messy rows
    cleaned = clean_clicks(raw)
    assert cleaned.count() == 3


def test_daily_click_counts_summarizes_by_button(spark):
    summary = daily_click_counts(clean_clicks(extract(spark)))
    results = {row.button: row for row in summary.collect()}

    assert results["buy"].total_clicks == 2
    assert results["buy"].unique_users == 2
    assert results["signup"].total_clicks == 1