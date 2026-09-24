# Clickstream Kitchen

I created a data engineering project that takes raw website click events, cleans and summarizes them with Apache Spark on Databricks, and stores the results as a Delta Lake table. Every push to `main` is automatically tested and deployed through a GitHub Actions CI/CD pipeline.

## How this works

The job in `src/etl_job.py` follows a classic ETL flow. The extract step loads a batch of raw click events. In production this data would stream in from a service like Amazon Kinesis; here it is sample data that intentionally includes a duplicate click from a network retry and a junk row with no user. The transform step removes those bad records and aggregates the clean events into daily totals per button, counting both total clicks and unique users. The load step writes the summary to `workspace.default.daily_clicks` as a Delta table, which gives the data ACID transactions, schema enforcement, and version history for time travel.

A simple way to picture it is a restaurant kitchen. Users clicking are customers placing orders, the stream is the ticket rail holding orders in line, Databricks is the chef turning raw ingredients into a finished dish, and the Delta table is the pantry with a logbook that only counts a dish once it has been officially signed in.

## CI/CD pipeline

The workflow in `.github/workflows/ci-cd.yml` runs in two stages. On every pull request and push to `main`, the CI stage installs PySpark and runs the pytest suite in `tests/`, which checks that duplicates and junk rows are removed and that the daily counts are correct. When a push to `main` passes CI, the CD stage uses the Databricks CLI and the bundle defined in `databricks.yml` to validate the configuration, deploy the job to the workspace, and run it. Databricks credentials are stored as encrypted GitHub repository secrets and are never committed to the code.

## Tech stack

Python, PySpark, Databricks serverless compute, Delta Lake, Databricks Declarative Automation Bundles, GitHub Actions, pytest, and Git.

## Running the tests locally

Requires Python 3.11 and Java 17.

    pip install -r requirements-dev.txt
    pytest -v

## Troubleshooting note

The first deployment passed validation and deployment but failed at run time, because serverless compute returned an I/O error when opening the job as a plain Python script. I traced the failure through the GitHub Actions logs, then converted the job to a Databricks notebook task, which fixed the run without changing any of the ETL logic or tests.

## Running this on AWS

This project runs on Databricks Free Edition, where compute is fully managed. In an AWS production setup, the same job would run on a Databricks cluster of EC2 instances that shuts down when idle, with the Delta table stored in an S3 bucket, so storage and compute scale and bill independently.