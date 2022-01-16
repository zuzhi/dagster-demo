#!/usr/bin/env python3
import csv
from datetime import datetime

import requests
from dagster import get_dagster_logger, job, op, repository, schedule


@op
def hello_cereal(context):
    response = requests.get("https://docs.dagster.io/assets/cereal.csv")
    lines = response.text.split("\n")
    cereals = [row for row in csv.DictReader(lines)]
    date = context.op_config["date"]
    get_dagster_logger().info(
        f"Today is {date}. Found {len(cereals)} cereals."
    )


@job
def hello_cereal_job():
    hello_cereal()


@schedule(
    cron_schedule="45 6 * * *",
    job=hello_cereal_job,
    execution_timezone="Asia/Shanghai",
)
def good_morning_schedule(context):
    date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return {"ops": {"hello_cereal": {"config": {"date": date}}}}


def weekend_filter(_context):
    weekno = datetime.today().weekday()
    # Returns true if current day is a weekend
    return weekno > 5


@schedule(
    cron_schedule="36 16 * * *",
    job=hello_cereal_job,
    execution_timezone="Asia/Shanghai",
    should_execute=weekend_filter,
)
def good_weekend_morning_schedule(context):
    date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return {"ops": {"hello_cereal": {"config": {"date": date}}}}


@repository
def hello_cereal_repository():
    return [hello_cereal_job, good_morning_schedule, good_weekend_morning_schedule]
