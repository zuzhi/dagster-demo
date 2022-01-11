#!/usr/bin/env python3
import requests
import csv
from dagster import job, op, get_dagster_logger


@op
def hello_cereal():
    response = requests.get("https://docs.dagster.io/assets/cereal.csv")
    lines = response.text.split("\n")
    cereals = [row for row in csv.DictReader(lines)]
    get_dagster_logger().info(f"Found {len(cereals)} cereals")

    return cereals


@op
def download_cereals():
    response = requests.get("https://docs.dagster.io/assets/cereal.csv")
    lines = response.text.split("\n")
    return [row for row in csv.DictReader(lines)]


@op
def find_sugariest(cereals):
    sorted_by_sugar = sorted(cereals, key=lambda cereal: cereal["sugars"])
    get_dagster_logger().info(
        f'{sorted_by_sugar[-1]["name"]} is the sugariest cereal'
    )


@op
def find_highest_calorie_cereal(cereals):
    sorted_cereals = list(
        sorted(cereals, key=lambda cereal: cereal["calories"])
    )
    return sorted_cereals[-1]["name"]


@op
def find_highest_protein_cereal(cereals):
    sorted_cereals = list(
        sorted(cereals, key=lambda cereal: cereal["protein"])
    )
    return sorted_cereals[-1]["name"]

@op
def display_results(most_calories, most_protein):
    logger = get_dagster_logger()
    logger.info(f"Most caloric cereal: {most_calories}")
    logger.info(f"Most protein-rich cereal: {most_protein}")


# @job
def hello_cereal_job():
    hello_cereal()


# @job
def serial():
    find_sugariest(download_cereals())

@job
def diamond():
    cereals = download_cereals()
    display_results(
        most_calories=find_highest_calorie_cereal(cereals),
        most_protein=find_highest_protein_cereal(cereals),
    )


def test_find_highest_calorie_cereal():
    cereals = [
        {"name": "hi-cal cereal", "calories": 400},
        {"name": "lo-cal cereal", "calories": 50},
    ]
    result = find_highest_calorie_cereal(cereals)
    assert result == "hi-cal cereal"


def test_diamond():
    res = diamond.execute_in_process()
    assert res.success
    assert res.output_for_node("find_highest_protein_cereal") == "Special K"


if __name__ == "__main__":
    result = hello_cereal_job.execute_in_process()
