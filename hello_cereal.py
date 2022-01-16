#!/usr/bin/env python3
from dagster.core.definitions.output import Out
from dagster.core.definitions.input import In
import requests
import csv
import sqlite3
from copy import deepcopy
from dagster import job, op, resource, graph, get_dagster_logger, DagsterType,\
                    TypeCheck, Field, String, config_from_files, file_relative_path,\
                    AssetMaterialization, EventMetadata, Output
import sqlalchemy
import sqlalchemy.ext.declarative
import os


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
def is_list_of_dicts(_, value):
    return isinstance(value, list) and all(
        isinstance(element, dict) for element in value
    )


SimpleDataFrame = DagsterType(
    name="SimpleDataFrame",
    type_check_fn=is_list_of_dicts,
    description="A naive representation of a data frame, e.g., as returned by csv.DictReader.",
)


run_config = {
        "ops": {
            "download_csv": {
                "config": {"url": "https://docs.dagster.io/assets/cereal.csv"}
            }
        }
    }


def less_simple_data_frame_type_check(_, value):
    if not isinstance(value, list):
        return TypeCheck(
            success=False,
            description=f"LessSimpleDataFrame should be a list of dicts, got {type(value)}",
        )

    fields = [field for field in value[0].keys()]

    for i in range(len(value)):
        row = value[i]
        idx = i + 1
        if not isinstance(row, dict):
            return TypeCheck(
                success=False,
                description=(
                    f"LessSimpleDataFrame should be a list of dicts, got {type(row)} for row {idx}"
                ),
            )
        row_fields = [field for field in row.keys()]
        if fields != row_fields:
            return TypeCheck(
                success=False,
                description=(
                    f"Rows in LessSimpleDataFrame should have the same fields, got {row_fields} "
                    f"for row {idx}, expected {fields}"
                ),
            )

    return TypeCheck(
        success=True,
        description="LessSimpleDataFrame summary statistics",
        metadata={
            "n_rows": len(value),
            "n_cols": len(value[0].keys()) if len(value) > 0 else 0,
            "column_names": str(
                list(value[0].keys()) if len(value) > 0 else []
            ),
        },
    )


LessSimpleDataFrame = DagsterType(
    name="LessSimpleDataFrame",
    type_check_fn=less_simple_data_frame_type_check,
    description="A less simple data frame type check with metadata and custom type checks.",
)


@op(config_schema={"url": str}, out=Out(SimpleDataFrame))
def download_csv(context):
    response = requests.get(context.op_config["url"])
    lines = response.text.split("\n")
    return [row for row in csv.DictReader(lines)]


# bad download_csv because of ouput type check will fail
# will throw an error `DagsterTypeCheckDidNotPass`
# @op(out=Out(SimpleDataFrame))
@op(out=Out(LessSimpleDataFrame))
def bad_download_csv():
    response = requests.get("https://docs.dagster.io/assets/cereal.csv")
    lines = response.text.split("\n")
    get_dagster_logger().info(f"Read {len(lines)} lines")
    return ["not_a_dict"]


# @op(ins={"cereals": In(SimpleDataFrame)})
# @op(ins={"cereals": In(LessSimpleDataFrame)})
# def sort_by_calories(cereals):
#     sorted_cereals = sorted(
#         cereals, key=lambda cereal: int(cereal["calories"])
#     )
#
#     get_dagster_logger().info(
#         f'Most caloric cereal: {sorted_cereals[-1]["name"]}'
#     )


@op
def sort_by_calories(context, cereals):
    sorted_cereals = sorted(
        cereals, key=lambda cereal: int(cereal["calories"])
    )
    least_caloric = sorted_cereals[0]["name"]
    most_caloric = sorted_cereals[-1]["name"]

    logger = get_dagster_logger()
    logger.info(f"Least caloric cereal: {least_caloric}")
    logger.info(f"Most caloric cereal: {most_caloric}")

    fieldnames = list(sorted_cereals[0].keys())
    sorted_cereals_csv_path = os.path.abspath(
        f"output/calories_sorted_{context.run_id}.csv"
    )
    os.makedirs(os.path.dirname(sorted_cereals_csv_path), exist_ok=True)

    with open(sorted_cereals_csv_path, "w") as fd:
        writer = csv.DictWriter(fd, fieldnames)
        writer.writeheader()
        writer.writerows(sorted_cereals)

    yield AssetMaterialization(
        asset_key="sorted_cereals_csv",
        description="Cereals data frame sorted by caloric content",
        metadata={
            "sorted_cereals_csv_path": EventMetadata.path(
                sorted_cereals_csv_path
            )
        },
    )
    yield Output(None)


@op(required_resource_keys={"warehouse"})
def normalize_calories(context, cereals):
    columns_to_normalize = [
        "calories",
        "protein",
        "fat",
        "sodium",
        "fiber",
        "carbo",
        "sugars",
        "potass",
        "vitamins",
        "weight",
    ]
    quantities = [cereal["cups"] for cereal in cereals]
    reweights = [1.0 / float(quantity) for quantity in quantities]

    normalized_cereals = deepcopy(cereals)
    for idx in range(len(normalized_cereals)):
        cereal = normalized_cereals[idx]
        for column in columns_to_normalize:
            cereal[column] = float(cereal[column]) * reweights[idx]

    context.resources.warehouse.update_normalized_cereals(normalized_cereals)


class LocalSQLiteWarehouse:
    def __init__(self, conn_str):
        self._conn_str = conn_str

    # In practice, you'll probably want to write more generic, reusable logic on your resources
    # than this tutorial example
    def update_normalized_cereals(self, records):
        conn = sqlite3.connect(self._conn_str)
        curs = conn.cursor()
        try:
            curs.execute("DROP TABLE IF EXISTS normalized_cereals")
            curs.execute(
                """CREATE TABLE IF NOT EXISTS normalized_cereals
                (name text, mfr text, type text, calories real,
                 protein real, fat real, sodium real, fiber real,
                 carbo real, sugars real, potass real, vitamins real,
                 shelf real, weight real, cups real, rating real)"""
            )
            curs.executemany(
                """INSERT INTO normalized_cereals VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [tuple(record.values()) for record in records],
            )
        finally:
            curs.close()


@resource(config_schema={"conn_str": Field(String)})
def local_sqlite_warehouse_resource(context):
    return LocalSQLiteWarehouse(context.resource_config["conn_str"])


Base = sqlalchemy.ext.declarative.declarative_base()


class NormalizedCereal(Base):
    __tablename__ = "normalized_cereals"
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    name = sqlalchemy.Column(sqlalchemy.String)
    mfr = sqlalchemy.Column(sqlalchemy.String)
    type = sqlalchemy.Column(sqlalchemy.String)
    calories = sqlalchemy.Column(sqlalchemy.Float)
    protein = sqlalchemy.Column(sqlalchemy.Float)
    fat = sqlalchemy.Column(sqlalchemy.Float)
    sodium = sqlalchemy.Column(sqlalchemy.Float)
    fiber = sqlalchemy.Column(sqlalchemy.Float)
    carbo = sqlalchemy.Column(sqlalchemy.Float)
    sugars = sqlalchemy.Column(sqlalchemy.Float)
    potass = sqlalchemy.Column(sqlalchemy.Float)
    vitamins = sqlalchemy.Column(sqlalchemy.Float)
    shelf = sqlalchemy.Column(sqlalchemy.Float)
    weight = sqlalchemy.Column(sqlalchemy.Float)
    cups = sqlalchemy.Column(sqlalchemy.Float)
    rating = sqlalchemy.Column(sqlalchemy.Float)


class SqlAlchemyPostgresWarehouse:
    def __init__(self, conn_str):
        self._conn_str = conn_str
        self._engine = sqlalchemy.create_engine(self._conn_str)


    def update_normalized_cereals(self, records):
        Base.metadata.bind = self._engine
        Base.metadata.drop_all(self._engine)
        Base.metadata.create_all(self._engine)
        NormalizedCereal.__table__.insert().execute(records)


@resource(config_schema={"conn_str": Field(String)})
def sqlalchemy_postgres_warehouse_resource(context):
    return SqlAlchemyPostgresWarehouse(context.resource_config["conn_str"])


@graph
def calories():
    normalize_calories(download_csv())

# calories_test_job = calories.to_job(
#     resource_defs={"warehouse": local_sqlite_warehouse_resource},
# )

# calories_test_job = calories.to_job(
#     resource_defs={"warehouse": local_sqlite_warehouse_resource},
#     config={"ops": {"download_csv": {"config": {"url": "https://docs.dagster.io/assets/cereal.csv"}}},
#             "resources": {"warehouse": {"config": {"conn_str": ":memory:"}}}},
# )

# calories_dev_job = calories.to_job(
#     resource_defs={"warehouse": sqlalchemy_postgres_warehouse_resource},
#     config=config_from_files(
#         [file_relative_path(__file__, "presets_dev_warehouse.yaml")]
#     ),
# )


# @job
def hello_cereal_job():
    hello_cereal()


# @job
def serial():
    find_sugariest(download_cereals())

# @job
def diamond():
    cereals = download_cereals()
    display_results(
        most_calories=find_highest_calorie_cereal(cereals),
        most_protein=find_highest_protein_cereal(cereals),
    )


# @job
def configurable_job():
    sort_by_calories(download_csv())


# @job
def bad_configurable_job():
    sort_by_calories(bad_download_csv())


# @job(resource_defs={"warehouse": local_sqlite_warehouse_resource})
def resources_job():
    normalize_calories(download_csv())


@job
def materialization_job():
    sort_by_calories(download_csv())


run_config = {
        "ops": {
            "download_csv": {
                "config": {"url": "https://docs.dagster.io/assets/cereal.csv"}
            }
        },
        "resources": {"warehouse": {"config": {"conn_str": ":memory:"}}}
    }


if __name__ == "__main__":
    # result = hello_cereal_job.execute_in_process()
    # result = configurable_job.execute_in_process(run_config=run_config)
    # result = calories_test_job.execute_in_process(run_config=run_config)
    # result = calories_test_job.execute_in_process()
    result = materialization_job.execute_in_process()
