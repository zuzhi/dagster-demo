#!/usr/bin/env python3
from dagster import repository
from hello_world import hello_dagster
from hello_cereal import materialization_job


# start_repos_marker_0
@repository
def hello_cereal_repository():
    return [hello_dagster, materialization_job]


# end_repos_marker_0
