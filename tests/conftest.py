import datetime
import pytest
from airflow import DAG

# What is a conftest?
# For more info, check out: https://docs.pytest.org/en/2.7.3/plugins.html?highlight=re


@pytest.helpers.register
def run_task(task, dag):
    dag.clear()
    task.run(
        start_date=dag.default_args["start_date"],
        end_date=dag.default_args["start_date"],
    )


@pytest.fixture
def test_dag():
    """Returns a test dag if the operator requires a dag to run in."""
    return DAG(
        "test_dag",
        default_args={"owner": "airflow",
                      "start_date": datetime.datetime.today()},
        schedule_interval=datetime.timedelta(days=1),
    )
