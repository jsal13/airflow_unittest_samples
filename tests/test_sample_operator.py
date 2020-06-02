from operators.sample_operator import CapitalizeLetters
import pytest
pytest_plugins = ["helpers_namespace"]


def test_simple_http_operator(test_dag):

    capitalize_letters = CapitalizeLetters(
        task_id="capitalize",
        letters="hey everyone",
        dag=test_dag,
    )

    pytest.helpers.run_task(task=capitalize_letters, dag=test_dag)
