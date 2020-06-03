import pytest
from operators.sample_operator import CapitalizeLetters


def test_capitalize_letters(test_dag):
    """Tests the capitalize_letters functions."""
    capitalize_letters = CapitalizeLetters(
        task_id="capitalize",
        letters="hey everyone",
        dag=test_dag,
    )

    pytest.helpers.run_task(task=capitalize_letters, dag=test_dag)
