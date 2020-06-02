from airflow import DAG
from airflow.operators.bash_operator import BashOperator


def create_dag(
        source,
        script_location,
        dag_id,
        default_args):

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=None,
        catchup=False
    )

    with dag:
        start_task = BashOperator(
            task_id='{}_{}'.format(source, status),
            bash_command='echo Starting {} workflow'.format(status),
            dag=dag
        )

        run_task = BashOperator(
            task_id='get_{}_images'.format(source),
            bash_command='python {} --mode default'.format(script_location),
            dag=dag
        )

        start_task >> run_task

    return dag


def test_create_dag_creates_correct_dependencies():
    dag = create_dag(
        'test_source',
        'test_script_location',
        'test_dag_id'
    )
    start_id = 'test_source_starting'
    run_id = 'get_test_source_images'
    start_task = dag.get_task(start_id)
    assert start_task.upstream_task_ids == set()
    assert start_task.downstream_task_ids == set([run_id])
    run_task = dag.get_task(run_id)
    assert run_task.upstream_task_ids == set([start_id])
    assert run_task.downstream_task_ids == set([])
