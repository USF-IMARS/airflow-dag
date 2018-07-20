"""
Tests some IMaRSETLPythonOperator features.
"""

from datetime import datetime

from airflow import DAG

from imars_dags.operators.IMaRSETLBashOperator import IMaRSETLBashOperator


this_dag = DAG(
    dag_id='zzz_test_etl_bash_op',
    default_args=dict(
        start_date=datetime.utcnow()
    )
)

test_op = IMaRSETLBashOperator(
    dag=this_dag,
    task_id='test_op',
    bash_command="echo 'my test op'",
)


test_op2 = IMaRSETLBashOperator(
    dag=this_dag,
    task_id='test_op2',
    bash_command="echo 'dt={{dt}} | myd01={{myd01}} | tmpdir={{tmpdir}}'",
    inputs={
        'myd01': 'product_id=5 AND date_time="2018-07-18 19:05:00"',
    },
    # outputs={
    #     'l2': {
    #         'area_id': 5
    #     }
    # },
    tmpdirs=[
        'tmpdir'
    ],
)

test_op >> test_op2
