"""
Tests some IMaRSETLPythonOperator features.
"""

from datetime import datetime

from airflow import DAG

from imars_dags.operators.IMaRSETLPythonOperator import IMaRSETLPythonOperator


this_dag = DAG(
    dag_id='zzz_test_etl_py_op',
    default_args=dict(
        start_date=datetime.utcnow()
    )
)
this_dag.doc_md = __doc__


def myfunc(**kwargs):
    print("calling the callable")

test_op = IMaRSETLPythonOperator(
    dag=this_dag,
    task_id='test_op',
    python_callable=myfunc,
)


test_op2 = IMaRSETLPythonOperator(
    dag=this_dag,
    task_id='test_op2',
    python_callable=myfunc,
    inputs={
        'myd01': 'product_id=5 AND date_time="2018-07-18 19:05:00"',
    },
    # outputs={
    #     'l2': {
    #         'area_id': 5
    #     }
    # },
    tmpdirs=[
        'mytempdir'
    ],
)


def myfunc2(*, infilename, mytmpdirname, **kwargs):
    print(
        "callable with context using op_kwargs to get:" +
        "\n\tinfilename:  {}" +
        "\n\tmpdirname: {}".format(infilename, mytmpdirname)
    )

test_op3 = IMaRSETLPythonOperator(
    dag=this_dag,
    task_id='test_op3',
    python_callable=myfunc2,
    inputs={
        'infilename': 'product_id=5 AND date_time="2018-07-18 19:05:00"',
    },
    outputs={
        # 'outfilename': {
        #     'area_id': 5
        # }
    },
    tmpdirs=[
        'mytmpdirname'
    ],
)

test_op >> test_op2 >> test_op3
