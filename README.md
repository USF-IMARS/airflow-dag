# airflow-dag
:blowfish: USF IMaRS Airflow DAGs

## dev workflow
0. edit DAG
1. `airflow list_dags` to check for syntax errs
2. `airflow test $dag_name $task_name 2017-07-07T07:07:07` to test operators
3. test in the LocalExecutor test environment if possible
4. push to master & await production deployment (at next puppet run)

## organizational rules
0. A DAG (Directed Acyclic Graph) defines a processing pipeline.
1. Each DAG is in it's own python file.
1. The DAG file name matches the name string passed to the DAG constructor.
2. The main DAG variable in the file is named `this_dag`.
3. Operator `task_id` string matches the operator variable name.

## installation
installation should be handled by the imars_airflow puppet module,
but here are the general steps:
1. clone the repo
2. cp settings/secrets.py manually
3. install repo & dependencies w/ `pip install -e .` using setup.py

## NOTES:
include this from a pip requirements.txt like:

`-e git://github.com/USF-IMARS/imars_dags.git@master#egg=imars_airflow_config`
