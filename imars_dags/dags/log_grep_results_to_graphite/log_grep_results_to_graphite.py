"""
This DAG sends statistics about task runs to graphite for visualization in
grafana.
.json files in ./dag_configs/ are named with a glob expression to match the
relevant dags.
Inside the files is a json object with keys to match task ids.
Within each task object there is a list of strings to grep for in the logs and
keys used to name the result.
"""
# std libs
from datetime import datetime
from datetime import timedelta
import os

# deps
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow_log_grepper.sanitize_glob_string import sanitize_glob_string


# TODO: more action and less talk in this command:
CLEAN_OLDER_DAGS_CMD = """
echo rm N-{{params.n_to_keep}} from {{params.dag_logs_path}}
find {{params.dag_logs_path}} -mindepth 2 -maxdepth 3
#     | xargs 1 | \
#     python3 keep_last_n.py -n {{params.n_to_keep}} "
"""

THIS_DIR = os.path.dirname(os.path.realpath(__file__))

with DAG(
    dag_id="log_grep_results_to_graphite",
    default_args={
        "start_date": datetime(2001, 11, 28)
    },
    concurrency=5,
    schedule_interval=timedelta(hours=1),
    catchup=False,  # latest only
    max_active_runs=1,
) as dag:
    dag.doc_md = __doc__  # sets web GUI to use docstring at top of file
    DAG_LOGS_PATH = "/srv/imars-objects/airflow_tmp/logs"
    DAG_CONFIGS_PATH = (
        THIS_DIR + "/dag_configs"
    )
    # for each dag_config
    for dag_config_file in os.listdir(DAG_CONFIGS_PATH):
        dag_glob = dag_config_file.replace(".json", "")
        # clean_older_dags = BashOperator(
        #     task_id='clean_older_{}'.format(dag_log_dir),
        #     bash_command=CLEAN_OLDER_DAGS_CMD,
        #     params={
        #         "dag_logs_path": DAG_LOGS_PATH + "/" + dag_log_dir,
        #         "n_to_keep": 2  # TODO: get per-DAG setting
        #     }
        # )
        grep_dag_logs = BashOperator(
            task_id=sanitize_glob_string(dag_glob),
            bash_command="""
                airflow_log_grepper_to_graphite \
                    '{{params.dag_greps_file}}' \
                    {{params.dag_logs_path}}
            """,
            params={
                "dag_logs_path": DAG_LOGS_PATH,
                "dag_greps_file":
                    DAG_CONFIGS_PATH + "/" + dag_config_file,
            },
            task_concurrency=3,
        )
        # clean_older_dags >> grep_dag_logs
