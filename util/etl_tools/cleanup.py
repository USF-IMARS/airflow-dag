"""
# === /tmp/ cleanup
# ======================================================================
"""
import shutil

from airflow.operators.python_operator import PythonOperator

from imars_dags.util.etl_tools.tmp_file import TMP_PREFIX


def tmp_cleanup_task(to_cleanup, task, **kwargs):
    for cleanup_path in to_cleanup:
        cleanup_path = task.render_template(
            '',
            cleanup_path,
            kwargs
        )
        if (
            cleanup_path.startswith(TMP_PREFIX) and
            len(cleanup_path.strip()) > len(TMP_PREFIX)
        ):
            print('rm -rf {}'.format(cleanup_path))
            shutil.rmtree(cleanup_path)
        else:
            raise ValueError(
                "\ncleanup paths must be in tmp dir '{}'".format(TMP_PREFIX) +
                "\n\t you attempted to 'rm -rf {}'".format(cleanup_path)
            )


def add_cleanup(dag, to_cleanup, upstream_operators):
    """
    upstream_operators : airflow.operators.*[]
        Operators which get wired before cleanup.
        These are the very last thing. Make sure everything is loaded first.
        Usually these are the operators doing the loading.
    """
    with dag as dag:
        tmp_cleanup = PythonOperator(
            task_id='tmp_cleanup',
            python_callable=tmp_cleanup_task,
            op_kwargs={'to_cleanup': to_cleanup},
            provide_context=True,
        )

        # to ensure we clean up even if something in the middle fails, we must
        # do some weird stuff. For details see:
        # https://github.com/USF-IMARS/imars_dags/issues/44
        # poke_until_tmp_cleanup_done = SqlSensor(
        #     # poke until the cleanup is done
        #     task_id='poke_until_tmp_cleanup_done',
        #     conn_id='airflow_metadata',
        #     soft_fail=False,
        #     poke_interval=60*10,           #check every ten minutes
        #     timeout=60*60,                 #for the first 60 minutes
        #     retries=0,                     #do~~n't~~ give up easily
        #     retry_delay=timedelta(hours=3),#~~but be patient between checks~~
        #     retry_exponential_backoff=False,
        #     sql="""
        #     SELECT * FROM task_instance WHERE
        #         task_id="tmp_cleanup"
        #         AND state IN ('success','failed')
        #         AND dag_id="{{ dag.dag_id }}"
        #         AND execution_date="{{ execution_date }}";
        #     """
        # )
        # start poking immediately
        # TODO: do we need another upstream task added here?
        # extract_file >> poke_until_tmp_cleanup_done

        # load(s) >> cleanup
        for up_operator in upstream_operators:
            up_operator >> tmp_cleanup

    return tmp_cleanup
