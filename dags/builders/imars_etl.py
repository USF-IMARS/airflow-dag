"""
allows for easy set up of ETL operations within imars-etl.
"""
import logging
import os, errno

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import SqlSensor

import imars_etl

def get_tmp_dir(dag_id):
    """
    returns temporary directory (template) for given dag.
    """
    directory="/srv/imars-objects/airflow_tmp/"+dag_id+"_{{ts_nodash}}"
    # try:
    #     # oops, can't mkdir here b/c of jinja template
    #     os.mkdir(directory)  # not mkdirs b/c we want to fail if unmounted
    # except OSError as e:
    #     if e.errno != errno.EEXIST:
    #         raise
    #     # else already exists
    #     return directory
    return directory

def add_tasks(
    dag, sql_selector, first_transform_operators, last_transform_operators,
    to_load, TMP_DIR, common_load_params={}, test=False
):
    """
    Parameters:
    -----------
    dag : airflow.DAG
        DAG we are building upon
    sql_selector : str
        "WHERE ____" style SQL selector string to search metadata db for input.
        This is to find the input file of your DAG.
        Example:
        "product_id=6 AND is_day_pass=1"
    first_transform_operators : airflow.operators.*[]
        Operators which get wired after extract. These are the first in your
        processing chain.
    last_transform_operators : airflow.operators.*[]
        Operators which get wired before load. These are the last in your
        processing chain.
    to_load : str[]
        Product `short_name`s which get loaded into the imars-etl data warehouse
        after processing is done. These are the output files of your DAG.
    common_load_params : dict
        Dictionary to be passed into imars-etl.load() with metadata that is
        common to all of your to_load output files. Check imars-etl docs and/or
        imars_metadata_db.file columns to find a list of potential values.


    ---------
    local file name is loaded into the DAG context and can be accessed like:
        {{ ti.xcom_pull(task_ids="extract_file")}}
    """
    with dag as dag:
        # === /tmp/ create
        # ======================================================================
        tmp_mkdir = BashOperator(
            task_id="tmp_mkdir",
            trigger_rule="all_done",
            bash_command="mkdir " + TMP_DIR,
        )

        # === Extract
        # ======================================================================
        def extract_file(**kwargs):
            """
            Extracts a file from the remote system & makes it avaliable locally.
            `templates_dict` should be used to pass sql_selection in the context
            provided by kwargs.

            parameters:
            -----------
            kwargs : keyword-arguments
                kwargs contains the context set by airflow.
                Within this context we expect the following variables:

                `sql_selection` should look something like:
                    'date_time="{{ dt }} AND product_id={{ product_id }}"'

            returns:
            --------
            fname : str
                File path to acess the extracted file locally.
                Because this is returned the path can be accessed by other tasks
                using xcom like:
                `{{ ti.xcom_pull(task_ids="extract_file") }}`
            """
            sql_selection = kwargs['templates_dict']['sql_selection']
            if kwargs['templates_dict']['test'] == "True":
                return "/tmp/fake/file.name"
            else:
                fname = imars_etl.extract({
                    "sql":sql_selection
                })['filepath']
                print(       "extracting product matching SQL:\n\t" + sql_selection)
                logging.info("extracting product matching SQL:\n\t" + sql_selection)
                # ti.xcom_push(key='fname', value=fname)
                return fname

        extract_file = PythonOperator(
            task_id='extract_file',
            provide_context=True,
            python_callable=extract_file,
            templates_dict={
                "sql_selection": 'date_time="{{ execution_date }}" AND (' + sql_selector + ')',
                "test": str(test)
            }
        )
        tmp_mkdir >> extract_file

        # === /tmp/ cleanup
        # ======================================================================
        tmp_cleanup = BashOperator(
            task_id="tmp_cleanup",
            trigger_rule="all_done",
            bash_command="rm -r " + TMP_DIR,
        )
        tmp_mkdir >> tmp_cleanup  # just in case we have 0 proc ops

        # to ensure we clean up even if something in the middle fails, we must
        # do some weird stuff. For details see:
        # https://github.com/USF-IMARS/imars_dags/issues/44
        poke_until_tmp_cleanup_done = SqlSensor(
            # poke until the cleanup is done
            task_id='poke_until_tmp_cleanup_done',
            conn_id='airflow_metadata',
            poke_interval=60*2,
            soft_fail=False,
            timeout=60*30,
            sql="""
            SELECT * FROM task_instance WHERE
                task_id="tmp_cleanup"
                AND state IN ('success','failed')
                AND dag_id="{{ dag.dag_id }}"
                AND execution_date="{{ execution_date }}";
            """
        )
        tmp_mkdir >> poke_until_tmp_cleanup_done

        # === Load
        # ======================================================================
        LOAD_TEMPLATE="""
            python3 -m imars_etl -vvv load \
                --product_type_name {{ params.product_type_name }} \
                --json '{{ params.json }}' \
                --directory """ + TMP_DIR

        for t_op in first_transform_operators:
            # extract(s) >> transform(s)
            extract_file >> t_op

        for t_op in last_transform_operators:
            for product_short_name in to_load:
                # set params for this file specifically:
                common_load_params["product_type_name"]=product_short_name

                load_operator = BashOperator(
                    task_id="load_" + product_short_name,
                    bash_command=LOAD_TEMPLATE,
                    params=common_load_params
                )
                # transform(s) >> load(s)
                t_op >> load_operator
                # load(s) >> mysql_update
                # load_operator >> update_input_file_meta_db
                # load(s) >> cleanup
                load_operator >> tmp_cleanup

                # TODO: if load operator fails with IntegrityError (duplicate)
                #    mark success or skip or something...
