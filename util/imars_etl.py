"""
allows for easy set up of ETL operations within imars-etl.
"""
import logging
import os, errno
from datetime import timedelta

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import SqlSensor

import imars_etl

# TODO: if tmp_filepath was part of a class we could store tmp files on the
#       instance and automatically add them to the `to_cleanup` list
def tmp_filepath(dag_id, suffix, ts="{{ts_nodash}}"):
    """
    returns temporary directory (template) for given dag.

    Parameters:
    -----------
    dag_id :
    suffix : str
        suffix to append to filepath, use this like your filename.
        examples: myFile.txt, fileToLoad, output_file.csv
    ts : str
        timestring for the current file in the form of ts_nodash
        example: 20180505T1345  # for date 2018-05-05T13:45
    """
    return (
        "/srv/imars-objects/airflow_tmp/" + dag_id
        + "_" + str(ts)
        + "_" + str(suffix)
    )

def tmp_format_str():
    return tmp_filepath("{dag_id}", "{tag}", ts="%Y%m%dT%H%M%S").split('/')[-1]

def add_tasks(
    dag, sql_selector, first_transform_operators, last_transform_operators,
    files_to_load=None,
    products_to_load_from_dir=None,
    to_cleanup=[], common_load_params={}, test=False
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
    files_to_load : dict[]
        paths to be loaded into the imars-etl data warehouse
        after processing is done. These are the output files of your DAG.
        required dict elements: {
            'filepath': ...
        }
    products_to_load_from_dir : dict[]
        products to load from directories
        required dict elements: {
            'directory': ...
            'product_type_name': ...
        }
    to_cleanup : str[]
        List of files we will rm to cleanup after everything is done.
        NOTE: Loaded files should be cleaned up manually using this.
        Will not work on directories (for safety reasons).
    common_load_params : dict
        Dictionary to be passed into imars-etl.load() with metadata that is
        common to all of your to_load output files. Check imars-etl docs and/or
        imars_metadata_db.file columns to find a list of potential values.


    ---------
    local file name is loaded into the DAG context and can be accessed like:
        {{ ti.xcom_pull(task_ids="extract_file")}}
    """
    to_cleanup.append("{{ ti.xcom_pull(task_ids='extract_file') }}")
    # NOTE: ^ this cleans up the extracted file and means that HAS_CLEANUP is
    #       always True. TODO: refactor code below to account for this
    HAS_CLEANUP = len(to_cleanup) > 0
    with dag as dag:
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
                })
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
        # tmp_mkdir >> extract_file

        # === /tmp/ cleanup
        # ======================================================================
        # loop through `to_cleanup` and rm instead of this:
        if HAS_CLEANUP:
            tmp_cleanup = BashOperator(
                task_id="tmp_cleanup",
                trigger_rule="all_done",
                bash_command="rm " + " ".join(to_cleanup),
            )

            # to ensure we clean up even if something in the middle fails, we must
            # do some weird stuff. For details see:
            # https://github.com/USF-IMARS/imars_dags/issues/44
            poke_until_tmp_cleanup_done = SqlSensor(
                # poke until the cleanup is done
                task_id='poke_until_tmp_cleanup_done',
                conn_id='airflow_metadata',
                soft_fail=False,
                poke_interval=60*2,              # check every two minutes
                timeout=60*9,                    # for the first 9 minutes
                retries=10,                      # don't give up easily
                retry_delay=timedelta(hours=1),  # but be patient between checks
                retry_exponential_backoff=True,
                sql="""
                SELECT * FROM task_instance WHERE
                    task_id="tmp_cleanup"
                    AND state IN ('success','failed')
                    AND dag_id="{{ dag.dag_id }}"
                    AND execution_date="{{ execution_date }}";
                """
            )
            # start poking immediately
            extract_file >> poke_until_tmp_cleanup_done

        # === Load
        # ======================================================================
        # loop through each to_load and load it
        for t_op in first_transform_operators:
            # extract(s) >> transform(s)
            extract_file >> t_op

        def load_task(**kwargs):
            load_args = kwargs['load_args']
            # default args we add to all load ops:
            load_args['verbose'] = 3
            load_args['load_format'] = tmp_format_str()

            # apply macros on filepath arg:
            task = kwargs['task']
            load_args['filepath'] = task.render_template(
                '',
                load_args['filepath'],
                kwargs
            )

            print('loading {}'.format(load_args))
            imars_etl.load(load_args)

        if   products_to_load_from_dir is not None and files_to_load is not None:
            to_load = products_to_load_from_dir + files_to_load
        elif products_to_load_from_dir is not None:
            to_load = products_to_load_from_dir
        elif files_to_load is not None:
            to_load = files_to_load
        else:
            raise AssertionError(
                "products_to_load_from_dir or files_to_load required"
            )
        for t_op in last_transform_operators:
            for load_args in to_load:
                try:
                    fpath = load_args['filepath'].split('_')[-1]  # suffix only
                except KeyError:
                    fpath = "{}_{}".format(
                        load_args['directory'].split('_')[-1],  # suffix only
                        load_args['product_type_name']
                    )

                load_operator = PythonOperator(
                    task_id='load_' + fpath,
                    python_callable=load_task,
                    op_kwargs={'load_args': load_args},
                    provide_context=True,
                )
                # transform(s) >> load(s)
                t_op >> load_operator
                # load(s) >> cleanup
                if HAS_CLEANUP:
                    load_operator >> tmp_cleanup

                # TODO: if load operator fails with IntegrityError (duplicate)
                #    mark success or skip or something...
