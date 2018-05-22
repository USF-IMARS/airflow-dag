"""
allows for easy set up of ETL operations within imars-etl.
"""
import logging
import os, errno
from datetime import timedelta
import shutil

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import SqlSensor

import imars_etl

from imars_dags.util.etl_tools.extract import add_extract

TMP_PREFIX="/srv/imars-objects/airflow_tmp/"
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
        TMP_PREFIX + dag_id
        + "_" + str(ts)
        + "_" + str(suffix)
    )

def tmp_format_str():
    return tmp_filepath("{dag_id}", "{tag}", ts="%Y%m%dT%H%M%S").split('/')[-1]

def get_tmp_file_suffix(load_args):
    """
    gets suffix from load_args dict for appending to operator names.
    eg for files:
        file_args = {'filepath': tmp_filepath(this_dag.dag_id, 'mysuffix')}
        name = 'load_' + get_tmp_file_suffix(file_args)
        >>> name == 'load_mysuffix'

    eg for directory:
        dir_args = {
            'directory': tmp_filepath(this_dag.dag_id, 'dir_name'),
            'product_type_name': 'prod_name')
        }
        name = 'load_' + get_tmp_file_suffix(dir_args)
        >>> name == 'load_dir_name_prod_name'
    """
    try:
        return load_args['filepath'].split('_')[-1]  # suffix only
    except KeyError:
        return "{}_{}".format(
            load_args['directory'].split('_')[-1],  # suffix only
            load_args['product_type_name']
        )



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

    extract_file = add_extract(dag, sql_selector, test)

    with dag as dag:
        # tmp_mkdir >> extract_file

        # === /tmp/ cleanup
        # ======================================================================
        # loop through `to_cleanup` and rm instead of this:
        if HAS_CLEANUP:

            def tmp_cleanup_task(**kwargs):
                to_cleanup = kwargs['to_cleanup']
                for cleanup_path in to_cleanup:
                    cleanup_path = kwargs['task'].render_template(
                        '',
                        cleanup_path,
                        kwargs
                    )
                    if (cleanup_path.startswith(TMP_PREFIX)) and len(cleanup_path.strip()) > len(TMP_PREFIX):
                        print('rm -rf {}'.format(cleanup_path))
                        # shutil.rmtree(cleanup_path)
                    else:
                        raise ValueError(
                            "\ncleanup paths must be in /tmp/ dir '{}'".format(TMP_PREFIX) +
                            "\n\t you attempted to 'rm -rf {}'".format(cleanup_path)
                        )

            tmp_cleanup = PythonOperator(
                task_id='tmp_cleanup',
                python_callable=tmp_cleanup_task,
                op_kwargs={'to_cleanup': to_cleanup},
                provide_context=True,
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

            # apply macros on all (template-enabled) args:
            ARGS_TEMPLATE_FIELDS = ['filepath', 'directory']
            task = kwargs['task']
            for key in load_args:
                if key in ARGS_TEMPLATE_FIELDS:
                    load_args[key] = task.render_template(
                        '',
                        load_args[key],
                        kwargs
                    )
                # else don't template the arg

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
                operator_suffix = get_tmp_file_suffix(load_args)

                load_operator = PythonOperator(
                    task_id='load_' + operator_suffix,
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
