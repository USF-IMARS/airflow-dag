"""
allows for easy set up of ETL operations within imars-etl.
"""
import logging
import os, errno

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.dummy_operator import DummyOperator

import imars_etl

from imars_dags.util.etl_tools.extract import add_extract
from imars_dags.util.etl_tools.tmp_file import tmp_format_str, get_tmp_file_suffix
from imars_dags.util.etl_tools.cleanup import add_cleanup



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

    extract_file = add_extract(dag, sql_selector, first_transform_operators, test)
    tmp_cleanup = add_cleanup(dag, to_cleanup)

    with dag as dag:
        # tmp_mkdir >> extract_file
        # === Load
        # ======================================================================
        # loop through each to_load and load it

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
