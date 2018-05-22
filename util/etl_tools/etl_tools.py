"""
allows for easy set up of ETL operations within imars-etl.
"""
import logging
import os, errno

from airflow.operators.bash_operator import BashOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.dummy_operator import DummyOperator

import imars_etl

from imars_dags.util.etl_tools.extract import add_extract
from imars_dags.util.etl_tools.tmp_file import tmp_format_str, get_tmp_file_suffix
from imars_dags.util.etl_tools.cleanup import add_cleanup
from imars_dags.util.etl_tools.load import add_load



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

    load_operators = add_load(dag, to_load, last_transform_operators)

    tmp_cleanup = add_cleanup(dag, to_cleanup, load_operators)
