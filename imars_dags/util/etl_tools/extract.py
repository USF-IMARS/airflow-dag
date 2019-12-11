import logging
import os

from airflow.operators.python_operator import PythonOperator

import imars_etl


def extract_file(templates_dict, **kwargs):
    """
    Extracts file from the remote system & makes it avaliable locally.
    `templates_dict` should be used to pass sql_selection in context
    provided by kwargs.

    parameters:
    -----------
    templates_dict : dict
        contains the context set by airflow.
        Within this context we expect the following variables:

        `sql_selection` should look something like:
            'date_time="{{ dt }} AND product_id={{ product_id }}"'

    returns:
    --------
    fname : str
        File path to acess the extracted file locally.
        This is returned so the path can be accessed by other tasks
        using xcom like:
        `{{ ti.xcom_pull(task_ids="extract_file") }}`
    """
    sql_selection = templates_dict['sql_selection']
    output_path = templates_dict['output_path']
    if templates_dict['test'] == "True":
        return "/tmp/fake/file.name"
    else:
        fname = imars_etl.extract(
            sql=sql_selection,
            output_path=output_path
        )
        msg = "extracting product matching SQL:\n\t" + sql_selection
        print(msg)
        logging.info(msg)
        # ti.xcom_push(key='fname', value=fname)
        return fname


def add_extract(
    dag, sql_selector, output_path, downstream_operators=[], test=False
):
    """
    sql_selector : str
        "WHERE ____" style SQL selector string to search metadata db for input.
        This is to find the input file of your DAG.
        Example:
        "product_id=6 AND is_day_pass=1"
    downstream_operators : airflow.operators.*[]
        Operators which get wired after extract. These are the first in your
        processing chain.
    """
    with dag as dag:
        # === Extract
        # ======================================================================

        blacklist = "{}/"
        sanitized_output_path = os.path.basename(output_path)
        sanitized_output_path = sanitized_output_path.replace(dag.dag_id, "")
        sanitized_output_path = sanitized_output_path.replace(
            "{{ts_nodash}}", ""
        )
        for char in blacklist:
            sanitized_output_path = sanitized_output_path.replace(char, "_")
        extract_file_op = PythonOperator(
            task_id='extract_' + sanitized_output_path,
            provide_context=True,
            python_callable=extract_file,
            templates_dict={
                "sql_selection":
                    'date_time="{{ execution_date }}" AND (' +
                    sql_selector + ')',
                "output_path": output_path,
                "test": str(test)
            }
        )

        for down_op in downstream_operators:
            # extract(s) >> transform(s)
            extract_file_op >> down_op

    return extract_file_op
