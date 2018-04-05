"""
allows for easy set up of ETL operations within imars-etl
"""
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.sensors import SqlSensor

import imars_etl

def add_tasks(
    dag, product_type_id, first_transform_operators, last_transform_operators,
    to_load, common_load_params={}
):
    """
    Parameters:
    -----------
    dag : airflow.DAG
        DAG we are building upon
    product_type_id : int
        Product id number from imars metadata db which we watch for as an input.
        This is the input file of your DAG.
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
    """
    with dag as dag:

        # TODO: remove this. (it is now in FileTriggerDAG)
        sql_selection="status={} AND product_type_id={}".format(
            3,  # STATUS.TO_LOAD
            product_type_id
        )
        sql_str="SELECT id FROM file WHERE " + sql_selection
        check_for_to_loads = SqlSensor(
            task_id='check_for_to_loads',
            conn_id="imars_metadata",
            sql=sql_str,
            soft_fail=True,
        )

        # === Extract
        # ============================================================================
        def extract_file(**kwargs):
            ti = kwargs['ti']
            fname = imars_etl.extract({
                "sql":SQL_SELECTION
            })['filepath']
            ti.xcom_push(key='fname', value=fname)
            return fname

        extract_file = PythonOperator(
            task_id='extract_file',
            provide_context=True,
            python_callable=extract_file,
        )

        # === Load
        # ============================================================================
        # === /tmp/ cleanup
        tmp_cleanup = BashOperator(
            task_id="tmp_cleanup",
            trigger_rule="all_done",
            bash_command="""
                rm -r /tmp/airflow_output_{{ ts }}
            """
        )
        # ensure we clean up even if something in the middle fails
        extract_file >> tmp_cleanup
        # NOTE: ^this^ doesn't work...
        #   possible fixes:
        #       1. Take a subdag param instead of operator lists & set subdag
        #           upstream from tmp_cleanup. The subdag will fail as a unit.
        #           * (-): but then all processing steps get lumped together
        #           * (-): this pushes extra complexity onto the implementing DAG
        #       2. pass on_retry_callback to every task
        # https://medium.com/handy-tech/airflow-tips-tricks-and-pitfalls-9ba53fba14eb
        #       3. set to `one_failed`, set every taks upstream, and add an
        #           always-fail operator (or duplicate the operator with an
        #           on_success)
        #           * (-): this will trigger cleanup before some are done
        #       4. duplicate task, one as-is and another with `one_failed` with
        #           every task upstream.

        # === mysql update
        update_input_file_meta_db = MySqlOperator(
            task_id="update_input_file_meta_db",
            sql=""" UPDATE file SET status=1 WHERE filepath="{{ ti.xcom_pull(task_ids="extract_file", key="fname") }}" """,
            mysql_conn_id='imars_metadata',
            autocommit=False,  # TODO: True?
            parameters=None,
        )

        # TODO: wire together
        # mysql_sensor >> extract(s) >> transform(s) >> load(s) >> cleanup
        #                                                 |----->> myql_update
        # mysql_sensor     >> extract(s)
        check_for_to_loads >> extract_file

        LOAD_TEMPLATE="""
            /opt/imars-etl/imars-etl.py -vvv load \
                --product_type_name {{ params.product_type_name }} \
                --json '{{ params.json }}' \
                --directory /tmp/airflow_output_{{ ts }}
        """

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
                load_operator >> update_input_file_meta_db
                # load(s) >> cleanup
                load_operator >> tmp_cleanup

                # TODO: if load operator fails with IntegrityError (duplicate)
                #    mark success or skip or something...
