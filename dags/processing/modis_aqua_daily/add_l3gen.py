"""
!!! DO NOT USE !!!

OLD, deprecated code. Most of it has been ported into the l2_to_l3 DAG, but
some remnants remain here.

TODO: mv useful things somewhere else & rm this file.

Usage:
    add_l3gen(
        this_dag,
        region_name=AREA_SHORT_NAME,
        gpt_xml=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "L3G_MODA_GOM_v2.xml"
        )
    )
"""
from airflow.operators.sensors import SqlSensor


def add_l3gen(dag, region_name, gpt_xml):
    with dag as dag:
        # NOTE: l3 stuff was here...
        # =========================================================================
        # =========================================================================
        # === wait for pass-level processing
        # =========================================================================
        # === wait for every granule to be checked for coverage
        # Passes when # of "success" controller dags for today >= 288
        # ie, when the dag has run for every 5min granule today.
        wait_for_all_day_granules_checked = SqlSensor(
            task_id='wait_for_all_day_granules_checked',
            conn_id='airflow_metadata',
            sql="""
            SELECT GREATEST(COUNT(state)-287, 0)
                FROM dag_run WHERE
                    (execution_date BETWEEN
                        '{{execution_date.replace(hour=0,minute=0)}}' AND
                        '{{execution_date.replace(hour=23,minute=59)}}'
                    )
                    AND dag_id='"""+region_name+"""_modis_aqua_coverage_check'
                    AND state='success';
            """
        )
        # wait_for_day_end >> wait_for_all_day_granules_checked

        # === wait for granules that were covered to finish processing.
        # Here we use an SqlSensor to check the metadata db instead of trying
        # to generate a dynamic list of ExternalTaskSensors.
        wait_for_pass_processing_success = SqlSensor(
            task_id='wait_for_pass_processing_success',
            conn_id='airflow_metadata',
            sql="""
                SELECT 1 - LEAST(COUNT(state),1)
                    FROM dag_run WHERE
                        (execution_date BETWEEN
                            '{{execution_date.replace(hour=0,minute=0)}}' AND
                            '{{execution_date.replace(hour=23,minute=59)}}'
                        )
                        AND dag_id='"""+region_name+"""_modis_aqua_granule'
                        AND state!='success'
                ;
                """
        )
        wait_for_all_day_granules_checked >> wait_for_pass_processing_success
        # wait_for_pass_processing_success >> l3gen
        # =========================================================================
