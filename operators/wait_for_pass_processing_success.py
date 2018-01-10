from airflow.operators.sensors import SqlSensor

def get_wait_for_pass_processing_success(region):
    # === wait for granules that were covered to finish processing.
    # Here we use an SqlSensor to check the metadata db instead of trying
    # to generate a dynamic list of ExternalTaskSensors.
    return SqlSensor(
        task_id='wait_for_pass_processing_success',
        conn_id='mysql_default',
        sql="""
            SELECT 1 - LEAST(COUNT(state),1)
                FROM dag_run WHERE
                    (execution_date BETWEEN
                        '{{execution_date.replace(hour=0,minute=0)}}' AND '{{execution_date.replace(hour=23,minute=59)}}')
                    AND dag_id='modis_aqua_pass_processing_'"""+region['place_name']+"""
                    AND state!='success'
            ;
            """
    )
