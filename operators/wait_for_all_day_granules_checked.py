from airflow.operators.sensors import SqlSensor

# === wait for every granule to be checked for coverage
# Passes when # of "success" controller dags for today >= 288
# ie, when the dag has run for every 5min granule today.
def get_wait_for_all_day_granules_checked():
    return SqlSensor(
    task_id='wait_for_all_day_granules_checked',
    conn_id='mysql_default',
    sql="""
    SELECT GREATEST(COUNT(state)-287, 0)
        FROM dag_run WHERE
            (execution_date BETWEEN
                '{{execution_date.replace(hour=0,minute=0)}}' AND '{{execution_date.replace(hour=23,minute=59)}}')
            AND dag_id='modis_aqua_passes_controller'
            AND state='success';
    """
)
