from datetime import datetime, timedelta

from airflow import DAG

from imars_dags.dags.ingest.cmr import coverage_check
from imars_dags.regions import gom
from imars_dags.util.globals import DEFAULT_ARGS
from imars_dags.util.get_dag_id import get_dag_id

CHECK_DELAY = timedelta(days=30)
default_args = DEFAULT_ARGS.copy()
delay_ago = datetime.utcnow()-CHECK_DELAY
default_args.update({  # round to
    'start_date': delay_ago.replace(minute=0,second=0,microsecond=0),
})

this_dag = DAG(
    dag_id=get_dag_id(
        __file__,
        region='gom',
        dag_name="s3a_ol_1_efr_cmr_coverage_check"
    ),
    default_args=default_args,
    schedule_interval=coverage_check.schedule_interval,
    catchup=True,
    max_active_runs=1
)

coverage_check.add_tasks(
    this_dag,
    region=gom,
    product_id=36, # myd01 TODO: rm this
    area_id=1,  # gom TODO: rm this
    cmr_search_kwargs={
        'echo_collection_id': 'C1370679936-OB_DAAC',
        # 'day_night_flag': 'day',  # day only for ocean color ?
    },
    check_delay=CHECK_DELAY,
)
