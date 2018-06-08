from datetime import datetime, timedelta

from airflow import DAG

from imars_dags.dags.ingest.cmr import coverage_check
from imars_dags.util.globals import DEFAULT_ARGS
from imars_dags.util.get_dag_id import get_dag_id

CHECK_DELAY = timedelta(hours=3)
default_args = DEFAULT_ARGS.copy()
delay_ago = datetime.utcnow()-CHECK_DELAY
default_args.update({  # round to
    'start_date': delay_ago.replace(minute=0,second=0,microsecond=0),
})

class CMRCoverageCheckDAG(DAG):
    def __init__(self,
        region, region_short_name, region_id,
        product_id, product_short_name,
        cmr_search_kwargs,
        granule_len,
        **kwargs
    ):

        super(CMRCoverageCheckDAG, self).__init__(
            dag_id=get_dag_id(
                __file__,
                region=region_short_name,
                dag_name="{}_cmr_coverage_check".format(product_short_name)
            ),
            default_args=default_args,
            schedule_interval=granule_len,
            catchup=True,
            max_active_runs=1
            **kwargs
        )

        coverage_check.add_tasks(
            self,
            region=region,
            product_id=product_id,
            area_id=region_id,
            cmr_search_kwargs=cmr_search_kwargs,
            check_delay=CHECK_DELAY,
        )
