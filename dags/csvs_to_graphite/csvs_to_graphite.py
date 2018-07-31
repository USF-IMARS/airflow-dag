"""
Reads each csv file and pushes the data into graphite.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from imars_dags.util.get_default_args import get_default_args

from imars_dags.dags.csvs_to_graphite._csv_to_graphite import main as csv2graph


this_dag = DAG(
    dag_id="csvs_to_graphite",
    default_args=get_default_args(
        start_date=datetime(2018, 7, 30)
    ),
    schedule_interval="@weekly",
    catchup=False,
    max_active_runs=1,
)


CSV2GRAPH_ARGS = [
    # === oc fgbnms
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/OC/EastFG_OC_ts_FGB.csv',
        'imars_regions.east_fgb.oc',
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/OC/STET_OC_ts_FGB.csv',
        'imars_regions.stet_fgb.oc',
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/OC/WestFG_OC_ts_FGB.csv',
        'imars_regions.west_fgb.oc',
    ),
    # === sst fgbnms
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/SST/EastFG_SST_ts_FGB.csv',
        'imars_regions.east_fgb.sst',
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/SST/STET_SST_ts_FGB.csv',
        'imars_regions.stet_fgb.sst',
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/SST/WestFG_SST_ts_FGB.csv',
        'imars_regions.west_fgb.sst',
    ),
    # === rivers near fgbnms
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/DISCH/BrazosRv_all.csv',
        'imars_regions.brazos.disch',
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/DISCH/BrazosRv_all.csv',
        'imars_regions.brazos_river.disch',
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/DISCH/MissRv_all.csv',
        'imars_regions.mississippi_river.disch',
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/DISCH/NechesRv_all.csv',
        'imars_regions.neches_river.disch',
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/DISCH/SabineRv_all.csv',
        'imars_regions.sabine_river.disch',
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/DISCH/TrinityRv_all.csv',
        'imars_regions.trinity_river.disch',
    ),
]


for args in CSV2GRAPH_ARGS:
    update_symlinks = PythonOperator(
        task_id=(
            "csv2graphite_" +
            args[1].replace('.', '_').replace('imars_regions_', '')
        ),
        python_callable=csv2graph,
        dag=this_dag,
        op_args=args,
    )
