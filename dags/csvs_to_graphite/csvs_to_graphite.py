"""
Reads each csv file and pushes the data into graphite.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from imars_dags.util.get_default_args import get_default_args


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
    [
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/OC/EastFG_OC_ts_FGB.csv',
        'imars_regions.east_fgbnms.chlor_a',
    ],
    [
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/OC/STET_OC_ts_FGB.csv',
        'imars_regions.stet_fgbnms.chlor_a',
    ],
    [
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/OC/WestFG_OC_ts_FGB.csv',
        'imars_regions.west_fgbnms.chlor_a',
    ],
    # === sst fgbnms
    [
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/SST/EastFG_SST_ts_FGB.csv',
        'imars_regions.east_fgbnms.sst',
    ],
    [
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/SST/STET_SST_ts_FGB.csv',
        'imars_regions.stet_fgbnms.sst',
    ],
    [
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/SST/WestFG_SST_ts_FGB.csv',
        'imars_regions.west_fgbnms.sst',
    ],
    # === rivers near fgbnms
    [
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/DISCH/BrazosRv_all.csv',
        'imars_regions.brazos.disch',
    ],
    [
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/DISCH/BrazosRv_all.csv',
        'imars_regions.brazos_river.disch',
    ],
    [
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/DISCH/MissRv_all.csv',
        'imars_regions.mississippi_river.disch',
    ],
    [
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/DISCH/NechesRv_all.csv',
        'imars_regions.neches_river.disch',
    ],
    [
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/DISCH/SabineRv_all.csv',
        'imars_regions.sabine_river.disch',
    ],
    [
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/DISCH/TrinityRv_all.csv',
        'imars_regions.trinity_river.disch',
    ],
]


# for args in CSV2GRAPH_ARGS:
#     PythonOperator(
#         task_id=(
#             "csv2graphite_" +
#             args[1].replace('.', '_').replace('imars_regions_', '')
#         ),
#         python_callable=csv2graph,
#         dag=this_dag,
#         op_args=args,
#         provide_context=True,
#     )
# same thing, but as bash op:
BashOperator(
    task_id=(
        "all_csvs2graphite"
    ),
    bash_command="""
        python2
        /home/airflow/dags/imars_dags/dags/csvs_to_graphite/_csvs_to_graphite.py
    """,
    dag=this_dag,
)
