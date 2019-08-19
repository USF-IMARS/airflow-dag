"""
Create mosaic of all WorldView habitat classification images
"""
# std libs
from datetime import datetime
import os

# deps
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# this package
from imars_dags.util.get_default_args import get_default_args
from imars_dags.util.get_dag_id import get_dag_id
from imars_dags.util.Area import Area


DAG_NAME = os.path.splitext(os.path.basename(__file__))[0]

AREAS = [
    'big_bend',
    'fl_se',
    'fl_ne',
    'monroe',
    'panhandle',
    'west_fl_pen',
    'tx_coast',
]

for area_short_name in AREAS:
    area = Area(area_short_name)
    area_short_name = area.short_name
    area_id = area.id
    this_dag = DAG(
        dag_id=get_dag_id(
            __file__,
            region=area_short_name,
            dag_name=DAG_NAME
        ),
        default_args=get_default_args(
            start_date=datetime(2019, 8, 18)
        ),
        schedule_interval="@daily",
        catchup=False,  # latest only
        max_active_runs=1,
    )
    this_dag.doc_md = __doc__

    build_vrt = BashOperator(
        dag=this_dag,
        task_id='build_vrt',
        bash_command="""
            gdalbuildvrt \
                /srv/imars-objects/{{area_short_name}}/wv_classif.vrt \
                /srv/imars-objects/{{area_short_name}}/tif_classification/*
        """,
        params={
            "area_short_name": area_short_name,
        }
    )
    create_overview = BashOperator(
        dag=this_dag,
        task_id='create_overview',
        bash_command="""
            gdaladdo -ro -r average \
                /srv/imars-objects/{{area_short_name}}/wv_classif.vrt \
                1
        """,
        params={
            "area_short_name": area_short_name,
        }
    )
    # colorize = BashOperator(
    #     dag=this_dag,
    #     task_id='colorize',
    #     bash_command="""
    #       TODO: custom modify overview grayscale to RGB like
    #           https://gis.stackexchange.com/a/272805/107752
    #
    #       custom_colorize.py \
    #           /srv/imars-objects/{{area_short_name}}/wv_classif.vrt.ovr
    #     """,
    #     params={
    #         "area_short_name": area_short_name,
    #     }
    # )
    create_tiles = BashOperator(
        dag=this_dag,
        task_id='create_tiles',
        bash_command="""
            gdal2tiles.py -p raster \
                /srv/imars-objects/{{area_short_name}}/wv2_classif.vrt.ovr \
                /srv/imars-objects/{{area_short_name}}/wv2_classif_tiles
        """,
        params={
            "area_short_name": area_short_name,
        }
    )
    build_vrt >> create_overview
    create_overview >> create_tiles  # TODO: rm this line & + colorize btwn ie:
    # create_overview >> colorize
    # colorize >> create_tiles

    # === add the dag to globals with unique name so airflow can find it
    globals()[this_dag.dag_id] = this_dag
