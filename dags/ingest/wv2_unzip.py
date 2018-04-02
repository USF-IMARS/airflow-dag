# =========================================================================
# wv2 unzip to final destination
# =========================================================================
from datetime import datetime,timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from imars_dags.util.globals import DEFAULT_ARGS
import imars_dags.dags.builders.imars_etl as imars_etl_builder

default_args = DEFAULT_ARGS.copy()
default_args.update({
    'start_date': datetime(2018, 3, 5, 16, 0),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
})

this_dag = DAG(
    dag_id="wv2_unzip",
    default_args=default_args,
    schedule_interval=timedelta(hours=1),
    catchup=False,  # NOTE: this & max_active_runs prevents duplicate extractions
    max_active_runs=1
)


unzip_wv2_ingest = BashOperator(
    task_id="unzip_wv2_ingest",
    dag = this_dag,
    bash_command="""
        unzip \
            {{ ti.xcom_pull(task_ids="extract_file", key="fname") }} \
            -d /tmp/airflow_output_{{ ts }}
    """
)

# these GIS_FILES need to be removed so they don't get accidentally ingested
# later on.
rm_spurrious_gis_files = BashOperator(
    task_id="rm_spurrious_gis_files",
    dag=this_dag,
    bash_command="""
        rm -r /tmp/airflow_output_{{ ts }}/*/*/GIS_FILES
    """
)
unzip_wv2_ingest >> rm_spurrious_gis_files

# a list of params for products we are loading from the output directory
to_load=[
    # INSERT INTO product (short_name,full_name,satellite,sensor) VALUES("att_wv2_m1bs","wv2 m 1b .att","worldview2","multispectral")
    "att_wv2_m1bs",
    # INSERT INTO product (short_name,full_name,satellite) VALUES("eph_wv2_m1bs","wv2 1b multispectral .eph","worldview2")
    "eph_wv2_m1bs",
    # INSERT INTO product (short_name,full_name,satellite) VALUES("geo_wv2_m1bs","wv2 1b multispectral .geo","worldview2")
    "geo_wv2_m1bs",
    # INSERT INTO product (short_name,full_name,satellite) VALUES("imd_wv2_m1bs","wv2 1b multispectral .imd","worldview2")
    "imd_wv2_m1bs",
    # INSERT INTO product (short_name,full_name,satellite) VALUES("ntf_wv2_m1bs","wv2 1b multispectral .ntf","worldview2");
    "ntf_wv2_m1bs",
    # INSERT INTO product (short_name,full_name,satellite) VALUES("rpb_wv2_m1bs","wv2 1b multispectral .rpb","worldview2");
    "rpb_wv2_m1bs",
    # INSERT INTO product (short_name,full_name,satellite) VALUES("til_wv2_m1bs","wv2 1b multispectral .til","worldview2");
    "til_wv2_m1bs",
    # INSERT INTO product (short_name,full_name,satellite) VALUES("xml_wv2_m1bs","wv2 1b multispectral .xml","worldview2");
    "xml_wv2_m1bs",
    # INSERT INTO product (short_name,full_name,satellite) VALUES("jpg_wv2_m1bs","wv2 1b multispectral .jpg","worldview2");
    "jpg_wv2_m1bs",
    # INSERT INTO product (short_name,full_name,satellite) VALUES("txt_wv2_m1bs","wv2 1b multispectral readme","worldview2");
    "txt_wv2_m1bs",
    # # # GIS FILES # load "$unzipped_path/GIS_FILES/"
    # INSERT INTO product (short_name,full_name,satellite) VALUES("shx_wv2_m1bs","wv2 1b multispectral .shx","worldview2");
    "shx_wv2_m1bs",
    # INSERT INTO product (short_name,full_name,satellite) VALUES("shp_wv2_m1bs","wv2 1b multispectral .shp","worldview2");
    "shp_wv2_m1bs",
    # INSERT INTO product (short_name,full_name,satellite) VALUES("prj_wv2_m1bs","wv2 1b multispectral .prj","worldview2");
    "prj_wv2_m1bs",
    # INSERT INTO product (short_name,full_name,satellite) VALUES("dbf_wv2_m1bs","wv2 1b multispectral .dbf","worldview2");
    "dbf_wv2_m1bs",

    # INSERT INTO product (short_name,full_name,satellite) VALUES("att_wv2_p1bs","wv2 1b panchromatic .att","worldview2")
    "att_wv2_p1bs",
    # INSERT INTO product (short_name,full_name,satellite) VALUES("eph_wv2_p1bs","wv2 1b panchromatic .eph","worldview2");
    "eph_wv2_p1bs",
    # INSERT INTO product (short_name,full_name,satellite) VALUES("geo_wv2_p1bs","wv2 1b panchromatic .geo","worldview2");
    "geo_wv2_p1bs",
    # INSERT INTO product (short_name,full_name,satellite) VALUES("imd_wv2_p1bs","wv2 1b panchromatic .imd","worldview2");
    "imd_wv2_p1bs",
    # INSERT INTO product (short_name,full_name,satellite) VALUES("ntf_wv2_p1bs","wv2 1b panchromatic .ntf","worldview2");
    "ntf_wv2_p1bs",
    # INSERT INTO product (short_name,full_name,satellite) VALUES("rpb_wv2_p1bs","wv2 1b panchromatic .rpb","worldview2");
    "rpb_wv2_p1bs",
    # INSERT INTO product (short_name,full_name,satellite) VALUES("til_wv2_p1bs","wv2 1b panchromatic .til","worldview2");
    "til_wv2_p1bs",
    # INSERT INTO product (short_name,full_name,satellite) VALUES("xml_wv2_p1bs","wv2 1b panchromatic .xml","worldview2");
    "xml_wv2_p1bs",
    # INSERT INTO product (short_name,full_name,satellite) VALUES("jpg_wv2_p1bs","wv2 1b panchromatic .jpg","worldview2");
    "jpg_wv2_p1bs",
    # INSERT INTO product (short_name,full_name,satellite) VALUES("txt_wv2_p1bs","wv2 1b panchromatic readme","worldview2");
    "txt_wv2_p1bs",
    # # # GIS_FILES
    # INSERT INTO product (short_name,full_name,satellite) VALUES("shx_wv2_p1bs","wv2 1b panchromatic .shx","worldview2");
    "shx_wv2_p1bs",
    # INSERT INTO product (short_name,full_name,satellite) VALUES("shp_wv2_p1bs","wv2 1b panchromatic .shp","worldview2");
    "shp_wv2_p1bs",
    # INSERT INTO product (short_name,full_name,satellite) VALUES("dbf_wv2_p1bs","wv2 1b panchromatic .dbf","worldview2");
    "dbf_wv2_p1bs",
]

imars_etl_builder.add_tasks(
    this_dag, 6, [unzip_wv2_ingest], [rm_spurrious_gis_files],
    to_load, common_load_params={
        "json":'{"status":3, "area_id":5}'
    }
)
