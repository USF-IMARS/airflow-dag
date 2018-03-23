# =========================================================================
# wv2 unzip to final destination
# =========================================================================
from datetime import datetime,timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.sensors import SqlSensor

from imars_dags.util.globals import DEFAULT_ARGS
import imars_etl

default_args = DEFAULT_ARGS.copy()
default_args.update({
    'start_date': datetime(2018, 3, 5, 16, 0),
    'retries': 1
})

this_dag = DAG(
    dag_id="wv2_unzip",
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)

# === wait for a valid target to process
SQL_SELECTION="status = 3 AND product_type_id = 6"
SQL_STR="SELECT id FROM file WHERE " + SQL_SELECTION
check_for_to_loads = SqlSensor(
    task_id='check_for_to_loads',
    conn_id="imars_metadata",
    sql=SQL_STR,
    soft_fail=True,
    dag=this_dag
)

# TODO: should set imars_product_metadata.status to "processing" to prevent
#    duplicates?

# === Extract
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
    dag=this_dag
)
check_for_to_loads >> extract_file

# === Transform
unzip_wv2_ingest = BashOperator(
    task_id="unzip_wv2_ingest",
    dag = this_dag,
    bash_command="""
        unzip \
            {{ ti.xcom_pull(task_ids="extract_file", key="fname") }} \
            -d /tmp/airflow_output_{{ ts }}
    """
)
extract_file >> unzip_wv2_ingest

# === wv2 schedule zip file for deletion
update_input_file_meta_db = MySqlOperator(
    task_id="update_input_file_meta_db",
    sql=""" UPDATE file SET status="to_delete" WHERE filepath="{{ ti.xcom_pull(task_ids="extract_file", key="fname") }}" """,
    mysql_conn_id='imars_metadata',
    autocommit=False,  # TODO: True?
    parameters=None,
    dag=this_dag
)

# TODO: delete the /tmp/ file(s)
# tmp_cleanup = BashOperator(...)

# === load result(s)
LOAD_TEMPLATE="""
 find /tmp/airflow_output_{{ ts }} \
     -type f \
     -regextype sed \
     -regex "$FILE_REGEX" \
     | xargs -n1 /opt/imars-etl/imars-etl.py -v load --product_type_id 7 --json '$METADATA_JSON' -f
 """
#  alternative to xargs:
#  -exec imars-etl load --filepath {} --json '$METADATA_JSON' \;

# a dict of products we are loading from the output directory
#    FILE_REGEX : defines a regex to select all the product files
#       (and only those files) from the /tmp/ directory created above.
#    METADATA_JSON : defines additional metadata for the product
#           * status 3 indicates the file has just been loaded
#           * area 5 is "no area"
to_load={
# INSERT INTO product (short_name,full_name,satellite,sensor)
#   VALUES("att_wv2_m1bs","wv2 m 1b .att","worldview2","multispectral")
    "att_wv2_m1bs":{
        "METADATA_JSON" : '{"status":3, "area_id":5}',
        "FILE_REGEX": '.*/[0-3][0-9][A-Z]\{3\}[0-9]\{8\}-M1BS-[0-9_]*_P[0-9]\{3\}.ATT$'
    }
     # TODO: load the rest of the m1bs files like above
    # INSERT INTO product (short_name,full_name,satellite)
    #   VALUES("eph_wv2_m1bs","wv2 1b multispectral .eph","worldview2")
    # INSERT INTO product (short_name,full_name,satellite)
    #   VALUES("geo_wv2_m1bs","wv2 1b multispectral .geo","worldview2")
    # INSERT INTO product (short_name,full_name,satellite)
    #   VALUES("imd_wv2_m1bs","wv2 1b multispectral .imd","worldview2")
    # INSERT INTO product (short_name,full_name,satellite)
    #   VALUES("ntf_wv2_m1bs","wv2 1b multispectral .ntf","worldview2")
    # INSERT INTO product (short_name,full_name,satellite)
    #   VALUES("rpb_wv2_m1bs","wv2 1b multispectral .rpb","worldview2")
    # INSERT INTO product (short_name,full_name,satellite)
    #   VALUES("til_wv2_m1bs","wv2 1b multispectral .til","worldview2")
    # INSERT INTO product (short_name,full_name,satellite)
    #   VALUES("xml_wv2_m1bs","wv2 1b multispectral .xml","worldview2")
    # INSERT INTO product (short_name,full_name,satellite)
    #   VALUES("jpg_wv2_m1bs","wv2 1b multispectral .jpg","worldview2")
    # INSERT INTO product (short_name,full_name,satellite)
    #   VALUES("txt_wv2_m1bs","wv2 1b multispectral readme","worldview2")
    # # GIS FILES # load "$unzipped_path/GIS_FILES/"
    # INSERT INTO product (short_name,full_name,satellite)
    #   VALUES("shx_wv2_m1bs","wv2 1b multispectral .shx","worldview2")
    # INSERT INTO product (short_name,full_name,satellite)
    #   VALUES("shp_wv2_m1bs","wv2 1b multispectral .shp","worldview2")
    # INSERT INTO product (short_name,full_name,satellite)
    #   VALUES("prj_wv2_m1bs","wv2 1b multispectral .prj","worldview2")
    # INSERT INTO product (short_name,full_name,satellite)
    #   VALUES("dbf_wv2_m1bs","wv2 1b multispectral .dbf","worldview2")

    # TODO: load each pass' p1bs files
    # INSERT INTO product (short_name,full_name,satellite)
    #   VALUES("att_wv2_p1bs","wv2 1b panchromatic .att","worldview2")
    # INSERT INTO product (short_name,full_name,satellite)
    #   VALUES("eph_wv2_p1bs","wv2 1b panchromatic .eph","worldview2")
    # INSERT INTO product (short_name,full_name,satellite)
    #   VALUES("geo_wv2_p1bs","wv2 1b panchromatic .geo","worldview2")
    # INSERT INTO product (short_name,full_name,satellite)
    #   VALUES("imd_wv2_p1bs","wv2 1b panchromatic .imd","worldview2")
    # INSERT INTO product (short_name,full_name,satellite)
    #   VALUES("ntf_wv2_p1bs","wv2 1b panchromatic .ntf","worldview2")
    # INSERT INTO product (short_name,full_name,satellite)
    #   VALUES("rpb_wv2_p1bs","wv2 1b panchromatic .rpb","worldview2")
    # INSERT INTO product (short_name,full_name,satellite)
    #   VALUES("til_wv2_p1bs","wv2 1b panchromatic .til","worldview2")
    # INSERT INTO product (short_name,full_name,satellite)
    #   VALUES("xml_wv2_p1bs","wv2 1b panchromatic .xml","worldview2")
    # INSERT INTO product (short_name,full_name,satellite)
    #   VALUES("jpg_wv2_p1bs","wv2 1b panchromatic .jpg","worldview2")
    # INSERT INTO product (short_name,full_name,satellite)
    #   VALUES("txt_wv2_p1bs","wv2 1b panchromatic readme","worldview2")
    # # GIS_FILES
    # INSERT INTO product (short_name,full_name,satellite)
    #   VALUES("shx_wv2_p1bs","wv2 1b panchromatic .shx","worldview2")
    # INSERT INTO product (short_name,full_name,satellite)
    #   VALUES("shp_wv2_p1bs","wv2 1b panchromatic .shp","worldview2")
    # INSERT INTO product (short_name,full_name,satellite)
    #   VALUES("dbf_wv2_p1bs","wv2 1b panchromatic .dbf","worldview2")
}

# imars-etl.load each of the file products listed in to_load
for output_key in to_load:
    output_val = to_load[output_key]
    load_operator = BashOperator(
        task_id="load_" + output_key,
        dag = this_dag,
        bash_command=LOAD_TEMPLATE.replace(
            '$METADATA_JSON',
            output_val["METADATA_JSON"]
        ).replace(
            '$FILE_REGEX',
            output_val['FILE_REGEX']
        )
    )
    load_operator >> update_input_file_meta_db
    # load_operator >> tmp_cleanup
