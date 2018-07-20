# =========================================================================
# this sets up a FileTriggerDAG that catches a whole bunch of products
# which do not launch DAGs and changes their status from `to_load` to `std`
# =========================================================================
# unused DAG import so airflow can find your dag
import airflow  # noqa F401

from imars_dags.dag_classes.file_triggers.FileTriggerDAG import FileTriggerDAG

"""
[imars_product_metadata]> select id,short_name from product ORDER BY id;
+----+------------------------+
| id | short_name             |
+----+------------------------+
|  7 | att_wv2_m1bs           |
|  8 | att_wv2_p1bs           |
|  9 | geo_wv2_m1bs           |
| 10 | imd_wv2_m1bs           |

| 12 | rpb_wv2_m1bs           |
| 13 | til_wv2_m1bs           |
| 14 | xml_wv2_m1bs           |
| 15 | jpg_wv2_m1bs           |
| 16 | txt_wv2_m1bs           |
| 17 | shx_wv2_m1bs           |
| 18 | shp_wv2_m1bs           |
| 19 | prj_wv2_m1bs           |
| 20 | dbf_wv2_m1bs           |
| 21 | eph_wv2_p1bs           |
| 22 | geo_wv2_p1bs           |
| 23 | imd_wv2_p1bs           |
| 24 | ntf_wv2_p1bs           |
| 25 | rpb_wv2_p1bs           |
| 26 | til_wv2_p1bs           |
| 27 | xml_wv2_p1bs           |
| 28 | jpg_wv2_p1bs           |
| 29 | txt_wv2_p1bs           |
| 30 | shx_wv2_p1bs           |
| 31 | shp_wv2_p1bs           |
| 32 | dbf_wv2_p1bs           |
| 33 | eph_wv2_m1bs           |
| 34 | zip_ntf_wv2_ftp_ingest |
+----+------------------------+
"""
this_dag = FileTriggerDAG(
    product_ids=[x for x in range(7,35) if x not in [11]],
    dags_to_trigger=[],
    dag_id="file_trigger_catchall_unused"
)
