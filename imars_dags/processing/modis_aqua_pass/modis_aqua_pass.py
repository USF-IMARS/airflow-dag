"""
processing for one modis aqua pass
"""
# std libs
from datetime import datetime
import os

# deps
from airflow import DAG

# this package
from imars_dags.util.get_dag_id import get_dag_id
from imars_dags.util.get_default_args import get_default_args
from imars_dags.operators.IMaRSETLBashOperator import IMaRSETLBashOperator
from imars_dags.util.globals import QUEUE

L1_PRODUCT_ID = 5
L2_PRODUCT_ID = 35
L3_PRODUCT_ID = 42
REGIONS = [
    ("gom", 1),
    ("fgbnms", 2)
]

for AREA_SHORT_NAME, AREA_ID in REGIONS:
    DAG_ID = get_dag_id(
        __file__, region=AREA_SHORT_NAME, dag_name="modis_aqua_pass"
    )
    this_dag = DAG(
        dag_id=DAG_ID,
        default_args=get_default_args(
            start_date=datetime.utcnow()
        ),
        schedule_interval=None,
    )
    this_dag.doc_md = __doc__

    l1_to_l2 = IMaRSETLBashOperator(
        task_id='l1_to_l2',
        bash_command="l1_to_l2.sh",
        should_overwrite=True,  # TODO: rm after reproc done
        inputs={
            "myd01_file":
                "product_id="+str(L1_PRODUCT_ID)+" AND date_time='{{ts}}'"
        },
        outputs={
            'l2_file': {
                "verbose": 3,  # TODO: rm?
                "product_id": L2_PRODUCT_ID,  # TODO: rm?
                "time": "{{ ts }}",  # ts.replace(" ", "T") ?  # TODO: rm?
                "sql": (
                    "product_id={} AND area_id={} ".format(
                            L2_PRODUCT_ID, AREA_ID
                    ) +
                    " AND date_time='{{ execution_date }}'"  # TODO: rm?
                ),
                "json": '{'  # noqa E131
                    '"status_id":3,'
                    '"area_short_name":"' + AREA_SHORT_NAME + '"'
                '}',
                'duplicates_ok': True,  # TODO: rm after reproc done
                'nohash': True,  # TODO: rm after reproc done
                # TODO: rm json &
                # "area_short_name": AREA_SHORT_NAME
            },
        },
        tmpdirs=["tmp_dir"],
        params={
            "par": os.path.join(
                os.path.dirname(os.path.realpath(__file__)),  # here
                "moda_l2gen.par"
            ),
        },
        queue=QUEUE.SAT_SCRIPTS,
        dag=this_dag,
    )

    l3gen = IMaRSETLBashOperator(
        task_id="l3gen",
        bash_command="""
            /opt/snap/bin/gpt {{ params.xml_file }} \
            -t {{ params.l3_output }} \
            -f NetCDF-BEAM \
            {{ params.l2_input }}
        """,
        should_overwrite=True,  # TODO: rm after reproc done
        params={
            "xml_file": os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                "L3G_MODA_GOM_v2.xml"
            )
        },
        inputs={
            "l2_input":
                "product_id="+str(L2_PRODUCT_ID)+" AND date_time='{{ts}}'"
        },
        outputs={
            'l3_output': {
                "verbose": 3,
                "product_id": L3_PRODUCT_ID,
                "time": "{{ ts }}",  # ts.replace(" ", "T") ?  # TODO: rm?
                "json": '{'
                    '"status_id":3,'  # noqa E131
                    '"area_short_name":"' + AREA_SHORT_NAME + '"'
                '}',
                "sql": (
                    "product_id={} AND area_id={} ".format(
                            L3_PRODUCT_ID, AREA_ID
                    ) +
                    " AND date_time='{{ execution_date }}'"  # TODO: rm?
                ),
                'duplicates_ok': True,  # TODO: rm after reproc done
                'nohash': True,  # TODO: rm after reproc done
            },
        },
        queue=QUEUE.SNAP,
        dag=this_dag,
    )

    l1_to_l2 >> l3gen

    # must add the dag to globals with unique name so airflow can find it
    globals()[DAG_ID] = this_dag
