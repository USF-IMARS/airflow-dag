"""
classification on WorldView-2 images

# old circe stuff:
# #SBATCH --job-name ="wv2_classification_py"
# #SBATCH --ntasks=1
# #SBATCH --mem-per-cpu=20480
# #SBATCH --time=3:00:00
# #SBATCH --array=0-3
"""
# std libs
from datetime import datetime
import os

# deps
from airflow import DAG

# this package
from imars_dags.util.etl_tools.tmp_file import tmp_format_str
from imars_dags.util.get_default_args import get_default_args
from imars_dags.util.globals import QUEUE
from imars_dags.util.get_dag_id import get_dag_id
from imars_dags.operators.IMaRSETLBashOperator import IMaRSETLBashOperator


def get_dag(area_short_name, area_id):
    this_dag = DAG(
        dag_id=get_dag_id(
            __file__,
            region=area_short_name,
            dag_name="wv2_classification"
        ),
        default_args=get_default_args(
            start_date=datetime.utcnow()
        ),
        schedule_interval=None,
    )

    JSON = ('{' +
        '"status_id":3,' +  # noqa E128
        '"area_short_name":"' + area_short_name + '"' +
    '}')
    # product ids from metadata db
    Rrs_ID = 37
    rrs_ID = 38
    bth_ID = 39
    classf_ID = 40

    # algorithm inputs
    FILTER = 0
    ID = 0
    LOC = 'testnew'
    ID_NUM = 0
    STAT = 3
    DT = 0

    # figure out what filenames should be:
    # ===========================================================================
    if FILTER:
        classf_output = "output_dir/{}_{}_DT_filt_{}_{}_{}.tif".format(
            ID, LOC, ID_NUM, FILTER, STAT
        )
    else:
        classf_output = "output_dir/{}_{}_DT_nofilt_{}.tif".format(
            ID, LOC, ID_NUM
        )
    # expected output filepaths
    Rrs_output = "output_dir/{}_{}_Rrs.tif".format(ID, LOC)
    rrs_output = "output_dir/{}_{}_rrs.tif".format(ID, LOC)
    bth_output = "output_dir/{}_{}_Bathy.tif".format(ID, LOC)
    outputs_to_load = {
        Rrs_output: {  # Rrs always an output
            "load_format": os.path.join(
                tmp_format_str(),
                Rrs_output.split('/')[-1]
            ),
            "json": JSON,
            "sql": 'area_id={} AND product_id={}'.format(area_id, Rrs_ID)
        }
    }

    if DT == 0:
        # Rrs only
        pass
    elif DT in [1, 2]:
        # Rrs, rrs, bathymetry for DT in [1,2]
        outputs_to_load[rrs_output] = {
            "load_format": os.join(
                tmp_format_str(),
                rrs_output.split('/')[-1]
            ),
            "json": JSON,
            "sql": 'area_id={} AND product_id={}'.format(area_id, rrs_ID)
        }
        outputs_to_load[bth_output] = {
            "load_format": os.join(
                tmp_format_str(),
                bth_output.split('/')[-1]
            ),
            "json": JSON,
            "sql": 'area_id={} AND product_id={}'.format(area_id, bth_ID)
        }
    else:
        raise ValueError("DT must be 0,1,2")

    if DT == 1:
        # Rrs, rrs, bathymetry **and classification**
        outputs_to_load[classf_output] = {
            "load_format": os.join(
                tmp_format_str(),
                classf_output.split('/')[-1]
            ),
            "json": JSON,
            "sql": 'area_id={} AND product_id={}'.format(area_id, classf_ID)
        }

    # ===========================================================================

    # NOTE: basename *must* have subsring that matches one of the regexes in
    # [imagery_utils.lib.utils.get_sensor](https://github.com/PolarGeospatialCenter/imagery_utils/blob/v1.5.1/lib/utils.py#L57)
    # (if not we get USF-IMARS/imars_dags#64) so here we match
    # "(?P<ts>\d\d[a-z]{3}\d{8})-(?P<prod>\w{4})?(?P<tile>\w+)?-(?P<oid>\d{12}_\d\d)_(?P<pnum>p\d{3})"
    # using a hard-coded sensor "wv02" + a fake date & catalog id
    classification = IMaRSETLBashOperator(  # noqa F841
        dag=this_dag,
        task_id='classification',
        should_overwrite=True,  # TODO: switch to False once reproc done
        should_cleanup=True,  # switch to False if debugging
        bash_command='classification.sh',
        tmpdirs=[
            'input_dir',
            'ortho_dir',
            'output_dir'
        ],
        inputs={
            # === ntf image is product_id # 11
            'input_dir/wv02_19890607101112_fake0catalog0id0.ntf':
                "product_id=11 AND date_time='{{ts}}'",
            # === met xml is product_id # 14
            'input_dir/wv02_19890607101112_fake0catalog0id0.xml':
                "product_id=14 AND date_time='{{ts}}'",
        },
        outputs=outputs_to_load,
        params={
            "id": ID,
            "crd_sys": "EPSG:4326",
            "dt": DT,
            "sgw": "5",
            "filt": FILTER,
            "stat": STAT,
            "loc": LOC,
            "id_number": ID_NUM  # (prev SLURM_ARRAY_TASK_ID) TODO: rm this?
        },
        queue=QUEUE.WV2_PROC,
    )

    return this_dag

na_dag = get_dag('na', 5)
big_bend_dag = get_dag('big_bend', 6)
