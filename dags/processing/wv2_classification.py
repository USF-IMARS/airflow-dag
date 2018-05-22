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
from datetime import datetime, timedelta
import subprocess
import configparser
import os

# deps
from airflow.operators.bash_operator import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow import DAG

# this package
import imars_dags.util.imars_etl as imars_etl_builder
from imars_dags.util.globals import DEFAULT_ARGS

DEF_ARGS = DEFAULT_ARGS.copy()
DEF_ARGS.update({
    'start_date': datetime.utcnow(),
    'retries': 1
})

SCHEDULE_INTERVAL=None
AREA_SHORT_NAME="na"

this_dag = DAG(
    dag_id="proc_wv2_classification_"+AREA_SHORT_NAME,
    default_args=DEF_ARGS,
    schedule_interval=SCHEDULE_INTERVAL
)

# === INPUT FILES ===
# TODO: need to figure out how to extract two inputs...
# ===========================================================================
## images1=`ls $WORK/tmp/test/sunglint/*.[nN][tT][fF]`
## images1a=($images1)
## image=${images1a[$SLURM_ARRAY_TASK_ID]}
# $image is product_type 12 or 25?
# |WV02_20170616150232_0000000000000000_17Jun16150232-M1BS-057796433010_01_P002.ntf | 12 |
# |WV02_20170616150231_0000000000000000_17Jun16150231-P1BS-057796433010_01_P001.ntf | 25 |

# ===
## met=`ls $WORK/tmp/test/sunglint/*.[xX][mM][lL]`
# $met is type 15 or 28?
# | WV02_20170616150232_0000000000000000_17Jun16150232-M1BS-057796433010_01_P002.xml |15 |
# | WV02_20170616150231_0000000000000000_17Jun16150231-P1BS-057796433010_01_P001.xml |28 |
# ===========================================================================

# output_dir1=/work/m/mjm8/tmp/test/ortho/
ORTHO_DIR = imars_etl_builder.tmp_filepath(this_dag.dag_id, 'ortho') + "/"
pgc_ortho = BashOperator(
    dag=this_dag,
    task_id='pgc_ortho',
    bash_command="""
        INPUT_FILE={{ ti.xcom_pull(task_ids="extract_file") }} &&
        python /work/m/mjm8/progs/pgc_ortho.py \
            -p 4326 \
            -c ns \
            -t UInt16 \
            -f GTiff \
            --no_pyramids \
            $INPUT_FILE \
            """ + ORTHO_DIR,
    # queue=QUEUE.WV2_PROC,
)

# ## Run Matlab code

wv2_proc_matlab = BashOperator(
    dag=this_dag,
    task_id='wv2_proc_matlab',
    bash_command="""
        ORTH_FILE="""+ORTHO_DIR+"""{{ os.path.basename(ti.xcom_pull(task_ids="extract_file")) }}_u16ns4326.tif &&
        MET={{ ti.xcom_pull(task_ids="extract_file") }}  &&
        matlab -nodisplay -nodesktop -r "WV2_Processing(\
            '$ORTH_FILE',\
            '$MET',\
            '{{params.crd_sys}}',\
            '{{params.dt}}',\
            '{{params.sgw}}',\
            '{{params.filt}}',\
            '{{params.stat}}',\
            '{{params.loc}}',\
            '{{params.SLURM_ARRAY_TASK_ID}}',\
            '{{params.rrs_out}}',\
            '{{params.class_out}}'\
        )"
    """,
    params={
        "rrs_out": "/work/m/mjm8/tmp/test/output/",
        "class_out": "/work/m/mjm8/tmp/test/output/",
        "crd_sys": "EPSG:4326",
        "dt": "0",
        "sgw": "5",
        "filt": "0",
        "stat": "3",
        "loc": "'testnew'",
        "SLURM_ARRAY_TASK_ID" : 0  # TODO: need to rm this
    }
    # queue=QUEUE.MATLAB,
)
