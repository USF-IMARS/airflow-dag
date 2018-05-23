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

# deps
from airflow.operators.bash_operator import BashOperator
from airflow import DAG

# this package
from imars_dags.util.etl_tools.extract import add_extract
from imars_dags.util.etl_tools.load import add_load
from imars_dags.util.etl_tools.tmp_file import tmp_filepath, tmp_filedir
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

# === EXTRACT INPUT FILES ===
# ===========================================================================
# $image is product_type 12 or 25?
# |WV02_20170616150232_0000000000000000_17Jun16150232-M1BS-057796433010_01_P002.ntf | 12 |
# |WV02_20170616150231_0000000000000000_17Jun16150231-P1BS-057796433010_01_P001.ntf | 25 |
ntf_basename = "input_image"
ntf_input_file = tmp_filepath(this_dag.dag_id, ntf_basename + '.ntf')
extract_ntf = add_extract(this_dag, "product_id=12", ntf_input_file)

# ===
# $met is type 15 or 28?
# | WV02_20170616150232_0000000000000000_17Jun16150232-M1BS-057796433010_01_P002.xml |15 |
# | WV02_20170616150231_0000000000000000_17Jun16150231-P1BS-057796433010_01_P001.xml |28 |
met_input_file = tmp_filepath(this_dag.dag_id, "input_met.xml")
extract_met = add_extract(this_dag, "product_id=15", met_input_file)
# ===========================================================================

# === DEFINE PROCESSING TRANSFORM OPERATORS ===
# ===========================================================================
# output_dir1=/work/m/mjm8/tmp/test/ortho/
ortho_dir, create_ortho_tmp_dir = tmp_filedir(this_dag, 'ortho')
pgc_ortho = BashOperator(
    dag=this_dag,
    task_id='pgc_ortho',
    bash_command="""
        python /work/m/mjm8/progs/pgc_ortho.py \
            -p 4326 \
            -c ns \
            -t UInt16 \
            -f GTiff \
            --no_pyramids \
            """ + ntf_input_file + " " + ortho_dir,
    # queue=QUEUE.WV2_PROC,
)
create_ortho_tmp_dir >> pgc_ortho
extract_ntf >> pgc_ortho

# the filepath that pgc_ortho should have written to
ortho_output_file = ortho_dir + ntf_basename + "_u16ns4326.tif"

# ## Run Matlab code
rrs_out, create_ouput_tmp_dir = tmp_filedir(this_dag, 'output')  # "/work/m/mjm8/tmp/test/output/"
class_out = rrs_out  # same as above  "/work/m/mjm8/tmp/test/output/"
wv2_proc_matlab = BashOperator(
    dag=this_dag,
    task_id='wv2_proc_matlab',
    bash_command="""
        ORTH_FILE=""" + ortho_output_file + """ &&
        MET=""" + met_input_file + """  &&
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
            '""" + rrs_out   + """',\
            '""" + class_out + """'\
        )"
    """,
    params={
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
create_ouput_tmp_dir >> wv2_proc_matlab
extract_met >> wv2_proc_matlab
pgc_ortho >> wv2_proc_matlab
# ===========================================================================

# === (UP)LOAD RESULTS ===
# ===========================================================================
# TODO: what goes here? rrs_out, class_out, ortho_dir ?
#       do we want to save all of these files or only some of them?
to_load = []

add_load(this_dag, to_load, [wv2_proc_matlab])
# ===========================================================================
