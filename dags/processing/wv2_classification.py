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
from airflow.operators.bash_operator import BashOperator
from airflow import DAG

# this package
from imars_dags.util.etl_tools.cleanup import add_cleanup
from imars_dags.util.etl_tools.extract import add_extract
from imars_dags.util.etl_tools.load import add_load
from imars_dags.util.etl_tools.tmp_file import tmp_filepath, tmp_filedir
from imars_dags.util.globals import DEFAULT_ARGS, QUEUE

DEF_ARGS = DEFAULT_ARGS.copy()
DEF_ARGS.update({
    'start_date': datetime.utcnow(),
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
# === ntf image is product_id # 11
# [imars_product_metadata]> SELECT id,short_name,full_name FROM product
#                           WHERE short_name="ntf_wv2_m1bs";
# | id | short_name   | full_name                 |
# +----+--------------+---------------------------+
# | 11 | ntf_wv2_m1bs | wv2 1b multispectral .ntf |
# basename has to have subsring that matches one of the regexes in
# [imagery_utils.lib.utils.get_sensor](https://github.com/PolarGeospatialCenter/imagery_utils/blob/v1.5.1/lib/utils.py#L57)
# (if not we get USF-IMARS/imars_dags#64) so here we match
# "(?P<ts>\d\d[a-z]{3}\d{8})-(?P<prod>\w{4})?(?P<tile>\w+)?-(?P<oid>\d{12}_\d\d)_(?P<pnum>p\d{3})"
# using a hard-coded sensor "wv02" + a fake date & catalog id
ntf_basename = "input_wv02_19890607101112_fake0catalog0id0"
ntf_input_file = tmp_filepath(this_dag.dag_id, ntf_basename + '.ntf')
extract_ntf = add_extract(this_dag, "product_id=11", ntf_input_file)

# === met xml is product_id # 14
# [imars_product_metadata]> SELECT id,short_name,full_name FROM product
#                           WHERE short_name="xml_wv2_m1bs";
# | id | short_name   | full_name                 |
# +----+--------------+---------------------------+
# | 14 | xml_wv2_m1bs | wv2 1b multispectral .xml |
met_input_file = tmp_filepath(this_dag.dag_id, ntf_basename + ".xml")
extract_met = add_extract(this_dag, "product_id=14", met_input_file)
# ===========================================================================

# === DEFINE PROCESSING TRANSFORM OPERATORS ===
# ===========================================================================
# output_dir1=/work/m/mjm8/tmp/test/ortho/
ortho_dir, create_ortho_tmp_dir = tmp_filedir(this_dag, 'ortho')
pgc_ortho = BashOperator(
    dag=this_dag,
    task_id='pgc_ortho',
    bash_command="""
        python /opt/imagery_utils/pgc_ortho.py \
            -p 4326 \
            -c ns \
            -t UInt16 \
            -f GTiff \
            --no-pyramids \
            """ + ntf_input_file + " " + ortho_dir,
    queue=QUEUE.WV2_PROC,
)
create_ortho_tmp_dir >> pgc_ortho
extract_ntf >> pgc_ortho

# the filepath that pgc_ortho should have written to
ortho_output_file = tmp_filepath(
    this_dag.dag_id,
    ntf_basename + "_u16ns4326.tif"
)
# filepath is actually inside the ortho_dir though...
ortho_output_file = os.path.join(
    ortho_dir,
    os.path.basename(ortho_output_file)
)

# ## Run Matlab code
rrs_out, create_ouput_tmp_dir = tmp_filedir(this_dag, 'output')  # "/work/m/mjm8/tmp/test/output/"
class_out = rrs_out  # same as above  "/work/m/mjm8/tmp/test/output/"
wv2_proc_matlab = BashOperator(
    dag=this_dag,
    task_id='wv2_proc_matlab',
    bash_command="""
        ORTH_FILE=""" + ortho_output_file + """ &&
        MET=""" + met_input_file + """  &&
        /opt/matlab/R2018a/bin/matlab -nodisplay -nodesktop -r "\
            cd('/opt/wv2_processing');\
            wv2_processing(\
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
            );\
            exit\
        "
    """,
    params={
        "crd_sys": "EPSG:4326",
        "dt": "0",
        "sgw": "5",
        "filt": "0",
        "stat": "3",
        "loc": "testnew",
        "SLURM_ARRAY_TASK_ID" : 0  # TODO: need to rm this
    },
    queue=QUEUE.WV2_PROC,
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

# load_tasks = add_load(this_dag, to_load, [wv2_proc_matlab])
# ===========================================================================
# TODO: turn this back on after to_load is figured out
# cleanup_task = add_cleanup(
#     this_dag,
#     to_cleanup=[ntf_input_file, met_input_file, rrs_out, ortho_dir],
#     upstream_operators=load_tasks
# )
