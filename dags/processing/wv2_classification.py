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
input_dir, create_input_tmp_dir = tmp_filedir(this_dag, 'input')
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
ntf_basename = "wv02_19890607101112_fake0catalog0id0"
ntf_input_file = os.path.join(input_dir, ntf_basename + ".ntf")
extract_ntf = add_extract(this_dag, "product_id=11", ntf_input_file)

# === met xml is product_id # 14
# [imars_product_metadata]> SELECT id,short_name,full_name FROM product
#                           WHERE short_name="xml_wv2_m1bs";
# | id | short_name   | full_name                 |
# +----+--------------+---------------------------+
# | 14 | xml_wv2_m1bs | wv2 1b multispectral .xml |
met_input_file = os.path.join(input_dir, ntf_basename + ".xml")
extract_met = add_extract(this_dag, "product_id=14", met_input_file)
# ===========================================================================

# === DEFINE PROCESSING TRANSFORM OPERATORS ===
# ===========================================================================
# output_dir1=/work/m/mjm8/tmp/test/ortho/
ortho_dir, create_ortho_tmp_dir = tmp_filedir(this_dag, 'ortho')
ortho_basename=ntf_basename+"_u16ns4326"
ortho_output_file = os.path.join(ortho_dir,  ortho_basename+".tif")
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
            """ + input_dir + " " + ortho_dir + """ &&
            [[ -s """ + ortho_output_file + """ ]]""",
    queue=QUEUE.WV2_PROC,
)

# ## Run Matlab code
rrs_out, create_ouput_tmp_dir = tmp_filedir(this_dag, 'output')  # "/work/m/mjm8/tmp/test/output/"
class_out = rrs_out  # same as above  "/work/m/mjm8/tmp/test/output/"
ID = 0
LOC = "testnew"
DT = 0
ID_NUM = 0
FILTER = 0
STAT = 3

# expected output filepaths
Rrs_output = "{}/{}_{}_Rrs.tif".format(  rrs_out, ID, LOC)
rrs_output = "{}/{}_{}_rrs.tif".format(  rrs_out, ID, LOC)
bth_output = "{}/{}_{}_Bathy.tif".format(rrs_out, ID, LOC)
if FILTER :
    classf_output = "{}/{}_{}_DT_filt_{}_{}_{}.tif".format(
        class_out, ID, LOC, ID_NUM, FILTER,STAT
    )
else :
    classf_output = "{}/{}_{}_DT_nofilt_{}.tif".format(
        class_out, ID, LOC, ID_NUM
    )

wv2_proc_matlab = BashOperator(
    dag=this_dag,
    task_id='wv2_proc_matlab',
    # WV2_Processing(
    #   images,id,met,crd_sys,dt,sgwin,filt,stat,loc,idnumber,rrs_out,class_out
    # )
    bash_command="""
        ORTH_FILE=""" + ortho_output_file + """ &&
        MET=""" + met_input_file + """  &&
        /opt/matlab/R2018a/bin/matlab -nodisplay -nodesktop -r "\
            cd('/opt/wv2_processing');\
            wv2_processing(\
                '$ORTH_FILE',\
                '{{params.id}}',\
                '$MET',\
                '{{params.crd_sys}}',\
                '{{params.dt}}',\
                '{{params.sgw}}',\
                '{{params.filt}}',\
                '{{params.stat}}',\
                '{{params.loc}}',\
                '{{params.id_number}}',\
                '""" + rrs_out   + """',\
                '""" + class_out + """'\
            );\
            exit\
        " &&
        [[ -s """ + Rrs_output + """ ]]""",  # Rrs should always be output
    params={
        "id" : ID,
        "crd_sys": "EPSG:4326",
        "dt": DT,
        "sgw": "5",
        "filt": FILTER,
        "stat": STAT,
        "loc": LOC,
        "id_number" : ID_NUM  # (prev SLURM_ARRAY_TASK_ID) TODO: rm this?
    },
    queue=QUEUE.WV2_PROC,
)
# ===========================================================================

# === (UP)LOAD RESULTS ===
# ===========================================================================
to_load = [
    {  # Rrs is always an output
        "filepath":Rrs_output,
        "verbose":3,
        "product_id":37,
        # "time":"2016-02-12T16:25:18",
        # "datetime": datetime(2016,2,12,16,25,18),
        "json":'{"status_id":3,"area_id":5}'
    }
]

if DT==0:
    # Rrs only
    pass
elif DT in [1,2]:
    # Rrs, rrs, bathymetry for DT in [1,2]
    to_load += [
        {
            "filepath":rrs_output,
            "verbose":3,
            "product_id":38,
            # "time":"2016-02-12T16:25:18",
            # "datetime": datetime(2016,2,12,16,25,18),
            "json":'{"status_id":3,"area_id":5}'
        },{
            "filepath":bth_output,
            "verbose":3,
            "product_id":39,
            # "time":"2016-02-12T16:25:18",
            # "datetime": datetime(2016,2,12,16,25,18),
            "json":'{"status_id":3,"area_id":5}'
        }
    ]
else:
    raise ValueError("DT must be 0,1,2")


if DT==1:
    # Rrs, rrs, bathymetry **and classification**
    to_load.append({
        "filepath":classf_output,
        "verbose":3,
        "product_id":40,
        # "time":"2016-02-12T16:25:18",
        # "datetime": datetime(2016,2,12,16,25,18),
        "json":'{"status_id":3,"area_id":5}'
    })

load_tasks = add_load(this_dag, to_load, [wv2_proc_matlab])
# ===========================================================================
cleanup_task = add_cleanup(
    this_dag,
    # NOTE: class_out excluded below b/c same as rrs_out
    to_cleanup=[input_dir, ortho_dir, rrs_out],
    upstream_operators=load_tasks
)

create_input_tmp_dir >> extract_ntf
create_input_tmp_dir >> extract_met

create_ortho_tmp_dir >> pgc_ortho
extract_ntf          >> pgc_ortho

create_ouput_tmp_dir >> wv2_proc_matlab
extract_met          >> wv2_proc_matlab
pgc_ortho            >> wv2_proc_matlab

# implied by `upstream_operators=[wv2_proc_matlab]`:
# wv2_proc_matlab      >> load_tasks

# implied by `upstream_operators=load_tasks`:
# load_tasks             >> cleanup_task
