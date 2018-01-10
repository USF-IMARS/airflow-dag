from airflow.operators.bash_operator import BashOperator

from imars_dags.settings.png_export_transforms import png_export_transforms
from imars_dags.util import satfilename
from imars_dags.util.globals import QUEUE

def get_l3_to_png(variable_name, region):
    try:
        var_transform = png_export_transforms[variable_name]
    except KeyError as k_err:
        # no transform found, passing data through w/o scaling
        # NOTE: not recommended. data is expected to be range [0,255]
        var_transform = "data"
    return BashOperator(
        task_id="l3_to_png_"+variable_name,
        bash_command="""
        /opt/sat-scripts/sat-scripts/netcdf4_to_png.py \
        {{params.satfilename.l3(execution_date, params.roi_place_name)}} \
        {{params.satfilename.png(execution_date, params.variable_name, params.roi_place_name)}} \
        {{params.variable_name}}\
        -t '{{params.transform}}'
        """,
        params={
            'satfilename': satfilename,
            'variable_name': variable_name,
            'transform': var_transform,
            'roi_place_name': region['place_name']
        },
        queue=QUEUE.SAT_SCRIPTS
    )
