from airflow.operators.bash_operator import BashOperator

from imars_dags.util.globals import QUEUE
from imars_dags.util import satfilename

def get_l3gen(region):
    # =========================================================================
    # === L3 Generation using GPT graph
    # =========================================================================
    # this assumes the l2 files for the whole day have already been generated
    #
    # example cmd:
    #     /opt/snap/5.0.0/bin/gpt L3G_MODA_GOM_vIMARS.xml
    #     -t /home1/scratch/epa/satellite/modis/GOM/L3G_OC/A2017313_map.nc
    #     -f NetCDF-BEAM
    #     /srv/imars-objects/modis_aqua_gom/l2/A2017313174500.L2
    #     /srv/imars-objects/modis_aqua_gom/l2/A2017313192000.L2
    #     /srv/imars-objects/modis_aqua_gom/l2/A2017313192500.L2
    #
    #     -t is the target (output) file, -f is the format

    def get_list_todays_l2s_cmd(exec_date, region):
        """
        returns an ls command that lists all l2 files using the path & file fmt,
        but replaces hour/minute with wildcard *
        """
        satfilename.l2(exec_date, region)
        fmt_str = satfilename.l2.filename_fmt.replace("%M", "*").replace("%H", "*")
        return "ls " + satfilename.l2.basepath(region) + exec_date.strftime(fmt_str)

    return BashOperator(
        task_id="l3gen",
        bash_command="""
            /opt/snap/5.0.0/bin/gpt /root/airflow/dags/imars_dags/settings/regions/{{params.roi_place_name}}/moda_l3g.xml \
            -t {{ params.satfilename.l3(execution_date, params.roi_place_name) }} \
            -f NetCDF-BEAM \
            `{{ params.get_list_todays_l2s_cmd(execution_date, params.roi_place_name) }}`
        """,
        params={
            'satfilename': satfilename,
            'get_list_todays_l2s_cmd':get_list_todays_l2s_cmd,
            'roi_place_name': region['place_name']
        },
        queue=QUEUE.SNAP
    )
    # =========================================================================
