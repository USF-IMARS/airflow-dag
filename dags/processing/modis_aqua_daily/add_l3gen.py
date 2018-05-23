from datetime import timedelta

from airflow.operators.sensors import TimeDeltaSensor, SqlSensor
from airflow.operators.bash_operator import BashOperator

from imars_dags.util.globals import QUEUE, SLEEP_ARGS
from imars_dags.util import satfilename

# TODO: replace usage of satfilename

def add_l3gen(dag, region_name, gpt_xml):
    with dag as dag:
        # =========================================================================
        # === delay to wait for day to end, so all passes that day are done.
        # =========================================================================
        wait_for_day_end = TimeDeltaSensor(
            delta=timedelta(hours=18),  # 12 hrs to midnight + 6 hrs just in case
            task_id='wait_for_day_end',
            **SLEEP_ARGS
        )
        # =========================================================================
        # =========================================================================
        # === L3 Generation using GPT graph
        # =========================================================================
        # this assumes the l2 files for the whole day have already been generated
        #
        # example cmd:
        #     /opt/snap/bin/gpt L3G_MODA_GOM_vIMARS.xml
        #     -t /home1/scratch/epa/satellite/modis/GOM/L3G_OC/A2017313_map.nc
        #     -f NetCDF-BEAM
        #     /srv/imars-objects/modis_aqua_gom/l2/A2017313174500.L2
        #     /srv/imars-objects/modis_aqua_gom/l2/A2017313192000.L2
        #     /srv/imars-objects/modis_aqua_gom/l2/A2017313192500.L2
        #
        #     -t is the target (output) file, -f is the format

        def get_list_todays_l2s_cmd(exec_date, roi):
            """
            returns an ls command that lists all l2 files using the path & file fmt,
            but replaces hour/minute with wildcard *
            """
            satfilename.l2(exec_date, roi)
            fmt_str = satfilename.l2.filename_fmt.replace("%M", "*").replace("%H", "*")
            return "ls " + satfilename.l2.basepath(roi) + exec_date.strftime(fmt_str)

        l3gen = BashOperator(
            task_id="l3gen",
            bash_command="""
                /opt/snap/bin/gpt {{params.gpt_xml_file}} \
                -t {{ params.satfilename.l3(execution_date, params.roi_place_name) }} \
                -f NetCDF-BEAM \
                `{{ params.get_list_todays_l2s_cmd(execution_date, params.roi_place_name) }}`
            """,
            params={
                'satfilename': satfilename,
                'get_list_todays_l2s_cmd':get_list_todays_l2s_cmd,
                'roi_place_name': region_name,
                'gpt_xml_file': gpt_xml
            },
            queue=QUEUE.SNAP
        )
        # =========================================================================
        # =========================================================================
        # === wait for pass-level processing
        # =========================================================================
        # === wait for every granule to be checked for coverage
        # Passes when # of "success" controller dags for today >= 288
        # ie, when the dag has run for every 5min granule today.
        wait_for_all_day_granules_checked = SqlSensor(
            task_id='wait_for_all_day_granules_checked',
            conn_id='airflow_metadata',
            sql="""
            SELECT GREATEST(COUNT(state)-287, 0)
                FROM dag_run WHERE
                    (execution_date BETWEEN
                        '{{execution_date.replace(hour=0,minute=0)}}' AND '{{execution_date.replace(hour=23,minute=59)}}')
                    AND dag_id='"""+region_name+"""_modis_aqua_coverage_check'
                    AND state='success';
            """
        )
        wait_for_day_end >> wait_for_all_day_granules_checked

        # === wait for granules that were covered to finish processing.
        # Here we use an SqlSensor to check the metadata db instead of trying
        # to generate a dynamic list of ExternalTaskSensors.
        wait_for_pass_processing_success = SqlSensor(
            task_id='wait_for_pass_processing_success',
            conn_id='airflow_metadata',
            sql="""
                SELECT 1 - LEAST(COUNT(state),1)
                    FROM dag_run WHERE
                        (execution_date BETWEEN
                            '{{execution_date.replace(hour=0,minute=0)}}' AND '{{execution_date.replace(hour=23,minute=59)}}')
                        AND dag_id='"""+region_name+"""_modis_aqua_granule'
                        AND state!='success'
                ;
                """
        )
        wait_for_all_day_granules_checked >> wait_for_pass_processing_success >> l3gen
        # =========================================================================
