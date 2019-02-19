"""
Downloads raw bouy data & ingests it into graphite.
Example bouy data file:
http://tabs.gerg.tamu.edu/~woody/newtabs/viewdata.php?buoy=tabs_fg2&filename=tabs_fg2.20180807.raw

Data file excerpt:  # noqa E501
```
   Date   |  Time  |   East |   North| Oxygen |WaterT| Tx| Ty|TC_Temp|TC_Cond|TC_Salt|Latitude|Longitude|
          |  (UTC) |  (cm/s)|  (cm/s)|  (uM)  | (C) |   |   |  (C) |(mS/cm)|       |  (N)  |   (W)  |
==========+========+========+========+========+======+===+===+=======+=======+=======+========+=========+
08/07/2018 00:00:00    17.43   11.07   210.7   30.96   0  -1   30.53   61.08   36.45  27.90068  93.62038
08/07/2018 00:30:00    21.73   10.40   215.1   30.82   0  -1   30.42   60.93   36.44  27.90070  93.62029
08/07/2018 01:00:00    22.67    9.38   216.2   30.79   0  -1   30.40   60.94   36.45  27.90079  93.62031
```
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from imars_dags.util.get_default_args import get_default_args


this_dag = DAG(
    dag_id="bouys_to_graphite_fgbnms",
    default_args=get_default_args(
        start_date=datetime(2014, 1, 1)
    ),
    schedule_interval="59 23 * * *",
    max_active_runs=3,  # low to ensure we don't hurt his server
)

# d/l file
# parse file & load to graphite
data_path = "tabs_fg2.{{ds_nodash}}.raw"
data_url = (
    "http://tabs.gerg.tamu.edu/~woody/newtabs/viewdata.php?buoy=tabs_fg2"
)
script_path = (
    "/home/airflow/dags/imars_dags/dags/ingest/bouys_to_graphite/" +
    "bouy_file_to_graphite.py"
)
load_data_to_graphite = BashOperator(
    task_id="load_data_to_graphite",
    bash_command=("""
        lynx -dump -minimal -nolist -nostatus -width=200 \
            '{{params.data_url}}&filename=tabs_fg2.{{ds_nodash}}.raw'\
            >> tabs_fg2.raw &&
        python2 {{params.script_path}} tabs_fg2.raw
    """),
    dag=this_dag,
    params={
        "data_url": data_url,
        "script_path": script_path
    }
)
