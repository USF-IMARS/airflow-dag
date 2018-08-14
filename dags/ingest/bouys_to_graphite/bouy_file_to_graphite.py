"""
loads given bouy data file into graphite.

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
import sys
import csv
from datetime import datetime

try:  # w/in airflow
    from imars_dags.dags.bouy_file_to_graphite import GraphiteInterface
except ImportError:  # as script
    import GraphiteInterface

HOSTNAME = "graphitemaster"  # TODO
PORT = 2004  # TODO

PREFIX = "imars_bouys.fg2"
FIELDS = [
    'date',
    'time',
    'east',
    'north',
    'oxygen',
    'water_temp',
    'tx',
    'ty',
    'tc_temp',
    'tc_cond',
    'tc_salt',
    'lat',
    'lon'
]
graphite_fields = FIELDS[2:]

data_file = sys.argv[1]

carbon = GraphiteInterface.GraphiteInterface(HOSTNAME, PORT)

with open(data_file, 'r') as datafile:
    r = csv.DictReader(
        datafile,
        fieldnames=FIELDS,
        delimiter=" ",
        skipinitialspace=True
    )

    for row in r:
        if r.line_num < 5:
            continue  # skip first 5 lines
        else:
            TIME_FMT_STR = "%m/%d/%Y %H:%M:%S"
            ts = datetime.strptime(
                row['date'] + " " + row['time'], TIME_FMT_STR
            ).strftime("%s")

            for field in graphite_fields:
                print("{}.{}\t|\t{}\t|\t{}".format(
                    PREFIX, field,
                    float(row[field]),
                    int(round(float(ts)))
                ))
                carbon.add_data(
                    "{}.{}".format(PREFIX, field),
                    float(row[field]),
                    int(round(float(ts)))
                )

            if r.line_num % 10 == 0:
                carbon.send_data()
                print(r.line_num)
