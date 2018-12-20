#!/usr/bin/env python
"""
Reads a single CSV file into graphite.

Based on https://gist.github.com/agleyzer/8697616

example usage:
_csv_to_graphite.py /path/to/file.csv graphite.id.string
"""
import csv
import sys

try:  # w/in airflow
    from imars_dags.dags.csvs_to_graphite import GraphiteInterface
except ImportError:  # as script
    import GraphiteInterface


HOSTNAME = "graphitemaster"  # TODO
PORT = 2004  # TODO

# all cols in the csv file
# 1. Time (UNIX)
# 2. Mean
# 3. Climatology
# 4. Anomaly
FIELDS = [
    "time", "mean", "climatology", "anomaly"
]

field_map = {title: num for (num, title) in enumerate(FIELDS)}

# cols we want to load into graphite
graphite_fields = FIELDS[1:]  # all but the first one (time)


def main(csv_path, prefix):
    carbon = GraphiteInterface.GraphiteInterface(HOSTNAME, PORT)

    with open(csv_path, 'r') as csvfile:
        r = csv.DictReader(csvfile, fieldnames=FIELDS, delimiter=' ')

        for row in r:
            if (row['time'].startswith("#")):
                continue

            # TIME_FMT_STR = "%m/%d/%Y %I:%M:%S %p"  # for other time formats
            # ts = datetime.strptime(
            #     row['time'], TIME_FMT_STR
            # ).strftime("%s")
            ts = row['time']

            for field in graphite_fields:
                carbon.add_data(
                    "{}.{}".format(prefix, field),
                    float(row[field]),
                    int(round(float(ts)))
                )

            if r.line_num % 10 == 0:
                carbon.send_data()
                print("line #{}".format(r.line_num))


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
