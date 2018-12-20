#!/usr/bin/env python
"""
Reads a single CSV file into graphite.

Based on https://gist.github.com/agleyzer/8697616

example usage:
_csv_to_graphite.py /path/to/file.csv graphite.id.string mean,clim,anom
"""
import csv
import sys

try:  # w/in airflow
    from imars_dags.dags.csvs_to_graphite import GraphiteInterface
except ImportError:  # as script
    import GraphiteInterface


HOSTNAME = "graphitemaster"  # TODO
PORT = 2004  # TODO


def main(csv_path, prefix, fields):
    print('loading {} to {} from file \n{}'.format(fields, prefix, csv_path))
    carbon = GraphiteInterface.GraphiteInterface(HOSTNAME, PORT)

    with open(csv_path, 'r') as csvfile:
        # TODO: if csv.Sniffer.has_header() ?
        # TODO: update soon:
        #   fields read automatically from header, (rm fieldnames=)
        #   delim=','
        r = csv.DictReader(
            csvfile, fieldnames=['time'] + fields, delimiter=' '
        )

        for row in r:
            if (row['time'].startswith("#")):
                continue

            # TIME_FMT_STR = "%m/%d/%Y %I:%M:%S %p"  # for other time formats
            # ts = datetime.strptime(
            #     row['time'], TIME_FMT_STR
            # ).strftime("%s")
            ts = row['time']

            for field in fields:
                carbon.add_data(
                    "{}.{}".format(prefix, field),
                    float(row[field]),
                    int(round(float(ts)))
                )

            if r.line_num % 10 == 0:
                carbon.send_data()
                print("line #{}".format(r.line_num))


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3])
