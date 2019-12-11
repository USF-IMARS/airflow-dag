#!/usr/bin/env python
"""
Reads a single CSV file into graphite.
Assumes "Time" is the first row and is a unix time in seconds.

Based on https://gist.github.com/agleyzer/8697616

example usage:
_csv_to_graphite.py /path/2/file.csv graphite.var.name.heir col,names,to,load

"""
import csv
import sys

try:  # w/in airflow
    from imars_dags.dags.csvs_to_graphite import GraphiteInterface
except ImportError:  # as script
    import GraphiteInterface


HOSTNAME = "graphitemaster"  # TODO
PORT = 2004  # TODO
OTHER_TIMEKEYS = ["Time"]


def main(csv_path, prefix, fields):
    print('loading {} to {} from file \n{}'.format(fields, prefix, csv_path))
    carbon = GraphiteInterface.GraphiteInterface(HOSTNAME, PORT)

    with open(csv_path, 'r') as csvfile:
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(csvfile.read(2048), delimiters=", ")
        csvfile.seek(0)
        has_head = sniffer.has_header(csvfile.read(2048))
        csvfile.seek(0)
        if(has_head):
            r = csv.DictReader(
                csvfile,
                delimiter=dialect.delimiter,
            )
        else:
            # assume time is first & other cols follow in order
            r = csv.DictReader(
                csvfile,
                delimiter=dialect.delimiter,
                fieldnames=['time'] + fields,
            )

        bytes_sent = 0
        for row in r:
            # check for other possible names of the time column
            if 'time' not in row:
                for alt_key in OTHER_TIMEKEYS:
                    if alt_key in row:
                        row['time'] = row[alt_key]
                        break
                else:
                    raise AssertionError("cannot find 'time' column")

            # skip comment rows
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
                bytes_sent += carbon.send_data()
                # print(".", end="")

        bytes_sent += carbon.send_data()  # send the last (partial) chunk
        print("done. {} bytes sent to graphite".format(bytes_sent))

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3])
