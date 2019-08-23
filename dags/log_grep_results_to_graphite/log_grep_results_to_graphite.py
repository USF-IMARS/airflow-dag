import os
import sys
import subprocess
from . import log_grepper


def sanitize_glob_string(glob_str):
    """
    Replaces characters in a glob string which might offend airflow's DAG and
    task naming conventions or graphite's metric namespacing.

    I.e. : convert glob strings to underscores, letters, numbers while trying
           to preserve the meaning of the glob.

   This works best if the glob_str uses only lowercase letters, because the
   replacements use uppercase letters.
    """
    for offensive_char, replacement in [
        ['*', 'X'],
        ['[', 'I'],
        [']', 'I'],
        ['{', 'I'],
        ['}', 'I'],
        [',', 'I'],
        ['.', 'O'],
        ['-', 'T'],
        ['?', 'Q'],
        ['/', 'V'],
    ]:
        glob_str = glob_str.replace(offensive_char, replacement)
    return glob_str


if __name__ == "__main__":
    print("-"*100)
    HOSTNAME = subprocess.check_output("hostname -s", shell=True).strip()
    DATE = subprocess.check_output("date +%s", shell=True).strip()

    greps_json_file = sys.argv[1]
    dag_logs_dir = sys.argv[2]
    try:
        testing = sys.argv[3]
        assert testing.lower() in [
            'y', '1', 'yes', 'test', 'testing', 'true'
        ]
        test = True
    except IndexError:
        testing = False
    # DAG name for graphite comes from filename
    dag_dir_glob = os.path.basename(greps_json_file).replace(".json", "")

    for match_name, count in log_grepper.get_grepped_log_counts(
        greps_json_file, dag_logs_dir
    ):
        metric = "{host}.exec.per_ten_min.airflow.logs.{dag}.{match}".format(
            host=HOSTNAME,
            dag=sanitize_glob_string(dag_dir_glob),
            match=match_name
        )
        print("{} {} {}".format(metric, count, DATE))
        if not testing:
            result = subprocess.check_output(
                (
                    "timeout 3 echo {metric} {count} {dt} | " +
                    "nc {server} {port}"
                ).format(
                    metric=metric,
                    count=count,
                    dt=DATE,
                    server="graphite",
                    port=2003
                ),
                shell=True,
            )
