# script which outputs status overview of airflow worker logs searched for
# known error strings.
# good for use as a telegraf exec to monitor airflow via graphite/influxdb.

# Rather than using this with telegraf, a cronjob combined with the
# following format puts less strain on the db here (NOTE that the
# crontab period _must_ match the graphite retention schema (eg:
# `*.exec.per_ten_min.*` matches with `*/10 * * * * *`)):

import os
import re
import operator
import pprint
from glob import glob
import json
pp = pprint.PrettyPrinter(indent=4)

logdir = "/home/airflow/logs"


def matchwalk_re(regex, directory):
    '''Yield path/filenames matching some regular expression
    from https://stackoverflow.com/a/49681926/1483986
    '''
    sep = os.path.sep
    pattern = re.compile(regex)
    for p, _, f in os.walk(directory):
        for i in range(len(f)):
            if pattern.search(os.path.join(p, f[i])):
                yield '{0}{1}{2}'.format(p, sep, f[i])
            # else:
            #    print(p)


def get_logfiles(base_log_path, dag_glob, task_glob):
    """
    Returns iterator of files in airflow DAG log directory.
    Expects dir structure like:
    /logs/dag_id/task_id/{task_instance_dt}/{n}.log
    """
    full_glob = "{}/{}/{}/*/*.log".format(
        base_log_path, dag_glob, task_glob
    )
    print("getting logs for glob " + full_glob)
    for log_path in glob(full_glob):
        yield log_path


def get_greps_from_config_json(json_config_fpath):
    """
    json config file should be named with a filename like `${dag_glob}.json`
    and look like:

    {
        "task_glob_1": [
            {
                "match_key_1": "string to grep for #1",
                "match_key_2": "other string to grep for"
            }
        ],
        "task_glob_2": [{...}]
    }
    """
    # DAG glob comes from filename
    dag_glob = os.path.basename(json_config_fpath).replace(".json", "")
    with open(json_config_fpath) as json_file:
        greps_by_task_globs_dict = json.load(json_file)
        return dag_glob, greps_by_task_globs_dict


def get_grepped_log_counts(greps_json_file, base_log_path):
    """
    returns sorted dict of counts for all log classifications
    """
    dag_glob, greps_by_task_globs = get_greps_from_config_json(greps_json_file)

    counts = {}
    # iterate over each task
    print("{} tasks glob strings found".format(len(greps_by_task_globs)))

    for task_glob, greps in list(greps_by_task_globs.items()):
        print("\t{}".format(task_glob))
        # import pdb; pdb.set_trace()
        for key, strin in list(greps.items()):
            counts[key] = 0
        counts['success'] = 0

        print("{} grep strings for this task glob".format(len(greps)))
        # search this task's logfiles
        for files in get_logfiles(base_log_path, dag_glob, task_glob):
            print("grepping {} files".format(
                len(files)
            ))
            unmatched_files = []
            # grep the file for strings
            # print(files) #entry.name)
            matches = []
            fstr = open(files).read()
            # special case for successful run:
            if fstr.strip().endswith("Command exited with return code 0"):
                counts['success'] += 1
                matches.append('success')
            for grep_key, grep_str in list(greps.items()):
                if grep_str in open(files).read():
                    counts[grep_key] += 1
                    matches.append(grep_key)
                    # print(grep_key)
            if len(matches) == 1:
                pass
            elif len(matches) > 1:
                # print('ERR: multiple matches!:{}'.format(matches))
                # print(files)
                for key in matches:
                    counts[key] -= 1
                multimatch_key = '_AND_'.join(matches)
                counts[multimatch_key] = counts.get(multimatch_key, 0) + 1
            else:  # matches < 1:
                print('UNMATCHED:')
                unmatched_files.append(files)  # entry.name)
                print(files)

        sorted_counts = sorted(counts.items(), key=operator.itemgetter(1))
        pp.pprint(sorted_counts)
        return sorted_counts
