TMP_PREFIX="/srv/imars-objects/airflow_tmp/"
# TODO: if tmp_filepath was part of a class we could store tmp files on the
#       instance and automatically add them to the `to_cleanup` list
def tmp_filepath(dag_id, suffix, ts="{{ts_nodash}}", n=0):
    """
    returns temporary directory (template) for given dag.

    Parameters:
    -----------
    dag_id :
    suffix : str
        suffix to append to filepath, use this like your filename.
        examples: myFile.txt, fileToLoad, output_file.csv
    n  : int
        file number. useful if extracting multiple files
    ts : str
        timestring for the current file in the form of ts_nodash
        example: 20180505T1345  # for date 2018-05-05T13:45
    """
    return (
        TMP_PREFIX + dag_id
        + "_" + str(ts)
        + "_" + str(suffix)
        # + "_" + str(n)  # TODO ?
    )

def tmp_filedir(dag_id, suffix, ts=None, n=None):
    path = tmp_filepath(dag_id, suffix, ts, n) + "/"
    # TODO: add mkdir BashOperator here
    return path

def tmp_format_str():
    return tmp_filepath("{dag_id}", "{tag}", ts="%Y%m%dT%H%M%S").split('/')[-1]

def get_tmp_file_suffix(load_args):
    """
    gets suffix from load_args dict for appending to operator names.
    eg for files:
        file_args = {'filepath': tmp_filepath(this_dag.dag_id, 'mysuffix')}
        name = 'load_' + get_tmp_file_suffix(file_args)
        >>> name == 'load_mysuffix'

    eg for directory:
        dir_args = {
            'directory': tmp_filepath(this_dag.dag_id, 'dir_name'),
            'product_type_name': 'prod_name')
        }
        name = 'load_' + get_tmp_file_suffix(dir_args)
        >>> name == 'load_dir_name_prod_name'
    """
    try:
        return load_args['filepath'].split('_')[-1]  # suffix only
    except KeyError:
        return "{}_{}".format(
            load_args['directory'].split('_')[-1],  # suffix only
            load_args['product_type_name']
        )
