import os


def get_dag_id(region=None, dag_name=None):
    """
    returns dag id created according to rules discussed in issue #49

    requires filepath (use `__file__`) or dag_type & dag_name
    region is optional
    """

    dag_id = dag_name

    if region is not None:
        dag_id += '_' + region

    return dag_id


def dag_name_from_filepath(dag_filepath):
    basename = os.path.basename(dag_filepath)
    return basename.replace('.py', '')
