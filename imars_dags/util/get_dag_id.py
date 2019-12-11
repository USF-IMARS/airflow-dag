import os

from imars_dags.util.DAGType import DAGType, dag_type_from_filepath


def get_dag_id(filepath=None, region=None, dag_type=None, dag_name=None):
    """
    returns dag id created according to rules discussed in issue #49

    requires filepath (use `__file__`) or dag_type & dag_name
    region is optional
    """
    assert (
        filepath is not None or
        (dag_type is not None and dag_name is not None)
    )

    if dag_type is None:
        dag_type = dag_type_from_filepath(filepath)
    else:
        assert dag_type in DAGType.all()

    if dag_name is None:
        dag_name = dag_name_from_filepath(filepath)

    dag_id = dag_type + '_' + dag_name

    if region is not None:
        dag_id += '_' + region

    return dag_id


def dag_name_from_filepath(dag_filepath):
    basename = os.path.basename(dag_filepath)
    return basename.replace('.py', '')
