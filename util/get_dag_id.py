from imars_dags.util.DAGType import DAGType

def get_dag_id(dag_type, dag_name, region=None):
    """
    returns dag id created according to rules discussed in issue #49
    """
    assert dag_type in DAGType.all()
    dag_id = dag_type + '_' + dag_name

    if region is not None:
        dag_id += '_' + region

    return dag_id
