"""
A list of DAG "types" identified for organizational purposes as described
[here]( https://github.com/USF-IMARS/imars_dags/issues/49 )
"""
import os

class DAGType:
    # NOTE: these must match directories for dag_type_from_filepath() to work
    PROCESSING   = 'processing'
    INGEST       = 'ingest'
    FILE_TRIGGER = 'file_triggers'

    @staticmethod
    def all():
        return [
            DAGType.PROCESSING,
            DAGType.INGEST,
            DAGType.FILE_TRIGGER
        ]

def dag_type_from_filepath(dag_filepath):
    for d_type in DAGType.all():
        if os.path.join("",d_type,"") in dag_filepath:
            return d_type
    else:
        raise ValueError("could not determine dag_type from filepath")
