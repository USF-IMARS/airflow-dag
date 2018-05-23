"""
A list of DAG "types" identified for organizational purposes as described
[here]( https://github.com/USF-IMARS/imars_dags/issues/49 )
"""
class DAGType:
    PROCESSING='proc'
    INGEST='ingest'
    FILE_TRIGGER='file_trigger'

    @staticmethod
    def all():
        return [DAGType.PROCESSING,DAGType.INGEST,DAGType.FILE_TRIGGER]
