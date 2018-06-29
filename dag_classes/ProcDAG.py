"""
"Processing" DAG - a DAG which performs ETL operations.

This class leverages imars-etl to hide complexity of the "extract" and "load"
operations and the cleanup of tmp files afterward.

**MUST** be used with "context manager" notation:
```python
with ProcDAG('my_dag') as dag:
    op = DummyOperator('op')

```
This is b/c we need to wire up load/extract operators *after* user has
defined the transform operators.

Example usage:
--------------
ProcDAG(
    inputs=['myd01','geofile']
    outputs=['myd02']
)
"""
from airflow import DAG

from imars_dags.util.etl_tools.tmp_file import tmp_filepath
from imars_dags.util.etl_tools.tmp_file import tmp_filedir
from imars_dags.util._render import _render


class ProcDAG(DAG):
    def __init__(
        self,

        dag_id,
        *args,

        inputs=[],
        outputs=[],
        tmpdirs=[],
        first_ops=[],
        last_ops=[],

        user_defined_macros={},
        user_defined_filters={},
        **kwargs
    ):
        """
        parameters:
        -----------
        inputs : str[]
            list of input filenames
        outputs : str[]
            list of outputs filenames
        tmpdirs : str[]
            list of temp directories to create
        first_ops : airflow.Operator[]
            list of operators which are first in processing chain.
            extraction of input files is wired before these.
        last_ops : airflow.Operator[]
            list of operators which are last in processing chain.
            loading & cleanup of output files is wired after these.
        """
        # get temp filepaths
        self.inputs = {}
        self.outputs = {}
        self.tmpdirs = {}
        self.mkdir_ops = []
        for inpf in inputs:
            self.inputs[inpf] = tmp_filepath(dag_id, inpf)
        for opf in outputs:
            self.outputs[opf] = tmp_filepath(dag_id, opf)
        for tdir in tmpdirs:
            self.tmpdirs[tdir], mkdir_op = tmp_filedir(self, tdir)
            self.mkdir_ops.append(mkdir_op)
        # add temp filepath macros so we can template w/ them
        user_defined_macros.update(self.inputs)
        user_defined_macros.update(self.outputs)

        # add the double-render filter
        user_defined_filters['render'] = _render

        super(ProcDAG, self).__init__(
            dag_id,
            *args,
            user_defined_macros=user_defined_macros,
            user_defined_filters=user_defined_filters,
            **kwargs
        )
        # TODO: add self.mkdir_ops
        # TODO: add extract ops for inputs
        # TODO: add load ops for outputs
        # TODO: add cleanup ops

    def __enter__(self):
        # nothing to do here?
        super(ProcDAG, self).__enter__(self)

    def __exit__(self):
        # TODO: wire extract operators to first transform operators
        # TODO: wire load operators to last transform operators
        super(ProcDAG, self).__exit__(self)
