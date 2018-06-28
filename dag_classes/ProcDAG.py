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
"""
from airflow import DAG


class ProcDAG(DAG):
    def __init__(
        self,
        *args,
        inputs=[],
        outputs=[],
        first_ops=[],
        last_ops=[],
        **kwargs
    ):
        super(ProcDAG, self).__init__(
            *args, **kwargs
        )
        # TODO: add load stuff
        # TODO: add extract stuff
        # TODO: add cleanup stuff

    def __enter__(self):
        # TODO: create extract operators (inputs)
        super(ProcDAG, self).__enter__(self)

    def __exit__(self):
        # TODO: wire extract operators to first transform operators
        # TODO: wire load operators to last transform operators
        super(ProcDAG, self).__exit__(self)
