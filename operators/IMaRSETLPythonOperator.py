from airflow.operators.python_operator import PythonOperator
from airflow.models import SkipMixin

from imars_dags.operators.IMaRSETLMixin import IMaRSETLMixin


class IMaRSETLPythonOperator(IMaRSETLMixin, SkipMixin, PythonOperator):

    def __init__(
        self,
        *args,

        dag=None,
        inputs={},  # aka extracts\
        outputs={},  # aka loads
        tmpdirs=[],

        op_kwargs={},
        **kwargs
    ):
        self.pre_init(inputs, outputs, tmpdirs, dag)

        # add tmp filepath macros so we can template w/ them
        # user_defined_macros.update(self.tmp_paths)
        op_kwargs.update(self.tmp_paths)

        super(IMaRSETLPythonOperator, self).__init__(
            *args,
            dag=dag,
            op_kwargs=op_kwargs,
            provide_context=True,
            **kwargs
        )
