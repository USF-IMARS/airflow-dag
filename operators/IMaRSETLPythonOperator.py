from os import makedirs

from airflow.operators.python_operator import PythonOperator
import imars_etl

from imars_dags.util.etl_tools.tmp_file import tmp_filepath
from imars_dags.util.etl_tools.tmp_file import tmp_filedir
from imars_dags.util.etl_tools.load import load_task
from imars_dags.util.etl_tools.cleanup import tmp_cleanup_task
from imars_dags.operators.IMaRSETLMixin import IMaRSETLMixin
from imars_dags.util._render import _render


class IMaRSETLPythonOperator(IMaRSETLMixin, PythonOperator):

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
