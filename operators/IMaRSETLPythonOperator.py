from os import makedirs

from airflow.operators.python_operator import PythonOperator
import imars_etl

from imars_dags.util.etl_tools.tmp_file import tmp_filepath
from imars_dags.util.etl_tools.tmp_file import tmp_filedir
from imars_dags.util.etl_tools.load import load_task
from imars_dags.util.etl_tools.cleanup import tmp_cleanup_task
from imars_dags.util._render import _render


class IMaRSETLPythonOperator(PythonOperator):
    """
    # =========================================================================
    # === "Transform" operator that "Extracts" & "Loads" auto-magically.
    # =========================================================================
    Leverages IMaRS ETL tools (https://github.com/usf-imars/imars-etl) to:
        * find & "Extract" input files by metadata
        * "Load" & cleanup output files & autofill (some) metadata
    """
    def __init__(
        self,
        *args,

        dag=None,
        inputs={},  # aka extracts
        outputs={},  # aka loads
        tmpdirs=[],

        op_kwargs={},
        **kwargs
    ):
        self.inputs = inputs
        self.outputs = outputs
        self.tmpdirs = tmpdirs

        # get tmp paths
        self.tmp_paths = {}
        self.tmp_dirs = []
        for inpf in inputs:
            self.tmp_paths[inpf] = tmp_filepath(dag.dag_id, inpf)
        for opf in outputs:
            self.tmp_paths[opf] = tmp_filepath(dag.dag_id, opf)
        for tdir in tmpdirs:
            tmpdir_path, mkdir_op = tmp_filedir(dag, tdir)
            self.tmp_paths[tdir] = tmpdir_path
            # mkdir_op unused

        # add tmp filepath macros so we can template w/ them
        # user_defined_macros.update(self.tmp_paths)
        op_kwargs.update(self.tmp_paths)

        # TODO:
        # add the double-render filter
        # user_defined_filters['render'] = _render

        super(IMaRSETLPythonOperator, self).__init__(
            *args,
            dag=dag,
            op_kwargs=op_kwargs,
            provide_context=True,
            **kwargs
        )

    def execute(self, context):
        try:
            self.render_all_paths(context)
            self.create_tmpdirs()
            self.extract_inputs()
            super(IMaRSETLPythonOperator, self).execute(context)
            self.load_outputs()
        finally:
            self.cleanup()

    def render_all_paths(self, context):
        for pathkey in self.tmp_paths:
            self.tmp_paths[pathkey] = self.render_template(
                '',
                self.tmp_paths[pathkey],
                context
            )

    def create_tmpdirs(self):
        print("creating tmpdirs...")
        for tdir in self.tmpdirs:
            tmp_dir_path = self.tmp_paths[tdir]
            print("{}=>{}".format(tdir, tmp_dir_path))
            makedirs(tmp_dir_path)

    def extract_inputs(self):
        print("extracting input files from IMaRS data warehouse...")
        for inpf in self.inputs:
            metadata = self.inputs[inpf]
            out_path = self.tmp_paths[inpf]
            print("{}\n\t->\t{}\n\t->\t{}\n\t->\t".format(
                inpf, metadata, out_path
            ))
            imars_etl.extract(
                sql=metadata,
                output_path=out_path
            )

    def load_outputs(self):
        print("loading output files into IMaRS data warehouse...")
        for outf in self.outputs:
            load_args = self.outputs[outf]
            output_path = self.tmp_paths[outf]
            load_args['filepath'] = output_path
            print("{}\n\t->\t{}\n\t->\t{}\n\t->\t".format(
                outf, output_path, load_args
            ))
            load_task(load_args, self)

    def cleanup(self):
        print("cleaning up temporary files...")
        for fkey, tmpf in self.tmp_paths.items():
            print("cleanup {} ({})".format(fkey, tmpf))
            tmp_cleanup_task([tmpf], self)
