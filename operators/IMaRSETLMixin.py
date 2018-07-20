from os import makedirs

import imars_etl

from imars_dags.util.etl_tools.tmp_file import tmp_filepath
from imars_dags.util.etl_tools.load import load_task
from imars_dags.util.etl_tools.cleanup import tmp_cleanup_task
from imars_dags.util._render import _render


class IMaRSETLMixin(object):
    """
    # =========================================================================
    # === "Transform" operator that "Extracts" & "Loads" auto-magically.
    # =========================================================================
    Leverages IMaRS ETL tools (https://github.com/usf-imars/imars-etl) to:
        * find & "Extract" input files by metadata
        * "Load" & cleanup output files & autofill (some) metadata
    """
    # =================== subclass helper methods ============================
    def pre_init(self, inputs, outputs, tmpdirs, dag):
        """
        Does generic handling of args.
        Should be the first line in a subclass's __init__() definition.
        """
        self.inputs = inputs
        self.outputs = outputs
        self.tmpdirs = tmpdirs

        self.tmp_paths = {}
        self.tmp_dirs = []
        for inpf in self.inputs:
            self.tmp_paths[inpf] = tmp_filepath(dag.dag_id, inpf)
        for opf in self.outputs:
            self.tmp_paths[opf] = tmp_filepath(dag.dag_id, opf)
        for tdir in self.tmpdirs:
            self.tmp_paths[tdir] = tmp_filepath(dag.dag_id, tdir)
            # NOTE: using tmp_filepath here instead of tmp_fildir because we do
            #   not want the auto-added mkdir operator.

        # TODO: how?!?
        # add the double-render filter
        # user_defined_filters['render'] = _render
    # =======================================================================
    # =================== BaseOperator Overrides ============================
    def render_template(self, attr, content, context):
        print("adding paths to context:\n\t{}".format(self.tmp_paths))
        enhanced_ctx = context.copy()
        # include tmp_paths in context so we can template with them
        for key, val in self.tmp_paths.items():
            # enhanced_ctx.update(self.tmp_paths) but raise err on overwrites
            if context.get(key) is not None:
                raise ValueError(
                    "tmp filepath uses reserved key {}. ".format(key) +
                    "Please use a different key. " +
                    "In-context value is '{}'".format(context.get(key))
                )
            else:
                enhanced_ctx[key] = val
        return super(IMaRSETLMixin, self).render_template(
            attr, content, enhanced_ctx
        )

    def execute(self, context):
        # TODO: use pre_execute, post_execute and prepare_template ?
        # https://airflow.apache.org/code.html#airflow.models.BaseOperator.post_execute
        try:
            self.render_all_paths(context)
            self.create_tmpdirs()
            self.extract_inputs()
            super(IMaRSETLMixin, self).execute(context)
            self.load_outputs()
        finally:
            self.cleanup()

    # =======================================================================
    # ====================== "private" methods ==============================
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
    # =======================================================================
