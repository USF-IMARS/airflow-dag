from os import makedirs
import re

import imars_etl
from airflow.exceptions import AirflowSkipException
from airflow.exceptions import AirflowException

from imars_dags.util.etl_tools.tmp_file import tmp_filepath
from imars_dags.util.etl_tools.load import get_default_load_args
from imars_dags.util.etl_tools.cleanup import _cleanup_tmp_file


class IMaRSETLMixin(object):
    """
    # =========================================================================
    # === "Transform" operator that "Extracts" & "Loads" auto-magically.
    # =========================================================================
    Leverages IMaRS ETL tools (https://github.com/usf-imars/imars-etl) to:
        * find & "Extract" input files by metadata
        * "Load" & cleanup output files & autofill (some) metadata

    inputs, outputs, and tmpdirs get injected into context['params'] so you
    can template with them as if they were passed into params.

    __init__ parameters:
    -------------------
    inputs : dict of strings
        Mapping of input keys (use like tmp filenames) to metadata SQL queries.
        Files are automatically extracted from the database & injected into
        context['params'] so you can template with them. Downloaded files
        are automatically cleaned up.
    outputs : dict of dicts
        Mapping of output keys (use like tmp filenames) to output metdata to
        be loaded into the data warehouse. Output files are automatically
        loaded into the warehouse and cleaned up after load finishes.
        The following metadata is automatically added to the output product:
            {
                "filepath": "{{ the_given_output_key }}",
                # TODO: more?
            }
    tmpdirs : str[]
        List of tmp directories to be created before run. The tmp dirs are
        automatically created and cleaned up after the job is done.
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
        for outkey in self.outputs:
            self.tmp_paths[outkey] = tmp_filepath(dag.dag_id, outkey)
        for tdir in self.tmpdirs:
            self.tmp_paths[tdir] = tmp_filepath(dag.dag_id, tdir)
            # NOTE: using tmp_filepath here instead of tmp_fildir because we do
            #   not want the auto-added mkdir operator.

        # TODO: assert(len(outputs) > 0) ???

    # =======================================================================
    # =================== BaseOperator Overrides ============================
    def render_template(self, attr, content, context):
        print("adding paths to context:\n\t{}".format(self.tmp_paths))
        # inject tmp_paths into context params so we can template with them
        # print("\n-----\nctx:\n\t{}\n----\n".format(context))
        for path_key, path_val in self.tmp_paths.items():
            context['params'].setdefault(
                path_key,
                # double-render the path_val (so we can use use macros like
                #   {{ts_nodash}} in the tmp_paths.
                super(IMaRSETLMixin, self).render_template(
                    attr, path_val, context
                )
            )
        return super(IMaRSETLMixin, self).render_template(
            attr, content, context
        )

    def pre_execute(self, context):
        # TODO: check metadta for output already exists?
        self._skip_if_output_exists(context)
        self.render_all_paths(context)
        super(IMaRSETLMixin, self).pre_execute(context)

    def execute(self, context):
        # TODO: use pre_execute and post_execute instead ?
        # https://airflow.apache.org/code.html#airflow.models.BaseOperator.post_execute
        try:
            self.create_tmpdirs()
            self.extract_inputs(context)
            super(IMaRSETLMixin, self).execute(context)
            self.load_outputs(context)
        finally:
            self.cleanup()

    # =======================================================================
    # ====================== "private" methods ==============================
    def _skip_if_output_exists(self, context):
        """
        sets the task state to "skipped" if all the outputs already exist.
        """
        print("checking for all outputs already exist...")
        for out_key, out_meta in self.outputs.items():
            sql = out_meta['sql']
            print('"{}" exists?'.format(sql))
            result = imars_etl.select(sql, first=True)
            print("result: {}".format(result))
            if result is not None:
                print('\tyep')
            else:
                print('\tnope')
                return
        else:  # we went through all products and didn't find any "nope"s
            self.skip(
                context['dag_run'],
                context['ti'].execution_date,
                [self]
            )
            raise AirflowSkipException(
                'All output products already exist in the metadata db.'
            )

    def render_all_paths(self, context):
        """
        Basically double-renders the path_val (so we can use use macros like
        {{ts_nodash}} in the tmp_paths.
        """
        for pathkey, pathval in self.tmp_paths.items():
            rendered_pathval = self.render_template(
                '',
                pathval,
                context
            )
            self.tmp_paths[pathkey] = rendered_pathval
        self.inject_tmpdirs_into_tmp_paths()
        # self.sanitize_tmp_paths()  # TODO: should we???

    def sanitize_tmp_paths(self):
        """
        NOTE: currently unused.

        Sanitizes keys in tmp_paths so each key becomes a valid python variable
        name. Based on https://stackoverflow.com/a/3303361/1483986
        """
        for pathkey, pathval in self.tmp_paths.items():
            # Remove invalid characters
            new_pathkey = re.sub('[^0-9a-zA-Z_]', '_', pathkey)

            # cannot start w/ number
            if new_pathkey[0].isdigit():
                new_pathkey = "_" + new_pathkey

            if pathkey != new_pathkey:
                print(
                    "WARN: key '{}' has invalid characters. ".format(pathkey) +
                    " Using key '{}' instead.".format(new_pathkey)
                )
                self.tmp_paths.pop(pathkey)
                self.tmp_paths[new_pathkey] = pathval

    def inject_tmpdirs_into_tmp_paths(self):
        """
        Renders paths with tmpdir keys.
        Example:
            `mytmpdir/myfile.txt` becomes `/tmp/id1234_mytmpdir/myfile.txt`
            (assuming mytmpdir's generated path is `/tmp/id1234_mytmpdir`)
        NOTE:
            This only handles *very* basic stuff. No dir-within-dirs, duplicate
            basenames, or other fancy crap.
        """
        for pathkey, pathval in self.tmp_paths.items():
            if '/' in pathkey:
                dir_key, basename = pathkey.split('/')
                rendered_pathval = self.tmp_paths[dir_key] + '/' + basename
                self.tmp_paths[pathkey] = rendered_pathval

    def create_tmpdirs(self):
        print("creating tmpdirs...")
        for tdir in self.tmpdirs:
            tmp_dir_path = self.tmp_paths[tdir]
            print("{}=>{}".format(tdir, tmp_dir_path))
            makedirs(tmp_dir_path)

    def _render_input_metadata(self, metadata, context):
        attr = ""
        return self.render_template(attr, metadata, context)

    def extract_inputs(self, context):
        print("extracting input files from IMaRS data warehouse...")
        for inpf in self.inputs:
            metadata = self._render_input_metadata(self.inputs[inpf], context)
            out_path = self.tmp_paths[inpf]
            print("{}\n\t->\t{}\n\t->\t{}\n\t->\t".format(
                inpf, metadata, out_path
            ))
            imars_etl.extract(
                sql=metadata,
                output_path=out_path
            )

    def _render_output_metadata(self, metadata, context):
        attr = ""
        for key, val in metadata.items():
            try:
                metadata[key] = self.render_template(attr, val, context)
            except AirflowException as af_ex:
                # Type ... used for parameter not supported for templating
                print(
                    "skipping over exception on output metadata render: "
                    "\n\t{}".format(af_ex)
                )
                pass

        return metadata

    def load_outputs(self, context):
        print("loading output files into IMaRS data warehouse...")
        for outf in self.outputs:
            load_args = self.outputs[outf]
            output_path = self.tmp_paths[outf]
            load_args['filepath'] = output_path
            load_args = self._render_output_metadata(load_args, context)
            print("{}\n\t->\t{}\n\t->\t{}\n\t->\t".format(
                outf, output_path, load_args
            ))
            load_args.update(get_default_load_args(**load_args))
            print('loading {}'.format(load_args))
            imars_etl.load(**load_args)

    def cleanup(self):
        print("cleaning up temporary files...")
        for fkey, tmpf in self.tmp_paths.items():
            print("cleanup {} ({})".format(fkey, tmpf))
            _cleanup_tmp_file(tmpf)
    # =======================================================================
