"""
# === Load
# ======================================================================
# loop through each to_load and load it
"""

from airflow.operators.python_operator import PythonOperator

import imars_etl

from imars_dags.util.etl_tools.tmp_file import tmp_format_str, get_tmp_file_suffix

def add_load(dag, to_load, upstream_operators):
    """
        upstream_operators : airflow.operators.*[]
            Operators which get wired before load. These are the last in your
            processing chain.
        to_load : dict[]
            paths or directories to be loaded into the imars-etl data warehouse
            after processing is done. These are the output files of your DAG.
            must resemble
                {
                    'filepath': ...
                }
            or
                {
                    'directory': ...
                    'product_type_name': ...
                }
            for detailed info see `imars_etl load --help`

    """
    with dag as dag:
        def load_task(**kwargs):
            load_args = kwargs['load_args']
            # default args we add to all load ops:
            load_args['verbose'] = 3
            load_args['load_format'] = tmp_format_str()

            # apply macros on all (template-enabled) args:
            ARGS_TEMPLATE_FIELDS = ['filepath', 'directory']
            task = kwargs['task']
            for key in load_args:
                if key in ARGS_TEMPLATE_FIELDS:
                    load_args[key] = task.render_template(
                        '',
                        load_args[key],
                        kwargs
                    )
                # else don't template the arg

            print('loading {}'.format(load_args))
            imars_etl.load(load_args)

        load_ops = []
        for up_op in upstream_operators:
            for load_args in to_load:
                operator_suffix = get_tmp_file_suffix(load_args)

                load_ops.append(PythonOperator(
                    task_id='load_' + operator_suffix,
                    python_callable=load_task,
                    op_kwargs={'load_args': load_args},
                    provide_context=True,
                ))
                # transform(s) >> load(s)
                up_op >> load_ops[-1]

                # TODO: if load operator fails with IntegrityError (duplicate)
                #    mark success or skip or something...
    return load_ops
