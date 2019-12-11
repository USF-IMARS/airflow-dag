# TODO: mv this file into IMaRSETL subpackage
from imars_dags.util.etl_tools.tmp_file import tmp_format_str


def get_default_load_args(**kwargs):
    """default args we add to all load ops"""
    def_args = dict(
        verbose=kwargs.get('verbose', 3),
        load_format=kwargs.get('load_format', tmp_format_str()),
    )
    return def_args
