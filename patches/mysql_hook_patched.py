from airflow.hooks.mysql_hook import MySqlHook

from imars_dags.patches.dbapi_hook_patched import DbApiHook_patched

class MySqlHook_patched(MySqlHook, DbApiHook_patched):
    pass
