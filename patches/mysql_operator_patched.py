# -*- coding: utf-8 -*-
from airflow.operators.mysql_operator import MySqlOperator

from imars_dags.patches.mysql_hook_patched import MySqlHook_patched as MySqlHook


class MySqlOperator_patched(MySqlOperator):
    """
    monkey-patched mysql_operator to add return value
    """
    # overrides MySqlOperator to add return value of hook.run
    def execute(self, context):
        self.log.info('Executing: %s', self.sql)
        hook = MySqlHook(mysql_conn_id=self.mysql_conn_id,
                         schema=self.database)
        return hook.run(
            self.sql,
            autocommit=self.autocommit,
            parameters=self.parameters)
