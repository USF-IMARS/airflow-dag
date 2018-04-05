from airflow.hooks.dbapi_hook import DbApiHook
from airflow.hooks.base_hook import BaseHook

class DbApiHook_patched(DbApiHook, BaseHook):
    def run(self, sql, autocommit=False, parameters=None):
        """
        overrides DbApiHook's run, but with return value added
        """
        results = []

        if isinstance(sql, basestring):
            sql = [sql]

        with closing(self.get_conn()) as conn:
            if self.supports_autocommit:
                self.set_autocommit(conn, autocommit)

            with closing(conn.cursor()) as cur:
                for s in sql:
                    if sys.version_info[0] < 3:
                        s = s.encode('utf-8')
                    self.log.info(s)
                    if parameters is not None:
                        results.append(cur.execute(s, parameters))
                    else:
                        results.append(cur.execute(s))

                conn.commit()

        return results
