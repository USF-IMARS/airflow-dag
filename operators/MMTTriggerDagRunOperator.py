from datetime import datetime
import logging

from airflow import settings
from airflow.utils.state import State
from airflow.models import DagBag
from airflow.operators.dagrun_operator import TriggerDagRunOperator, DagRunOrder

class MMTTriggerDagRunOperator(TriggerDagRunOperator):
    """
    MMT-patched for passing explicit execution date
    (otherwise it's hard to hook the datetime.now() date).
    Use when you want to explicity set the execution date on the target DAG
    from the controller DAG.

    Adapted from Paul Elliot's solution on airflow-dev mailing list archives:
    http://mail-archives.apache.org/mod_mbox/airflow-dev/201711.mbox/%3cCAJuWvXgLfipPmMhkbf63puPGfi_ezj8vHYWoSHpBXysXhF_oZQ@mail.gmail.com%3e

    Parameters
    ------------------
    execution_date: str
        the custom execution date (jinja'd)

    Usage Example:
    -------------------
    my_dag_trigger_operator = MMTTriggerDagRunOperator(
        execution_date="{{execution_date}}"
        task_id='my_dag_trigger_operator',
        trigger_dag_id='my_target_dag_id',
        python_callable=lambda: random.getrandbits(1),
        params={},
        dag=my_controller_dag
    )
    """
    # NOTE: I would like to add 'task_id', but it does not work
    template_fields = ('execution_date','trigger_dag_id',)

    def __init__(
        self, trigger_dag_id, python_callable, execution_date,
        *args, **kwargs
        ):
        self.execution_date = execution_date
        super(MMTTriggerDagRunOperator, self).__init__(
            trigger_dag_id=trigger_dag_id, python_callable=python_callable,
           *args, **kwargs
       )

    def execute(self, context):
        run_id_dt = datetime.strptime(self.execution_date, '%Y-%m-%d %H:%M:%S')
        dro = DagRunOrder(run_id='trig__' + run_id_dt.isoformat())
        dro = self.python_callable(context, dro)
        if dro:
            session = settings.Session()
            dbag = DagBag(settings.DAGS_FOLDER)
            trigger_dag = dbag.get_dag(self.trigger_dag_id)
            dr = trigger_dag.create_dagrun(
                run_id=dro.run_id,
                state=State.RUNNING,
                execution_date=self.execution_date,
                conf=dro.payload,
                external_trigger=True)
            logging.info("Creating DagRun {}".format(dr))
            session.add(dr)
            session.commit()
            session.close()
        else:
            logging.info("Criteria not met, moving on")
