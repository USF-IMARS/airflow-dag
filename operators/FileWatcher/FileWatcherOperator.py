"""
Sets up a watch for a product file type in the metadata db.
"""
from datetime import datetime

from airflow import settings
from airflow.models import DagBag
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.state import State
import imars_etl

from imars_dags.operators.FileWatcher.FileTriggerSubDAG \
    import get_sql_selection

DAWN_OF_TIME = datetime(2018, 5, 5, 5, 5)  # any date in past is fine


class FileWatcherOperator(PythonOperator):
    def __init__(
        self,
        *args,
        product_ids,
        dags_to_trigger,
        area_names=['na'],
        provide_context=True,
        start_date=DAWN_OF_TIME,
        retries=0,
        **kwargs
    ):
        """
        Parameters
        ----------
        product_ids : int[]
            list of `product_ids` for the product we are watching.
        dags_to_trigger : str[]
            list of DAG names to trigger when we get a new product.
        area_names: str[]
            list of RoIs that we should consider triggering
            example: ['na', 'gom', 'fgbnms']

        """
        super(FileWatcherOperator, self).__init__(
            python_callable=_trigger_dags,
            op_kwargs={
                'product_ids': product_ids,
                'dags_to_trigger': dags_to_trigger,
            },
            templates_dict={
                'metadata_file_filepath': 'metadata_file_filepath',
            },
            provide_context=provide_context,
            retries=retries,
            start_date=start_date,
            **kwargs
        )


def _trigger_dags(
    ds,
    *args,
    product_ids,
    dags_to_trigger,
    templates_dict={},
    **kwargs
):
    # === get file metadata
    # SELECT {cols} FROM file WHERE {sql} {post_where};
    print('SELECT {} FROM file WHERE {} {};'.format(
        '*', get_sql_selection(product_ids), "ORDER BY last_processed DESC LIMIT 1"
    ))
    file_metadata = imars_etl.select(
        # cols="id,area_id",
        sql=get_sql_selection(product_ids),
        post_where="ORDER BY last_processed DESC LIMIT 1",
        first=True,
    )
    # print("\n\tmeta:\n\t{}\n".format(file_metadata))
    # logging.info("\n\n\tmeta:\n\t{}".format(file_metadata))

    # convert area_id to area_name
    file_metadata['area_name'] = imars_etl.id_lookup(
        table='area',
        value=file_metadata['area_id']
    )

    # trigger the dags
    roi_name = file_metadata['area_name']
    trigger_date = file_metadata['date_time'].strftime('%Y-%m-%d %H:%M:%S.%f')
    for processing_dag_name in dags_to_trigger:
        # processing_dag_name is root dag,
        # but each region has a dag
        dag_to_trigger = "{}_{}".format(
            processing_dag_name, roi_name
        )

        # trigger_dag_id=dag_to_trigger,
        run_id_dt = datetime.strptime(
            trigger_date, '%Y-%m-%d %H:%M:%S.%f'
        )
        session = settings.Session()
        dbag = DagBag(settings.DAGS_FOLDER)
        trigger_dag = dbag.get_dag(dag_to_trigger)
        dr = trigger_dag.create_dagrun(
            run_id='trig__' + run_id_dt.isoformat(),
            state=State.RUNNING,
            execution_date=trigger_date,
            # conf=dro.payload,  # ??? docs say: user defined dictionary
            #                    #               passed from CLI :type: dict
            external_trigger=True
        )
        # logging.info("Creating DagRun {}".format(dr))
        session.add(dr)
        session.commit()
        session.close()

        # === update status and/or last_processed:
        # TODO: use something like imars_etl.update() ???
        mysql_hook = MySqlHook(
            mysql_conn_id='imars_metadata'  # TODO: rm hardcoded value
        )
        mysql_hook.run(
            "UPDATE file SET status_id={status} WHERE id={f_id}",
            parameters={
                'status': 1,
                'f_id': file_metadata['id'],
                'last_processed': str(datetime.now())
                # expected date format: 2018-12-19 23:34:08.256244
            }
        )