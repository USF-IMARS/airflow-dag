"""
Sets up a watch for a product file type in the metadata db.
"""
from datetime import datetime
from datetime import timezone

from _mysql_exceptions import IntegrityError
from airflow import settings
from airflow.models import DagBag
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.state import State
from airflow.exceptions import AirflowSkipException

import imars_etl

DAWN_OF_TIME = datetime(2018, 5, 5, 5, 5)  # any date in past is fine


def get_sql_selection(product_ids):
    VALID_STATUS_IDS = [1, 3, 4]
    return "status_id IN ({}) AND product_id IN ({})".format(
        ",".join(map(str, VALID_STATUS_IDS)),
        ",".join(map(str, product_ids))
    )


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
                'area_names': area_names,
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
    area_names,
    templates_dict={},
    **kwargs
):
    # === get file metadata
    # SELECT {cols} FROM file WHERE {sql} {post_where};
    # print('SELECT {} FROM file WHERE {} {};'.format(
    #     '*',
    #     get_sql_selection(product_ids),
    #     "ORDER BY last_processed DESC LIMIT 1"
    # ))
    result = imars_etl.select(
        cols="id,area_id,date_time",
        sql=get_sql_selection(product_ids),
        post_where="ORDER BY last_processed ASC LIMIT 1",
        first=True,
    )
    file_metadata = dict(
        id=result[0],
        area_id=result[1],
        date_time=result[2],
    )
    # print("\n\tmeta:\n\t{}\n".format(file_metadata))
    # logging.info("\n\n\tmeta:\n\t{}".format(file_metadata))

    # convert area_id to area_name
    file_metadata['area_name'] = imars_etl.id_lookup(
        table='area',
        value=file_metadata['area_id']
    )

    if file_metadata['area_name'] not in area_names:
        print((
            "File area '{}' not included in DAG AREAS list. "
            "Skipping DAG triggers."
        ).format(file_metadata['area_name']))
    else:
        # trigger the dags
        roi_name = file_metadata['area_name']
        trigger_dt = file_metadata['date_time']
        # "fix" for "ValueError: naive datetime is disallowed":
        # (assumes tz is UTC)
        trigger_dt = trigger_dt.replace(tzinfo=timezone.utc)
        trigger_date = trigger_dt.strftime('%Y-%m-%d %H:%M:%S.%f')
        print("triggering dags for ds={}...".format(trigger_date))
        for processing_dag_name in dags_to_trigger:
            # processing_dag_name is root dag,
            # but each region has a dag
            dag_to_trigger = "{}_{}".format(
                processing_dag_name, roi_name
            )
            print(dag_to_trigger + "...")

            session = settings.Session()
            dbag = DagBag(settings.DAGS_FOLDER)
            trigger_dag = dbag.get_dag(dag_to_trigger)
            try:
                dr = trigger_dag.create_dagrun(
                    run_id='trig__' + trigger_dt.isoformat(),
                    state=State.RUNNING,
                    execution_date=trigger_dt,
                    # conf=dro.payload,  # ? docs say: user defined dictionary
                    #                    #          passed from CLI :type: dict
                    external_trigger=True
                )
                # logging.info("Creating DagRun {}".format(dr))
                session.add(dr)
                session.commit()
            except IntegrityError:
                # mark this task "skipped" if duplicate
                # TODO: how to ensure this is duplicate and not foreign
                #       key issue?
                raise AirflowSkipException(
                    'Processing DAG for this date_time already in airflow db.'
                )
            finally:
                session.close()
        print("...done. {} DAGs triggered.".format(len(dags_to_trigger)))
        # === update status and/or last_processed:
        # TODO: use something like imars_etl.update() ???
    mysql_hook = MySqlHook(
        mysql_conn_id='imars_metadata'  # TODO: rm hardcoded value
    )
    dt_now = str(datetime.now())
    sql_update = (
        'UPDATE file SET '
        'status_id={},last_processed="{}" WHERE id={}'
    ).format(
        1,  # 1 = status:standard
        dt_now,
        file_metadata['id'],
        # expected date format: 2018-12-19 23:34:08.256244
    )
    print("updating metadata db:\n\t" + sql_update)
    mysql_hook.run(
        sql_update
    )
    # print("file id#{}.last_processed set to {}".format(
    #     file_metadata['id'], dt_now
    # ))
