"""
Sets up a watch for a product file type in the metadata db.
"""
from datetime import datetime
from datetime import timezone
import subprocess
import os.path
import socket

from airflow import settings
from airflow.models import DagBag
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.state import State
from airflow.exceptions import AirflowSkipException

import imars_etl

from imars_dags.util.globals import QUEUE

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
        # queue=QUEUE.IPFS_PRIVATE_NODE,
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
            # queue=queue,
            **kwargs
        )


def update_metadata_db(file_metadata, validation_meta):
    mysql_hook = MySqlHook(
        mysql_conn_id='imars_metadata'  # TODO: rm hardcoded value
    )
    dt_now = str(datetime.now())
    sql_update = (
        'UPDATE file SET '
        'status_id={},last_processed="{}",proc_counter=proc_counter+1,'
        'last_ipfs_host="{}",multihash="{}" '
        'WHERE id={}'
    ).format(
        1, dt_now,
        validation_meta['last_ipfs_host'], validation_meta['multihash'],
        file_metadata['id'],
        # expected date format: 2018-12-19 23:34:08.256244
    )
    print("updating metadata db:\n\t" + sql_update)
    mysql_hook.run(
        sql_update
    )


def _validate_file(f_meta):
    """performs validation on file row before triggering"""
    fpath = f_meta['filepath']
    # ensure accessible at local
    assert os.path.isfile(fpath)
    # ensure accessbile over IPFS
    # TODO: adding it everytime is probably overkill
    #       but it needs to be added, not just hashed.
    new_hash = subprocess.check_output(
        # "ipfs add -Q --nocopy {localpath}".format(
        "ipfs add -Q --only-hash {localpath}".format(
            localpath=fpath
        )
    )
    old_hash = f_meta["multihash"]
    if old_hash is not None and old_hash != "" and old_hash != new_hash:
        raise ValueError(
            "file hash does not match db!\n\tdb_hash:{}\n\tactual:{}"
        )
    return {
        "last_ipfs_host": socket.gethostname(),
        "multihash": new_hash
    }


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
    sql_selection = get_sql_selection(product_ids)
    post_where_str = "ORDER BY last_processed ASC LIMIT 1"
    # SELECT {cols} FROM file WHERE {sql} {post_where};
    print('SELECT {} FROM file WHERE {} {};'.format(
        '*',
        sql_selection,
        post_where_str
    ))
    result = imars_etl.select(
        cols="id,area_id,date_time,filepath,multihash",
        sql=sql_selection,
        post_where=post_where_str,
        first=True,
    )
    file_metadata = dict(
        id=result[0],
        area_id=result[1],
        date_time=result[2],
        filepath=result[3],
        multihash=result[4],
    )
    # print("\n\tmeta:\n\t{}\n".format(file_metadata))
    # logging.info("\n\n\tmeta:\n\t{}".format(file_metadata))

    # convert area_id to area_name
    file_metadata['area_name'] = imars_etl.id_lookup(
        table='area',
        value=file_metadata['area_id']
    )
    validation_meta = _validate_file(file_metadata)
    n_dags_triggered = 0
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

            try:
                session = settings.Session()
                dbag = DagBag(settings.DAGS_FOLDER)
                trigger_dag = dbag.get_dag(dag_to_trigger)
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
                n_dags_triggered += 1
            except Exception as err:
                # mark this task "skipped" if duplicate
                if('uplicate' in str(err)):
                    # NOTE: yes, this is a terrible way to check but it's the
                    #      best I can do in this circumstance.
                    # we need to update even if skipping
                    pass
                else:
                    raise
            finally:
                session.close()
        print("...done. {} DAGs triggered.".format(n_dags_triggered))
    # === update status and/or last_processed:
    # TODO: use something like imars_etl.update() ???
    update_metadata_db(file_metadata, validation_meta)
    if n_dags_triggered < 1:
        raise AirflowSkipException(
            'No DAGs triggered for this file. '
            'DAGRuns may already exist.'
        )
