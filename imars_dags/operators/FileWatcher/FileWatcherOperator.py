"""
Sets up a watch for a product file type in the metadata db.
"""
from datetime import datetime
from datetime import timezone

from airflow import settings
from airflow.models import DagBag
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.state import State
from airflow.exceptions import AirflowSkipException

import imars_etl

import airflow  # required to set up PYTHONPATH
# from imars_dags.operators.FileWatcher.check_ipfs_accessible \
#     import check_ipfs_accessible
from imars_dags.operators.FileWatcher.check_locally_accessible \
    import check_locally_accessible
from imars_dags.operators.FileWatcher.check_for_duplicates \
    import check_for_duplicates
from imars_dags.operators.FileWatcher.check_filesize_match \
    import check_filesize_match

DAWN_OF_TIME = datetime(2018, 5, 5, 5, 5)  # any date in past is fine
VALID_STATUS_IDS = [1, 2, 3]  # += NULL


def get_sql_selection(product_ids):
    # 1=std, 2=external, 3=to_load
    return (
        "(status_id IN ({}) OR status_id IS NULL) AND product_id IN ({})"
    ).format(
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
        validation_meta['status_id'], dt_now,
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
    new_status = int(f_meta.get("status_id", 1))
    print("validating fpath:\n\t{}".format(fpath))
    try:
        check_locally_accessible(f_meta)
    except AssertionError as a_err:
        print(a_err)
        new_status = 8  # status_id.lost

    # TODO: make available on ipfs
    # hash, ipfs_host = check_ipfs_accessible(f_meta)
    hash, ipfs_host = (f_meta['filepath'], "NA")  # temporary disable

    try:
        check_filesize_match(f_meta)
    except RuntimeError as r_err:
        print(r_err)
        new_status = 6  # status_id.wrong_size

    try:
        check_for_duplicates(f_meta)
    except NotImplementedError:
        print('this file found to be a duplicate of another in db.')
        new_status = 7  # status_id.duplicate

    return {
        "last_ipfs_host": ipfs_host,
        "multihash": hash,
        "status_id": new_status,
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
    post_where_str = "ORDER BY proc_counter,last_processed ASC LIMIT 1"
    # SELECT {cols} FROM file WHERE {sql} {post_where}
    print('SELECT {} FROM file WHERE {} {}'.format(
        '*',
        sql_selection,
        post_where_str
    ))
    result = imars_etl.select(
        cols="id,area_id,date_time,filepath,multihash,product_id,n_bytes",
        sql=sql_selection,
        post_where=post_where_str,
        first=True,
    )[0]
    file_metadata = dict(
        id=result[0],
        area_id=result[1],
        date_time=result[2],
        filepath=result[3],
        multihash=result[4],
        product_id=result[5],
        n_bytes=result[6],
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
    elif validation_meta['status_id'] not in VALID_STATUS_IDS:
        print(
            "Non-normal status id; file failed an integrity check. "
            "Skipping DAG triggers."
        )
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
            print("triggering " + dag_to_trigger + "...")

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
                if (any([
                    substr in str(err) for substr in [
                        'uplicate',  # duplicates are fine
                        # "no row" also means duplicate. see:
                        # https://issues.apache.org/jira/browse/AIRFLOW-2319
                        'No row was found for one'
                    ]
                ])):
                    # NOTE: yes, this is a terrible way to check but it's the
                    #      best I can do in this circumstance.
                    # we need to update even if skipping
                    print(
                        'DagRun already exists for this (file + DAG) combo.'
                    )
                    pass
                else:
                    print(
                        'Non-duplicate error while triggering DAGRun ' +
                        '#{} {} @ {}'.format(
                            n_dags_triggered, dag_to_trigger, trigger_date
                        )
                    )
                    print("Stringified exception: {}".format(str(err)))
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
