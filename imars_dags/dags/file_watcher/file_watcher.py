"""
---------------------------------------------------------------------------
Sets up a file watcher that catches a whole bunch of products
and triggers DAGs and changes their status in the metadata db.
---------------------------------------------------------------------------
Tasks in this DAG watch the imars metadata db for `status_id=="to_load"`
files of a certain `product` type. The `product.short_name` is used in the
name of the DAG in the form `file_trigger_{short_name}.py`. The
`last_processed` column is used to prioritize DAG triggering.

A file_trigger DAG only triggers external processing DAGs and updates
`file.status_id`.

---------------------------------------------------------------------------

External DAGs triggered should be triggered with `execution_date` set using
metadata from the file. Because of this each file within a `product_id`
and `area_id` must have a unique `date_time`.
I.e.:

    SELECT COUNT(*) FROM file WHERE
        date_time="2015-02-01 13:30:00" AND product_id=6;

should return only 1 result for any value of `date_time` and `product_id`.

This should be enforced by the following constraint:
`CONSTRAINT pid_and_date UNIQUE(product_id,date_time,area_id)`

---------------------------------------------------------------------------

The following diagram illustrates the relationship between this DAG and
`file.state` in the metadata db:

```
                                                    |====> ["std"]
    (auto_ingest_dag) =|                            |
    manual_ingest =====|==> ["to_load"] ==(file_trigger_dag)=> (proc_dag_1)
                                                    |====> (processing_dag_2)
                                                    |       ...
    key:                                            |====> (processing_dag_n)
    ---------
    (dag_name)
    ["status"]
```
"""
from airflow import DAG

from imars_dags.operators.FileWatcher.FileWatcherOperator \
    import FileWatcherOperator


this_dag = DAG(
    dag_id="file_watcher",
    catchup=False,  # latest only
    schedule_interval="* * * * *",
    max_active_runs=1
)
this_dag.doc_md = __doc__

with this_dag as dag:
    claimed_ids = []  # used to help prevent unwanted duplicate id claims
    # ======================================================================
    # === incoming wv2 .ntf files (already unzipped)
    # pid=11, short_name=ntf_wv2_m1bs
    assert 11 not in claimed_ids
    from imars_dags.dags.wv2_classification import wv_classification
    file_trigger_ntf_wv2_m1bs = FileWatcherOperator(
        task_id="file_trigger_ntf_wv2_m1bs",
        product_ids=[11],
        dags_to_trigger=[
            wv_classification.DAG_NAME,
        ],
        area_names=wv_classification.AREAS,
    )
    claimed_ids.append(11)

    # === incoming zipped wv2 files
    # id 6 == zip_wv3_ftp_ingest
    # assert 6 not in claimed_ids
    # from imars_dags.dags.processing import wv2_unzip
    # file_trigger_zip_wv2_ftp_ingest = FileWatcherOperator(
    #     task_id="file_trigger_zip_wv2_ftp_ingest",
    #     product_ids=[6],
    #     dags_to_trigger=[
    #         get_dag_id(
    #             dag_name=wv2_unzip.DAG_NAME, dag_type=DAGType.PROCESSING
    #         )
    #     ],
    #     area_names=wv2_unzip.AREAS
    # )
    # claimed_ids.append(6)

    # === incoming Sentinel 3 zipped EFR files
    # assert 36 not in claimed_ids
    # from imars_dags.dags.processing.s3_chloro_a import s3_chloro_a
    # file_trigger_s3a_zipped_ol_1_efr = FileWatcherOperator(
    #     task_id="file_trigger_s3a_zipped_ol_1_efr",
    #     product_ids=[36],
    #     dags_to_trigger=[
    #         get_dag_id(
    #             dag_name=s3_chloro_a.DAG_NAME, dag_type=DAGType.PROCESSING
    #         )
    #     ],
    #     area_names=s3_chloro_a.AREAS
    # )
    # claimed_ids.append(36)

    # ======================================================================
    # ids cannot be claimed more than once; that would cause missing DAGRuns.
    # assert no duplicates in claimed_ids
    # (https://stackoverflow.com/a/1541827/1483986)
    if len(claimed_ids) != len(set(claimed_ids)):
        raise AssertionError(
            "too many claims on product #s {}".format(
                set([x for x in claimed_ids if claimed_ids.count(x) > 1])
                )
        )
