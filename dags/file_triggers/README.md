DAGs in `/file_triggers/` watches the imars metadata db for `status_id=="to_load"`
files of a certain `product` type. The `product.short_name` is used in the
name of the DAG in the form `file_trigger_{short_name}.py`.

A file_trigger DAG only triggers external processing DAGs and updates
`file.status_id`.

---------------------------------------------------------------------------

External DAGs triggered should be triggered with `execution_date` set using
metadata from the file. Because of this each file within a `product_id`
and `area_id` must have a unique `date_time`.
I.e.:
`SELECT COUNT(*) FROM file WHERE date_time="2015-02-01 13:30:00" AND product_id=6`
should return only 1 result for any value of `date_time` and `product_id`.

This *should* be enforced by the following constraint:
`CONSTRAINT pid_and_date UNIQUE(product_id,date_time,area_id)`
(...but it isn't right now.)

---------------------------------------------------------------------------

The following diagram illustrates the relationship between these DAGs and
`file.state` in the metadata db:

```
                                                    |====> ["std"]
(auto_ingest_dag) =|                                |
manual_ingest =====|==> ["to_load"] ==(file_trigger_dag)===> (processing_dag_1)
                                                      |====> (processing_dag_2)
                                                      |       ...
key:                                                  |====> (processing_dag_n)
---------
(dag_name)
["status"]

```
