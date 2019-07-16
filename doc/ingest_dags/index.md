## Checklist: create an ingest dag
1. identify ingested data in terms of `product_id`(s) from [`imars_product_metadata.product`](https://github.com/USF-IMARS/imars_puppet/blob/test/modules/role/files/sql/product_metadata_rows.sql)s.
2. identify your area(s) from `imars_product_metadata.area`s
3. create a "DAG Definition .py file" in `imars_dags/dags/ingest/`
    * see [example.py](https://github.com/USF-IMARS/imars_dags/blob/master/doc/ingest_dags/example.py)
    * use `BashOperator` or `PythonOperator` to do your ingest
4. add any needed .sh or .py scripts to this repo too
