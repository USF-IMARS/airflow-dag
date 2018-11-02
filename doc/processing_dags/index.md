## Checklist: create a processing dag
1. identify input data in terms of `product_id`(s) from [`imars_product_metadata.product`](https://github.com/USF-IMARS/imars_puppet/blob/test/modules/role/files/sql/product_metadata_rows.sql)s.
2. identify output data in terms of `product_id`s
3. identify area(s) from `imars_product_metadata.area`s
4. create a "DAG Definition .py file" in `imars_dags/dags/processing/`
    * see [example.py](https://github.com/USF-IMARS/imars_dags/blob/master/doc/processing_dags/example.py)
    * use `IMaRSETLBashOperator` or `IMaRSETLPythonOperator`s to define inputs & outputs
5. add any needed .sh or .py files here too
