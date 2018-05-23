# airflow-dag
:blowfish: USF IMaRS Airflow DAGs

Don't hesitate to
[open an issue](https://github.com/USF-IMARS/imars_dags/issues/47) if you are
confused or something isn't working for you.
This is under heavy development so documentation or code may contain errors.

## DAG development guidelines
### Basic DAG Development Workflow
0. `ssh` into test server (probably `imars-airflow-test.marine.usf.edu`)
1. Edit DAGs on github (or directly on server)
2. Download your changes from github to the test server (if applicable)
    * `git pull` from `/home/airflow/airflow/dags/imars_dags`
3. Use airflow [test command(s)](https://airflow.apache.org/tutorial.html#testing) to test a DAG run on the server.
    * `airflow list_dags` should show your DAG and no errors
    * `airflow test $dag_name $task_name $dt` will create a test DAG Run

### organizational rules
0. A DAG (Directed Acyclic Graph) defines a processing pipeline.
1. Each DAG is in it's own python file.
1. The DAG file name matches the name string passed to the DAG constructor.
2. The main DAG variable in the file is named `this_dag`.
3. Operator `task_id` string matches the operator variable name.

### Tips for IMaRS data processing DAGs
Data procesing DAGs in general follow the ETL pattern by using the helpers in
 `dags/util/etl_tools/`.

A typical automated processing pipeline should look something like:

```

[external data src]---(ingest DAG)--->(imars-etl.load)<---------------------\
                                          |                                  \
           [imars_product_metadata db]<---|--->[IMaRS object storage]         \
            |                                              |                   \
  (product FileTriggerDAG)                                (imars-etl.extract)  |
            |                                                              |   |
            |--->(product processing DAG)<--\                              |   |
            |--->(product processing DAG)<---[local copy of input data]<---|   |
            |--->(product processing DAG)<--/                                  |
            |---> ...                \\\                                       |
                                      \--->[local copy of output data]---------|
```

Within this there are 3 types of DAGs to be defined:

1. product processing DAGs : create a new data product by transforming data from
                             existing products. Eg: L1 file into L2.
2. ingest DAGs : fetch data from external sources and load the object & metadata
                 into IMaRS' system.
3. FileTriggerDAGs : Start up a list of processing DAGs when a new product file
                     is added. There must be only one of these per product type.

#### imars_metadata_db
A MySQL database of IMaRS data/image products is maintained independently of
airflow. This database (imars_product_metadata) contains information about the
data products like the datetime of each granule, the product "type", and
coverage areas. This information can be searched by connecting to the database
directly, or through the use of the
[imars-etl](https://github.com/usf-imars/imars-etl) package.
This database serves two functions:

1. enable airflow scripts to trigger based on files added with `FileTriggerDAG`
2. allow IMaRS users to search for data products by metadata.

#### imars-etl
Using `imars-etl` is critical for fetching and uploading IMaRS data products.
"ETL" is short for Extract-Transform-Load and this describes data processing in
general:

1. "extract" products we want to use as inputs from IMaRS' data server(s).
2. "transform" the data via some processing method(s)
3. "load" the result as a new data product into IMaRS' data system.

The imars-etl package aims to simplify the "extract" and "load" steps by hiding
the complexity of IMaRS' data systems behind a nice CLI.

To make things even more simple for airflow DAGs `./dags/util/etl_tools`
includes some helper functions to set up imars-etl operators automatically.
The helper will add extract, load, and cleanup operators to your DAG to wrap
around your processing operators like so:
```
(imars-etl extract)-->(your processing operators)-->(imars-etl load)
      \                                               \
       \-----------------------------------------------\-->(clean up local files)
```

#### FileTriggerDAG
A `FileTriggerDAG` is a DAG which checks the IMaRS product metadata database for
new files and starts up processing DAGs.

## installation
installation should be handled by the imars_airflow puppet module,
but here are the general steps:
1. clone the repo
2. cp settings/secrets.py manually
3. install repo & dependencies w/ `pip install -e .` using setup.py

## NOTES:
include this from a pip requirements.txt like:

`-e git://github.com/USF-IMARS/imars_dags.git@master#egg=imars_airflow_config`
