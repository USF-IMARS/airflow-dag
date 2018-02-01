# Using IMaRS airflow
This document gives a broad overview of how to accomplish some basic research
tasks.

## Triggering a DAG
Many DAGs are triggered automatically on scheduled interval.
Other DAGs may need to be triggered manually or by another DAG with [TriggerDagRunOperator](https://airflow.apache.org/_modules/dagrun_operator.html).

### Manually Trigger DAG from web UI
To create a DAG with `execution_date` of right now, just click the "play" button next to the DAG.
For creating a DAG with specific parameters go to `Browse -> Dag Runs -> Create`.
[ref](https://stackoverflow.com/a/43375518/1483986)

### Manually Trigger DAG from CLI

`airflow run my_dag_name 2017-1-23T12:13`

This will not re-run a DAG that has already run.
To re-run an existing DAG add `--force=true` to the command above.
You could also `clear` the DAG run first.
To re-run a set of scheduled DAG runs use `backfill`.

* Good summary at [this S.O. answer](https://stackoverflow.com/a/41817854/1483986)
* [official CLI docs](https://airflow.incubator.apache.org/cli.html).

## Adding a new Region of Interest (RoI)

### code
1. create file to define basic region attributes at `./regions/<region_name>.py`
    - lat / lon should be identical to any xml or par in files in part (2)
2. create package directory /dags/<regionName>
    - directory name must match `place_name` from step (1)
    - create dags here using the factory methods in `./dags/builders/`
        * NOTE: you will probably start by copying another region's dag

### backend
A new endpoint for the region must be set up on the backend that conforms with
the [imars-objects atomic product hierarchy](https://github.com/USF-IMARS/IMaRS-docs/blob/master/docs/management_data/imars-objects.md) and `./util/satfilename.py`.
Here are the basic steps to set up a new NFS endpoint at `/srv/imars-objects/my_region`
1. choose a server to host the data & ssh there
2. create the new `imars-object` dir
    - `[root@thing2 ~]# mkdir /thing2/sat-products/my_region && cd /thing2/sat-products/my_region`
3. create any product subdirectories needed (or copy them from another region)
    - `[root@thing2 my_region]# mkdir metadata-ini myd01 l1b l2 l3 hkm geo qkm png_chlor_a`
4. configure permissions
    - `[root@thing2 my_region]# chown -R nfsnobody:nfsnobody .`
5. Next, in the puppet configuration for the data server:
    1. add a `profile::nfs_server::exports` to the `/data/node/my_server.yaml`
        - `/thing2/sat-products/my_region: { }`
    2. (optional) add backup using `backdat::fileset:`
    3. add the new nfs share to `profile::imars_datashare_client_autofs` in `mounts` `imars-objects` `mapcontents`
        - `ao1 $default_options thing2${host_suffix}:/ao1`,
6. The endpoint is now ready (after puppet runs on each of your nodes)

## Creating Operators
1. Define the operator within your DAG.

## Re-running tasks
Tasks can be re-run using the web GUI.
To re-run an individual task select the task in tree view and "clear" it.
To re-run many tasks:

1. start at [the list of task instances](http://airflowmaster/admin/taskinstance/)
2. add filters until you have narrowed down to the set of tasks you want to re-run
3. check the boxes next to the tasks
4. "with selected" > Delete
