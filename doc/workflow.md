A general overview of what working with airflow looks like.
There are multiple ways to do things - each letter-based sub-ordering (eg 1a, 1b) is an alternative method of 
accomplishing the same overarching goal indicated by the number.

Everything here refers to the test airflow server. 
Moving things from the test server to the production server is accomplished by merging the branches.

# cheatsheet
* [airflow test server webGUI](http://imars-airflow-test.marine.usf.edu/admin/)
* [grafana airflow monitoring dash](http://grafana.marine.usf.edu:3000/dashboard/db/airflow)

# 1) edit dags
## 1a) via github
Edit files directly on github on the master branch.
The server will automatically pull your changes (via puppet).
Pulls occur approximately once every half hour.

## 1b) directly on the server
Files can be modified on the test server directly using ssh+vi, sshfs, or another method.

# 2) test dagRuns
More advanced usage via the airflow CLI is available if you can ssh into the airflow server, login as the airflow user, then use `airflow help`.

## 2.1 Starting new DagRuns
### 2.1a) via GUI
DagRuns can be triggered immediately using the airflow web GUI using the "Trigger DAG" button at the Dag list.
Note that for ["granule datematched"](https://github.com/USF-IMARS/imars_dags/blob/master/doc/coverage_checks.md#granule-datematched)
DAGs, this will not work because the `execution_date` must match the file date.

## 2.2 Re-running a DagRun
To re-run a DagRun, you must `clear` the state of each task & the DagRun itself.
I find this is easiest from the "tree view" of the DAG.
Click the DAG and "clear" or else clear tasks individually. 
Once cleared, the task(s) will queue again and rerun.

# 3) view results
## 3a) web GUI
Most useful views in the airflow web GUI:
* overview of all dags
* tree view
* logs for each task

## 3b grafana dashboard
The grafana dashboard shows details over time about dags, tasks, nodes, and products.

## 3c Celery Flower
The production server runs a celery flower that shows real-time information on the nodes & tasks running.
