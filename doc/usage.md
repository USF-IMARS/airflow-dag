# Using IMaRS airflow
This document gives a broad overview of how to accomplish some basic research
tasks.

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
Checklist for creating an imars-objects chunk [here](https://github.com/USF-IMARS/IMaRS-docs/tree/master/docs/management_data/imars-objects).

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
