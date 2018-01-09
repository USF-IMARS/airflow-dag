# Using IMaRS airflow
This document gives a broad overview of how to accomplish some basic research
tasks.

## Adding a new Region of Interest (RoI)
1. add the basic region information to `settings/regions.py`
    - lat / lon should be identical to any in files in part (2)
2. add xml & par files to `/settings/regions/<place_name>/`
    - directory name must match `place_name` from `regions.py`

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
