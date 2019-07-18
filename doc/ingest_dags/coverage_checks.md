"coverage check" is defined as the process of checking a satellite data feed
for granules (timestamped files) that cover your Region of Interest (RoI).

Below are some terms used to describe coverage check strategies:

## Latest-only
This approach checks only the most recent granule.
It can run on any schedule, but ideally your scheduling should have a frequency
greater than or equal to the granule publication frequency.
Useful for NRT ingests, but impossible to backfill.

A DAG which performs a *latest-only* coverage check will resemble:

```python
latest_only_coverage_dag = DAG(
    schedule_interval = timedelta(minutes=5),
    catchup=False,  # latest only
    max_active_runs=1
)
```

## Granule-datematched
The execution_date of the task matches the date of the granule being checked.
So a task run with "execution_date" of 2018-06-06T36:33:00 checks the
granule at that datetime.
Granule length can be important here, especially in cases where the granules
are of variable length (eg sentinel-3).

A DAG which performs a *granule-datematched* coverage check will resemble:

```python
latest_only_coverage_dag = DAG(
    default_args=get_default_args(
        # this start date should be the first granule to ingest
        start_date=datetime(2001, 11, 28, 14, 00, 00)
    ),
    schedule_interval = timedelta(minutes=5),  # the frequency of granules
    catchup=True,  # latest only
)
```

## Feed-checked
A "feed-checked" coverage check validates the feed before checking coverage.
In other words: there are two tasks:

1. check that requested granule exists
2. check if requested granule covers RoI
