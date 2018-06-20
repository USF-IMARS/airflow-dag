"coverage check" is defined as the process of checking a satellite data feed
for granules (timestamped files) that cover your Region of Interest (RoI).

Below are some terms used to describe our coverage checks:

## Latest-only
This approach checks only the most recent granule.
It can run on any schedule, but ideally your scheduling should have a frequency
greater than or equal to the granule publication frequency.
Useful for NRT ingests, but impossible to backfill.

## Granule-datematched
The execution_date of the task matches the date of the granule being checked.
So a task run with "execution_date" of 2018-06-06T36:33:00 checks the
granule at that datetime.
Granule length can be important here, especially in cases where the granules
are of variable length (eg sentinel-3).

## Feed-checked
A "feed-checked" coverage check validates the feed before checking coverage.
In other words: there are two tasks:

1. check that requested granule exists
2. check if requested granule covers RoI
