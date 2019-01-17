"""
Here, you'll see that the roi (FK), product (chlor_a), and location
(LK -  Looe Key) are contained in the filename in the following example.
This naming convention will hold for FGB and any other roi we add.

/srv/imars-objects/modis_aqua_fk/EXT_TS_AQUA/OC/FKdb_chlor_a_TS_MODA_daily_LK.csv

In these files, the first two columns are what we want. Time is column 1 in
unix time format. These are just the daily unbinned data at this point. Column
2 is the mean. Now that I look at these files, I see a lot of NaN values in
column 2 (mean). This is is when we have clouds or other flags that mask out
the data.
"""

import _csv_to_graphite as csv2graph

# === sat ts:
ROIS = ['DT', 'LK', 'MR', 'SFP12', 'SFP68', 'SFP57', 'SFP30', 'SFP3']
OC_PRE = "/EXT_TS_AQUA/OC/FKdb_"
for roi in ['fk']:  # TODO: fgbnms (but sub-locs will be different)
    for prod in [
        [OC_PRE, 'chlor_a'],
        [OC_PRE, 'nflh'],
        [OC_PRE, 'Rrs_667'],
        ["/EXT_TS_AQUA/SST4/FKdb_", "sst4"]
    ]:
        for loc in ROIS:
            csv2graph.main(
                (
                    "/srv/imars-objects/modis_aqua_{roi}"
                    "{prod}_TS_MODA_daily_{loc}.csv"
                ).format(
                    roi=roi, prod="".join(prod), loc=loc
                ),
                'imars_regions.fk.roi.{loc}.{prod}'.format(
                    loc=loc, prod=prod[1]
                ),
                ["mean"]
            )

# === bouys:
# /srv/imars-objects/modis_aqua_fk/SAL_TS_NDBC/pkyf1_NDBC_sal_FKdb.csv
# /srv/imars-objects/modis_aqua_fk/SAL_TS_NDBC/pkyf1_NDBC_temp_FKdb.csv
for produ in ['sal', 'temp']:
    csv2graph.main(
        (
            "/srv/imars-objects/modis_aqua_fk/SAL_TS_NDBC/"
            "pkyf1_NDBC_{}_FKdb.csv"
        ).format(
            produ
        ),
        'imars_regions.fk.bouys.pkyf1.{}'.format(
            produ
        ),
        ["mean", "climatology", "anomaly"]
    )

# === rivers:
csv2graph.main(
    "/srv/imars-objects/modis_aqua_fk/DISCH_TS_USGS/USGS_disch_FKdb.csv",
    'imars_regions.fk.river',
    ["FL_Bay_river_index"]
)
