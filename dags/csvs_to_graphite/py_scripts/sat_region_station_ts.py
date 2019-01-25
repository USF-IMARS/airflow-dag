"""
Here, you'll see that the roi (FK), product (chlor_a), and location
(LK -  Looe Key) are contained in the filename in the following example.
This naming convention will hold for FGB and any other roi we add.

/srv/imars-objects/modis_aqua_fk/EXT_TS_AQUA/OC/FKdb_chlor_a_TS_MODA_daily_LK.csv
/srv/imars-objects/modis_aqua_fk/EXT_TS_AQUA/OC/FKdb_chlor_a_TS_MODA_weekly_LK.csv

In these files, the first two columns are what we want. Time is column 1 in
unix time format. These are just the daily unbinned data at this point. Column
2 is the mean. Now that I look at these files, I see a lot of NaN values in
column 2 (mean). This is is when we have clouds or other flags that mask out
the data.
"""

import _csv_to_graphite as csv2graph

# === sat ts:
ROIS = [
    'BB', 'UK', 'MR', 'SFP12', 'SFP18', 'WS', 'LK', 'DT', 'GOM', 'SR', 'FLB'
]
FILENAME_FORMAT = (
    "/srv/imars-objects/modis_aqua_{roi}"
    "{prod}_TS_MODA_{timescale}_{loc}.csv"
)
OC_PRE = "/EXT_TS_AQUA/OC/FKdb_"
for roi in ['fk']:  # TODO: fgbnms (but sub-locs will be different)
    for prod in [
        [OC_PRE, 'chlor_a'],
        [OC_PRE, 'nflh'],
        [OC_PRE, 'Rrs_667'],
        ["/EXT_TS_AQUA/SST4/FKdb_", "sst4"]
    ]:
        for loc in ROIS:
            # === daily
            fname = FILENAME_FORMAT.format(
                roi=roi, prod="".join(prod), loc=loc, timescale="daily"
            )
            csv2graph.main(
                fname,
                'imars_regions.fk.roi.{loc}.{prod}'.format(
                    loc=loc, prod=prod[1]
                ),
                ["mean"]
            )

            # === weekly
            # TODO: enable? what graphite namespace?
            # fname = FILENAME_FORMAT.format(
            #     roi=roi, prod="".join(prod), loc=loc, timescale="weekly"
            # )
            # csv2graph.main(
            #     fname,
            #     'weekly.imars_regions.fk.roi.{loc}.{prod}'.format(
            #         loc=loc, prod=prod[1]
            #     ),
            #     ["mean"]
            # )
