"""
Here, you'll see that the roi (FK), product (chlor_a), and location
(LK -  Looe Key) are contained in the filename in the following example.
This naming convention will hold for FGB and any other roi we add.

/srv/imars-objects/modis_aqua_fk/EXT_TS_AQUA/OC/FKdb_chlor_a_TS_MODA_1D_LK.csv

In these files, the first two columns are what we want. Time is column 1 in
unix time format. These are just the daily unbinned data at this point. Column
2 is the mean. Now that I look at these files, I see a lot of NaN values in
column 2 (mean). This is is when we have clouds or other flags that mask out
the data.
"""

import _csv_to_graphite as csv2graph


for roi in ['fk']:  # TODO: fgbnms (but sub-locs will be different)
    for prod in ['chlor_a', 'nflh', 'Rrs_667']:  # TODO: + 'sst4' - not OC dir
        for loc in ['LK', 'MR', 'DTN', 'WS12', 'WS57_3', 'WS68']:
            csv2graph.main(
                (
                    "/srv/imars-objects/modis_aqua_{roi}/EXT_TS_AQUA/OC/"
                    "FKdb_{prod}_TS_MODA_1D_{loc}.csv"
                ).format(
                    roi=roi, prod=prod, loc=loc
                ),
                'imars_regions.{loc}.{prod}'.format(
                    loc=loc, prod=prod
                ),
                ["mean"]
            )
