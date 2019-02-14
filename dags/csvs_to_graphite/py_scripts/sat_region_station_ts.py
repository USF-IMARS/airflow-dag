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

FILENAME_FORMAT = (
    "{directory}{prod}_TS_MODA_{timescale}_{loc}.csv"
)


def csv2graph_roi(roi, subregions, directory):
    # TODO: could use glob instead of manually passing subregions
    for prod in [
        "/EXT_TS_AQUA/OC/{roi}db_chlor_a",
        "/EXT_TS_AQUA/OC/{roi}db_nflh",
        "/EXT_TS_AQUA/OC/{roi}db_Rrs_667",
        "/EXT_TS_AQUA/OC/{roi}db_ABI",
        "/EXT_TS_AQUA/SST4/{roi}db_sst4"
    ]:
        for loc in subregions:
            # === daily
            product_name = prod.split("db_")[1]
            fname = FILENAME_FORMAT.format(
                directory=directory,
                prod=prod.format(
                    roi=roi
                ),
                timescale="daily",
                loc=loc,
            )
            csv2graph.main(
                fname,
                'imars_regions.{roi}.roi.{loc}.{prod}'.format(
                    loc=loc, product_name=product_name,
                    roi=roi
                ),
                ["mean", "climatology", "anomaly"]
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
