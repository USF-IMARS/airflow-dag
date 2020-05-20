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


def csv2graph_roi(
    roi, subregions, directory,
    db_version="",  # v2
    prod_format_strings=[
        "/EXT_TS_AQUA/OC/{roi_upper}db_chlor_a",
        "/EXT_TS_AQUA/OC/{roi_upper}db_nflh",
        "/EXT_TS_AQUA/OC/{roi_upper}db_Rrs_667",
        "/EXT_TS_AQUA/OC/{roi_upper}db_ABI",
        "/EXT_TS_AQUA/SST4/{roi_upper}db_sst4"
    ],
    FILENAME_FORMAT="{directory}{prod}_TS_MODA_{timescale}_{loc}.csv"
):
    """
    Parameters:
    -----------
    roi : str
        lowercase (large) region of interest.
        e.g. fk or fgb
    subregions: str []
        small RoI names from within the larger RoI.
    directory: str (path)
        path to the directory which contains the files.
    
    Example Usage:
    --------------
    csv2graph_roi(
        roi='FK',
        subregions=[
            'BB', 'UK', 'MR', 'SFP12', 'SFP18', 'WS', 'LK', 'DT', 'WFS', 'SR',
            'IFB', 'FLB'
        ],
        directory='/srv/imars-objects/modis_aqua_fk'
    )
        
    """
    # TODO: could use glob instead of manually passing subregions
    for prod in prod_format_strings:
        for loc in subregions:
            # === daily
            product_name = prod.split("db{}_".format(db_version))[1]
            fname = FILENAME_FORMAT.format(
                directory=directory,
                prod=prod.format(
                    roi_upper=roi.upper()
                ),
                timescale="daily",
                loc=loc,
            )
            csv2graph.main(
                fname,
                'imars_regions.{roi_lower}.roi.{loc}.{product_name}'.format(
                    loc=loc,
                    product_name=product_name,
                    roi_lower=roi.lower()
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
