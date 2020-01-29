"""
loads Dan's river data csv files into graphite
"""

import _csv_to_graphite as csv2graph

csv2graph.main(
    "/srv/imars-objects/modis_aqua_fk/DISCH_TS_USGS/USGS_disch_FKdb.csv",
    'imars_regions.fk.river',
    ["mean", "climatology", "anomaly"]
)
