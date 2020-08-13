"""
loads Dan's river data csv files into graphite
"""

import _csv_to_graphite as csv2graph

csv2graph.main(
    "/srv/imars-objects/fk/DISCH_CSV_USGS/USGS_disch_FWCdb_EFL.csv",
    'imars_regions.fwc.river.east',
    ["mean", "climatology", "anomaly"]
)

csv2graph.main(
    "/srv/imars-objects/fk/DISCH_CSV_USGS/USGS_disch_FKdb.csv",
    'imars_regions.fwc.river.west',
    ["mean", "climatology", "anomaly"]
)
