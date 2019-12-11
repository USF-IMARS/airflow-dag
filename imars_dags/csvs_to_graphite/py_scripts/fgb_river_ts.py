"""
loads Dan's river data csv files into graphite
"""

import _csv_to_graphite as csv2graph

ROI = 'fgb'
csv2graph.main(
    "/srv/imars-objects/modis_aqua_fgbnms/" +
    "DISCH_TS_USGS/USGS_disch_FGBdb_TX.csv",
    'imars_regions.fgb.river.tx',
    ["mean", "climatology", "anomaly"]
)
csv2graph.main(
    "/srv/imars-objects/modis_aqua_fgbnms/" +
    "DISCH_TS_USGS/USGS_disch_FGBdb_MS.csv",
    'imars_regions.fgb.river.ms',
    ["mean", "climatology", "anomaly"]
)
