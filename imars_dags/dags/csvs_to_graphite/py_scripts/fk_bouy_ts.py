"""
=== bouys:
/srv/imars-objects/modis_aqua_fk/SAL_TS_NDBC/pkyf1_NDBC_sal_FKdb.csv
/srv/imars-objects/modis_aqua_fk/SAL_TS_NDBC/pkyf1_NDBC_temp_FKdb.csv
"""

import _csv_to_graphite as csv2graph

for produ in ['sal', 'temp']:
    for roi in [
        'bnkf1', 'BOBALLEN', 'bobf1', 'BUTTERNUT', 'LITTLERABBIT',
        'lrkf1', 'PETERSON', 'pykf1', 'WHIPRAY', 'wrbf1'
    ]
    csv2graph.main(
        (
            "/srv/imars-objects/fk/SAL_TS_NDBC/" +
            f"{roi}_NDBC_{produ}_FKdb.csv"
        ),
        f"imars_regions.fk.bouys.{roi}.{produ}",
        ["mean", "climatology", "anomaly"]
    )
