"""
=== bouys:
/srv/imars-objects/modis_aqua_fk/SAL_TS_NDBC/pkyf1_NDBC_sal_FKdb.csv
/srv/imars-objects/modis_aqua_fk/SAL_TS_NDBC/pkyf1_NDBC_temp_FKdb.csv
"""

import _csv_to_graphite as csv2graph

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
