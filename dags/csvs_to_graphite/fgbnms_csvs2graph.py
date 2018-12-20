"""
Reads each csv file and pushes the data into graphite.
"""
import _csv_to_graphite as csv2graph

# all cols in the csv file
# 1. Time (UNIX)
# 2. Mean
# 3. Climatology
# 4. Anomaly
FIELDS = [
    "mean", "climatology", "anomaly"
]

CSV2GRAPH_ARGS = [  # TODO: fix duplication here & in dag
    # === oc fgbnms
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/OC/EastFG_OC_ts_FGB.csv',
        'imars_regions.east_fgbnms.chlor_a', FIELDS
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/OC/STET_OC_ts_FGB.csv',
        'imars_regions.stet_fgbnms.chlor_a', FIELDS
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/OC/WestFG_OC_ts_FGB.csv',
        'imars_regions.west_fgbnms.chlor_a', FIELDS
    ),
    # === sst fgbnms
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/SST/EastFG_SST_ts_FGB.csv',
        'imars_regions.east_fgbnms.sst', FIELDS
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/SST/STET_SST_ts_FGB.csv',
        'imars_regions.stet_fgbnms.sst', FIELDS
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/SST/WestFG_SST_ts_FGB.csv',
        'imars_regions.west_fgbnms.sst', FIELDS
    ),
    # === rivers near fgbnms
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/DISCH/BrazosRv_all.csv',
        'imars_regions.brazos.disch', FIELDS
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/DISCH/BrazosRv_all.csv',
        'imars_regions.brazos_river.disch', FIELDS
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/DISCH/MissRv_all.csv',
        'imars_regions.mississippi_river.disch', FIELDS
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/DISCH/NechesRv_all.csv',
        'imars_regions.neches_river.disch', FIELDS
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/DISCH/SabineRv_all.csv',
        'imars_regions.sabine_river.disch', FIELDS
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/DISCH/TrinityRv_all.csv',
        'imars_regions.trinity_river.disch', FIELDS
    ),
]


def main():
    for args in CSV2GRAPH_ARGS:
        print("pushing csv into graphite:")
        print(args)
        csv2graph.main(*args)


if __name__ == "__main__":
    main()
