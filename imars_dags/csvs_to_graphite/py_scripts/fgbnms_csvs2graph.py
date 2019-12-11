"""
Deprecated method has been replaced by sat_region_station_ts. (I think...)
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
        'imars_regions.fgbnms.roi.east.chlor_a', FIELDS
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/OC/STET_OC_ts_FGB.csv',
        'imars_regions.fgbnms.roi.stet.chlor_a', FIELDS
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/OC/WestFG_OC_ts_FGB.csv',
        'imars_regions.fgbnms.roi.west.chlor_a', FIELDS
    ),
    # === sst fgbnms
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/SST/EastFG_SST_ts_FGB.csv',
        'imars_regions.fgbnms.roi.east.sst', FIELDS
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/SST/STET_SST_ts_FGB.csv',
        'imars_regions.fgbnms.roi.stet.sst', FIELDS
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/SST/WestFG_SST_ts_FGB.csv',
        'imars_regions.fgbnms.roi.west.sst', FIELDS
    ),
    # === rivers near fgbnms
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/DISCH/BrazosRv_all.csv',
        'imars_regions.fgbnms.river.brazos.disch', FIELDS
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/DISCH/MissRv_all.csv',
        'imars_regions.fgbnms.river.mississippi.disch', FIELDS
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/DISCH/NechesRv_all.csv',
        'imars_regions.fgbnms.river.neches.disch', FIELDS
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/DISCH/SabineRv_all.csv',
        'imars_regions.fgbnms.river.sabine.disch', FIELDS
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/DISCH/TrinityRv_all.csv',
        'imars_regions.fgbnms.river.trinity.disch', FIELDS
    ),
]


def main():
    for args in CSV2GRAPH_ARGS:
        print("pushing csv into graphite:")
        print(args)
        csv2graph.main(*args)


if __name__ == "__main__":
    main()
