"""
Reads each csv file and pushes the data into graphite.
"""
import _csv_to_graphite as csv2graph

CSV2GRAPH_ARGS = [  # TODO: fix duplication here & in dag
    # === oc fgbnms
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/OC/EastFG_OC_ts_FGB.csv',
        'imars_regions.east_fgbnms.chlor_a',
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/OC/STET_OC_ts_FGB.csv',
        'imars_regions.stet_fgbnms.chlor_a',
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/OC/WestFG_OC_ts_FGB.csv',
        'imars_regions.west_fgbnms.chlor_a',
    ),
    # === sst fgbnms
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/SST/EastFG_SST_ts_FGB.csv',
        'imars_regions.east_fgbnms.sst',
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/SST/STET_SST_ts_FGB.csv',
        'imars_regions.stet_fgbnms.sst',
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/SST/WestFG_SST_ts_FGB.csv',
        'imars_regions.west_fgbnms.sst',
    ),
    # === rivers near fgbnms
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/DISCH/BrazosRv_all.csv',
        'imars_regions.brazos.disch',
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/DISCH/BrazosRv_all.csv',
        'imars_regions.brazos_river.disch',
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/DISCH/MissRv_all.csv',
        'imars_regions.mississippi_river.disch',
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/DISCH/NechesRv_all.csv',
        'imars_regions.neches_river.disch',
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/DISCH/SabineRv_all.csv',
        'imars_regions.sabine_river.disch',
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/DISCH/TrinityRv_all.csv',
        'imars_regions.trinity_river.disch',
    ),
]


def main():
    for args in CSV2GRAPH_ARGS:
        print("pushing csv into graphite:")
        print(args)
        csv2graph.main(*args)


if __name__ == "__main__":
    main()
