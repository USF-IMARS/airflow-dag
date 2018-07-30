"""
Reads each csv file and pushes the data into graphite.
"""
import _csv_to_graphite as csv2graph

CSV2GRAPH_ARGS = [
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/OC/EastFG_OC_ts_FGB.csv',
        'east_fgb',
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/OC/STET_OC_ts_FGB.csv',
        'stet_fgb',
    ),
    (
        '/srv/imars-objects/modis_aqua_fgbnms/CSVTS/OC/WestFG_OC_ts_FGB.csv',
        'west_fgb',
    ),
]


def main():
    for args in CSV2GRAPH_ARGS:
        csv2graph.main(args)


if __name__ == "__main__":
    main()
