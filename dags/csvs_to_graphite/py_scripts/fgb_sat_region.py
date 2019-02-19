from sat_region_station_ts import csv2graph_roi

csv2graph_roi(
    'FGB',
    [
        'SS1', 'SS2', 'SS3', 'SS4', 'SS5', 'SS6', 'SS7', 'SS8',
        'STET', 'WFG', 'EFG'
    ],
    '/srv/imars-objects/modis_aqua_fgbnms'
)
