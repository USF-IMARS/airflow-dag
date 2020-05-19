from sat_region_station_ts import csv2graph_roi

# example file: /srv/imars-objects/fk/EXT_TS_VSNPP/SSTN/FKdbv2_sstn_TS_VSNPP_daily_BB.csv

csv2graph_roi(
    roi='fk',
    [
        'BB', 'BIS', 'CAR', 'DT', 'DTN', 'EFB', 'ED_IN', 'EK_MID', 'FLB', 'FROCK', 
        'IFB', 'KW', 'LK', 'MIA', 'MK', 'MOD', 'MQ', 'MR', 'MUK', 'PBI', 'PEV', 'SANDK', 
        'SFP10', 'SFP11', 'SFP12', 'SFP13', 'SFP14', 'SFP15_5', 'SFP15', 'SFP16', 'SFP17',
        'SFP18', 'SFP19', 'SFP1', 'SFP20', 'SFP21_5', 'SFP22_5', 'SFP22', 'SFP23', 'SFP24',
        'SFP2', 'SFP30_5', 'SFP31', 'SFP32', 'SFP33', 'SFP34', 'SFP39', 'SFP40', 'SFP41',
        'SFP42', 'SFP45', 'SFP46', 'SFP47', 'SFP48', 'SFP49', 'SFP4', 'SFP50', 'SFP51',
        'SFP52', 'SFP53', 'SFP54', 'SFP5_5', 'SFP55', 'SFP56', 'SFP57_2', 'SFP57_3',
        'SFP57', 'SFP5', 'SFP61', 'SFP62', 'SFP63', 'SFP64', 'SFP6_5', 'SFP61', 'SFP62',
        'SFP63', 'SFP64', 'SFP6_5', 'SFP65', 'SFP66', 'SFP67', 'SFP69', 'SFP6', 'SFP70',
        'SFP7', 'SFP8', 'SFP9_5', 'SPF9', 'SLI', 'SOM', 'SR', 'UFB1', 'UFB2', 'UFB4', 'UK',
        'UK_IN', 'UK_MID', 'UK_OFF', 'WFB', 'WFS', 'WS'
    ],
    '/srv/imars-objects/fk/',
    db_version="v2",
    prod_format_strings=[
        '/EXT_TS_VSNPP/SSTN/{roi_upper}dbv2_sstn',
        '/EXT_TS_VSNPP/OC/{roi_upper}dbv2_chlor_a',
        '/EXT_TS_VSNPP/OC/{roi_upper}dbv2_Rrs_671',
        '/EXT_TS_VSNPP/OC/{roi_upper}dbv2_Kd_490',
        '/EXT_TS_VSNPP/OC/{roi_upper}dbv2_ABI',
    ]
    FILENAME_FORMAT="{directory}{prod}_TS_VSNPP_{timescale}_{loc}.csv"
)
