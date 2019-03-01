"""
extracts band levels for all known points in Rrs product
and ensures they are in the master csv file.
"""
import os

import numpy as np
import pandas as pd
import imars_etl

from imars_dags.dags.processing.wv2_classification.scripts.read_bands_at \
    import read_bands_at


def add_img_points_to_csv(execution_date, op_kwargs, **kwargs):
    product_id = op_kwargs['product_id']
    ISO_8601_SPACEY_FMT = "%Y-%m-%d %H:%M:%S.%f"

    print("extracting image file...")
    rrs_fpath = imars_etl.extract("date_time='{}' AND product_id={}".format(
        execution_date.strftime(ISO_8601_SPACEY_FMT),
        product_id
    ))
    # TODO: should extract this & load (update) later?
    master_csv_fpath = (
        "/srv/imars-objects/na/landclass_points_master_dataframe_rrs.csv"
    )
    # point csv file made by script at:
    # https://github.com/USF-IMARS/landcover-analysis/blob/master/landcover_classify/shp_points_to_csv.py
    points_csv_fpath = (
        os.path.join(
            os.path.dirname(os.path.realpath(__file__)),  # here
            "landclasses_master_pointlist.csv"
        )
        # "/home/airflow/dags/imars_dags/dags/processing/wv2_classification/"
        # "scripts/landclasses_master_pointlist.csv"
    )
    master_df = pd.read_csv(master_csv_fpath)
    # expected master header:
    # ,band0,band1,band2,band3,band4,band5,band6,band7,cover_class,lat,lon,date_time

    pts_df = pd.read_csv(points_csv_fpath)
    # expected pts file header: ,lat,lon,alt,cover_class
    n_pts_before = len(pts_df)

    pts_list = pts_df[['lat', 'lon']].values
    # === get band values for all points:
    print("=== reading points from image...")
    pts_bands = read_bands_at(rrs_fpath, pts_list, longformat=False)
    # pts_bands looks like [[band0,band1,band2,...,band7],...]
    # === remove nan rows
    pre_len = len(pts_bands)
    pts_bands = pts_bands[~np.isnan(pts_bands).any(axis=1)]
    print("{} valid values found (was {:2.2f}% NaNs)".format(
        len(pts_bands),
        100 - 100*len(pts_bands)/pre_len
    ))

    if len(pts_bands) == 0:
        return "no points in coverage area"

    print("=== updating df...")
    new_pts_band_df = pd.DataFrame(
        pts_bands,
        columns=['band'+str(n) for n in range(8)]  # band0, band1...band7
    )
    new_pts_band_df['date_time'] = execution_date
    # TODO: these still line up... right?
    new_pts_band_df['cover_class'] = pts_df['cover_class']
    new_pts_band_df['lat'] = pts_df['lat']
    new_pts_band_df['lon'] = pts_df['lon']

    new_df = master_df.merge(new_pts_band_df, on=list(master_df), how='outer')
    new_df.drop_duplicates(subset=['name', 'col3'], inplace=True, keep='last')

    # TODO: imars_etl.load this instead of overwrite path?
    new_df.to_csv(master_csv_fpath)
    n_pts_after = len(new_df)
    return "{} pts read; {} added".format(
        len(pts_bands),
        n_pts_after - n_pts_before
    )
