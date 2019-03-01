from imars_dags.dags.processing.wv2_classification.scripts.add_img_points_to_csv \
    import add_img_points_to_csv

from datetime import datetime

add_img_points_to_csv(
    datetime(2016, 10, 26, 15, 6, 44),
    {'product_id': 37}
)
