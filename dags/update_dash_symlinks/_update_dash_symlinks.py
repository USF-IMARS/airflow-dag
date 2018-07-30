"""
Updates {N}_weeks_ago.png symlinks so dashboards can static link to these
locations.

Uses IMaRS metadata db to get filepath to the latest files, then puts symlinks
in the same directory as the latest files.
"""
import os

import imars_etl

N_HISTORICAL_WEEKS = 5

PRODUCTS = [
    # product_family chlor_a
    dict(
        pid=43,
        short_name='a1km_chlor_a_7d_mean_png'
    ),
    dict(
        pid=44,
        short_name='a1km_chlor_a_7d_anom_png'
    ),
    # product_family sst
    dict(
        pid=45,
        short_name='a1km_sst_7d_mean_png'
    ),
    dict(
        pid=46,
        short_name='a1km_sst_7d_anom_png'
    )
]

# TODO: iterate over multiple areas too
region_short_name = 'fgbnms'


def main():
    for product in PRODUCTS:
        pid = product['pid']
        product_short_name = product['short_name']
        for weeks_ago in range(0, N_HISTORICAL_WEEKS):
            src_path = imars_etl.select(
                columns='filepath',
                sql=(
                    'product_id={} ORDER BY date_time DESC '.format(pid) +
                    ' LIMIT 1 OFFSET {}'.format(weeks_ago)
                )
            )['filepath']
            dest_path = os.path.join(
                "/srv", "imars-objects", region_short_name, "dash_links",
                "{}_{}_weeks_ago.png".format(product_short_name, weeks_ago)
            )

            if os.path.exists(dest_path):  # clear existing link if exists
                assert os.path.islink(dest_path)
                os.remove(dest_path)

            os.symlink(src_path, dest_path)


if __name__ == "__main__":
    main()
