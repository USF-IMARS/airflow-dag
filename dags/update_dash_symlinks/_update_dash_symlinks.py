"""
Updates {N}_weeks_ago.png symlinks so dashboards can static link to these
locations.

Uses IMaRS metadata db to get filepath to the latest files, then puts symlinks
in the same directory as the latest files.
"""
import os

import imars_etl

N_HISTORICAL_WEEKS = 5


def main():
    for pid in [43, 44, 45, 46]:  # product IDs
        for weeks_ago in range(0, N_HISTORICAL_WEEKS):
            src_path = imars_etl.select(
                columns='filepath',
                sql=(
                    'product_id={} ORDER BY date_time DESC '.format(pid) +
                    ' LIMIT 1 OFFSET {}'.format(weeks_ago)
                )
            )['filepath']
            dest_path = os.path.join(
                os.path.dirname(src_path),
                "{}_weeks_ago.png".format(weeks_ago)
            )

            if os.path.exists(dest_path):  # clear existing link if exists
                assert os.path.islink(dest_path)
                os.remove(dest_path)

            os.symlink(src_path, dest_path)


if __name__ == "__main__":
    main()
