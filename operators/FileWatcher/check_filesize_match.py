from os import stat


def check_filesize_match(f_meta):
    """ verify filesize matches size in DB """
    NONE_VALUES = [None, 'None', "NA", ""]
    try:
        db_size = f_meta['n_bytes']
        f_size = stat(f_meta['filepath']).st_size
        DIFF_THRESHOLD = 0.05 * int(db_size)  # assume +/- 5%
        if db_size in NONE_VALUES or f_size in NONE_VALUES:
            print(
                (
                    "Invalid file size. DB:'{}', F:'{}';"
                    " skipping filesize check."
                ).format(
                    db_size, f_size
                )
            )
        elif abs(int(db_size) - int(f_size)) > DIFF_THRESHOLD:
            raise RuntimeError(
                "file size in database does not match actual "
                " '{}'!='{}' ".format(db_size, f_size)
            )
            # TODO: do something about this.
        else:
            print("File size verified.")
            return
    except (TypeError, ValueError):
        print(
            (
                "Invalid file size. DB:'{}', F:'{}';"
                " skipping filesize check."
            ).format(
                db_size, f_size
            )
        )
