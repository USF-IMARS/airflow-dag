def mxd03(
    product_datetime,
    sat_char
):
    """ builds a filename for M*D03.YYDDDHHMMSS.hdf formatted paths.
    These are level 1 GEO files for modis.

    Parameters
    -----------------
    sat_char : char
        Y for Aqua, O for Terra
    root_path : str filepath
        path in which all files live
    """
    return "M{}D03.{}.hdf".format(
        sat_char,
        product_datetime.strftime("%y%j%H%M%S")
    )
