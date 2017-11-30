def mxd03(
    product_datetime,
    sat_char
):
    """ builds a file path for M*D03.YYDDDHHMMSS.hdf formatted paths.
    These are level 1 GEO files for modis.

    Parameters
    -----------------
    sat_char : char
        Y for Aqua, O for Terra
    root_path : str filepath
        path in which all files live
    """
    base_path="/srv/imars-objects/nrt-pub/data/aqua/modis/level1/"
    return base_path+"M{}D03.{}.hdf".format(
        sat_char,
        product_datetime.strftime("%y%j%H%M%S")
    )

def l1a_LAC_bz2(
    product_datetime
):
    """ Gets file path for 1a aqua modis files zipped together from OB.DAAC.
    """
    base_path="/srv/imars-objects/homes/scratch/epa/satellite/modis/GOM/L2G_MODA_sub1973_day/"
    return base_path+"A{}00.L1A_LAC.bz2".format(
        product_datetime.strftime("%Y%j%H%M")
    )

def l1a_LAC(
    product_datetime
):
    """ returns file path for unzipped modis aqua files (see also l1a_LAC_bz2)
    """
    return "/srv/imars-objects/modis_aqua_gom/l1a/" + "A{}00.L1A_LAC".format(
        product_datetime.strftime("%Y%j%H%M")
    )

def l1a_geo(
    product_datetime
):
    return "/srv/imars-objects/modis_aqua_gom/geo/" + "A{}00.GEO".format(
        product_datetime.strftime("%Y%j%H%M")
    )
