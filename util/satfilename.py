"""
This file defines various functions to build filepaths in a standard way.
Some general rules/assumptions that summarize behavior here:

### directory structure:
* the root data directory is split up by region
* each "region subdirectory" contains a common list of product directories
* the "product subdirectories" should contain only one type of product (and one filetype). Products from different sources, satellites, or processing methods should not share a product directory unless the products are identical. "Products" made up of multiple filetypes must be split into multiple directories.
* no directory structure should exist beyond the "product subdirectories"

### defining "product"
* a "product" in this context is a group of files that have all metadata (processing/source provenence, region, etc) in common except for their datetime (and metadata affected by different datetime like satellite location or actual bounding box).
* different versions of products (ie if geo files are being generated in a new way) should be separated out into a new product directory, not lumped in with the older product. Appending `_v2` or a more descriptive name to the end of the product directory as needed. If the new product version *really* wants to include the older files, sym-links should be created to link to the older version's files (eg `ln -s ./2017-02-13_v2.GEO ../geo/2017-02-13.GEO`).
* empty directories should be deleted and created only when needed.

### filenames:
* filenames within a product directory should all conform to the same pattern
* filenames should include:
    - the datetime of the product (preferably in ISO 8601 format)
    - something to identify the "product type"
* the datetime should be the first part of the filename

### Example directory structure:
```
/root-data-directory
    /region1
        /myd01
        /mod01
        /myd03
        /m0d03
        /geo
        /geo_v2
    /region2
        /myd01
        /myd03
        /geo
        /geo_v2
```
Note the common product directories and the two `geo` directories where a new version was separated out into a new product. Filenames in `geo` and `geo_v2` are probably similar, but shoud not be identical.
"""
ISO_8601_FMT="%Y-%m-%dT%H:%M:%SZ"

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

def myd01(product_datetime):
    """ modis aqua l1.
        I *think* these files are the same as l1a_LAC, but from LANCE.
    """
    return (
        "/srv/imars-objects/modis_aqua_gom/myd01/" +
        product_datetime.strftime("A%Y%j.%H%M.hdf")
    )

def l1a_geo(
    product_datetime
):
    return "/srv/imars-objects/modis_aqua_gom/geo/" + "A{}00.GEO".format(
        product_datetime.strftime("%Y%j%H%M")
    )

def okm(product_datetime):
    return "/srv/imars-objects/modis_aqua_gom/l1b/" + "A{}00.L1B_LAC".format(
        product_datetime.strftime("%Y%j%H%M")
    )

def hkm(product_datetime):
    return "/srv/imars-objects/modis_aqua_gom/hkm/" + "A{}00.L1B_HKM".format(
        product_datetime.strftime("%Y%j%H%M")
    )
def qkm(product_datetime):
    return "/srv/imars-objects/modis_aqua_gom/qkm/" + "A{}00.L1B_QKM".format(
        product_datetime.strftime("%Y%j%H%M")
    )

l2_basepath="/srv/imars-objects/modis_aqua_gom/l2/"
l2_filename_fmt="A%Y%j%H%M00.L2"
def l2(product_datetime):
    return l2_basepath + product_datetime.strftime(l2_filename_fmt)

def l3(prod_dat):
    return "/srv/imars-objects/modis_aqua_gom/l3/" + prod_dat.strftime(ISO_8601_FMT) + "_l3.nc"

def png(product_datetime, region_name):
    return "/srv/imars-objects/modis_aqua_gom/png/" + region_name + "_" + str(product_datetime) + ".png"

def metadata(prod_datetime):
    """ path to flat-file metadata key-value store """
    return (
        "/srv/imars-objects/modis_aqua_gom/metadata-ini/metadata_" +
        prod_datetime.strftime(ISO_8601_FMT) + ".ini"
    )


# === old & unused pathers:
def l1a_LAC_bz2(
    product_datetime
):
    """ DEPRECATED : myd01 should be used instead!
    Gets file path for 1a aqua modis files zipped together from OB.DAAC.
    """
    base_path="/srv/imars-objects/homes/scratch/epa/satellite/modis/GOM/L2G_MODA_sub1973_day/"
    return base_path+"A{}00.L1A_LAC.bz2".format(
        product_datetime.strftime("%Y%j%H%M")
    )

def l1a_LAC(
    product_datetime
):
    """ DEPRECATED : myd01 should be used instead!
        returns file path for unzipped modis aqua files (see also l1a_LAC_bz2)
        I *think* these are myd01 files, same as myd01(), but from PO.DAAC.
    """
    return "/srv/imars-objects/modis_aqua_gom/l1a/" + "A{}00.L1A_LAC".format(
        product_datetime.strftime("%Y%j%H%M")
    )
