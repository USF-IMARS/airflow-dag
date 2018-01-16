"""
This file defines various functions to build filepaths in a standard way.
Some general rules/assumptions that summarize behavior here:

### directory structure:
* the root data directory is split up by region
* each "region subdirectory" contains a common list of product directories
* the "product subdirectories" should contain only one type of product (and one filetype).
    - Products from different sources, satellites, or processing methods should not share a product directory unless the products are identical.
    - "Products" made up of multiple filetypes must be split into multiple directories.
* no directory structure should exist beyond the "product subdirectories"

### defining "product"
* a "product" in this context is a group of files that have all metadata (processing/source provenence, region, etc) in common except for their datetime (and metadata affected by different datetime like satellite location or actual bounding box).
* different versions of products (ie if geo files are being generated in a new way) should be separated out into a new product directory, not lumped in with the older product.
    - Appending `_v2` or a more descriptive name to the end of the product directory as needed.
    - If the new product version *really* wants to include the older files, sym-links should be created to link to the older version's files (eg `ln -s ./2017-02-13_v2.GEO ../geo/2017-02-13.GEO`).
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
        /geo_v2
```
Note the common product directories and the two `geo` directories where a new version was separated out into a new product.
Filenames in `geo` and `geo_v2` are probably similar, but shoud not be identical.
"""
ISO_8601_FMT="%Y-%m-%dT%H:%M:%SZ"

class BaseSatFilepath():
    """
    Base constructor for saffilename pathing function.
    Instantiate with the filename format string and product_id.
    Calls to instantiations of this class will return the path.
    """
    def __init__(self, product_id, filename_fmt):
        """
            Parameters
            --------------
            filename_fmt : str
                Filename format string.
                Supports formatters from datetime.strftime.
                Example: "%Y-%m-%dT%H:%M:%SZ_my_product.png"
            product_id : str
                Unique string to identify the product.
                Determines which subdir to use within imars-objects/region/.
        """
        self.filename_fmt = filename_fmt
        self.product_id=product_id

    def basepath(self, region):
        """ returns the directory these products are placed in. """
        return "/srv/imars-objects/modis_aqua_{}/{}/".format(region, self.product_id)

    def __call__(self, exec_time, region_id):
        """
        Parameters
        ------------
        exec_time : datetime.datetime
            execution_date for the product (eg the start_time of the granule)
        region_id : str
            region.place_name from regions.<region_name> used to create path
            within imars-objects.
        """
        return self.basepath(region_id) + exec_time.strftime(self.filename_fmt)

""" zipped l1a (myd01) files from OB.DAAC """
l1a_lac_hdf_bz2 = BaseSatFilepath("l1a_lac_hdf_bz2", "A%Y%j%H%M00.L1A_LAC.x.hdf.bz2")

""" modis aqua l1.
    I *think* these files are the same as l1a_LAC, but from LANCE.
"""
myd01 = BaseSatFilepath("myd01", "A%Y%j.%H%M.hdf")

l1a_geo = BaseSatFilepath("geo", "A%Y%j%H%M00.GEO")

okm = BaseSatFilepath("l1b", "A%Y%j%H%M00.L1B_LAC")

hkm = BaseSatFilepath("hkm", "A%Y%j%H%M00.L1B_HKM")

qkm = BaseSatFilepath("qkm", "A%Y%j%H%M00.L1B_QKM")

l2 = BaseSatFilepath("l2", "A%Y%j%H%M00.L2")

l3 = BaseSatFilepath("l3", ISO_8601_FMT+"_l3.nc")

l3_pass = BaseSatFilepath("l3_pass", ISO_8601_FMT+"_l3.nc")

def png(product_datetime, variable_name, region_id):
    """
    Returns path for png product with given attributes.
    Although this is similar to a BaseSatFilepath, we don't use it here because
    we also need to use the variable name to formulate base_path & filename.
    """
    return (
        "/srv/imars-objects/modis_aqua_" + region_id + "/png_" + variable_name + "/" +
        product_datetime.strftime(ISO_8601_FMT) +
        "_" + variable_name + ".png"
    )

metadata = BaseSatFilepath("metadata-ini", "metadata_"+ISO_8601_FMT+".ini")
