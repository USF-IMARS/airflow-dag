from math import sqrt

from osgeo import gdal
from osgeo import osr
import numpy as np
import pandas as pd


def read_bands_at(fpath, points, longformat=False):
    """
    read image at fpath and return bands.

    parameters
    ----------
    points: list of tuples
        [(lat, lon), (lat, lon)] points to extract band values

    returns
    -------
    band_values : list of lists
        [[band1, band2], [band1, band2]] band values for each point requested
        list returned is in same order as points.
    """
    # === open the file & get the pixel values at this pixel
    ds = gdal.Open(fpath)
    assert ds is not None
    # get georeference info
    # === assert transform is null?
    # print("pre-calc-geo:")
    # print(ds.GetGeoTransform())
    # assert ds.GetGeoTransform() == [0.0, 1.0, 0.0, 0.0, 0.0, 1.0]
    src_s_ref = osr.SpatialReference(wkt=ds.GetProjection())
    if (
        src_s_ref.IsProjected() == 0 and
        src_s_ref.GetAttrValue('geogcs') is None and
        src_s_ref.ExportToWkt() == ''
    ):
        _calibrate_ntf_w_gcps(ds)
    # print("post-calc-geo:")
    # print(ds.GetGeoTransform())
    # assert ds.GetGeoTransform() == [
    #     p1.GCPX,
    #     x_res,
    #     x_skew,
    #     p1.GCPY,
    #     y_skew,
    #     y_res,
    # ]
    if longformat:
        return _read_bands_to_pandas_dataframe(ds, points)
    else:  # wide format
        return _read_bands_to_numpy_array(ds, points)


def _read_bands_to_pandas_dataframe(ds, points):
    """
    Returns "long-format" pandas dataframe.
    """
    res = pd.DataFrame(
        columns=['band_n', 'pixel_value', 'x', 'y']
    )
    pts_in_bounds = 0
    pts_out_bounds = 0
    for band_n in range(ds.RasterCount):
        band = ds.GetRasterBand(band_n+1)  # 1-based index
        data = band.ReadAsArray()
        for point_n, point in enumerate(points):
            x = point[0]
            y = point[1]
            xOffset, yOffset = _get_offsets(ds, x, y)
            try:
                value = data[yOffset][xOffset]
                # print("band {} {},{} (point#{})= {}".format(
                #     band_n, xOffset, yOffset, point_n, value
                # ))
                res = res.append({
                    'band_n': band_n,
                    'pixel_value': value,
                    'x': x,
                    'y': y
                }, ignore_index=True)
                pts_in_bounds += 1
            except IndexError:
                # print("point {},{} is out-of-image".format(yOffset, xOffset))
                pts_out_bounds += 1
    # should be multiple of # of bands
    pts_in_bounds /= ds.RasterCount
    pts_out_bounds /= ds.RasterCount
    print("{:0.1f}% coverage ({} in-image {} out-of-image)".format(
        100*pts_in_bounds/(pts_out_bounds+pts_in_bounds),
        pts_in_bounds,
        pts_out_bounds
    ))
    return res


def _get_offsets(ds, x, y):
    """returns index for x,y locations in ds raster bands"""
    (
        xOrigin, pixelWidth, xskew, yOrigin, yskew, pixelHeight
    ) = ds.GetGeoTransform()
    xOffset = int((x - xOrigin) / pixelWidth)
    yOffset = int((y - yOrigin) / pixelHeight)
    return xOffset, yOffset


def _read_bands_to_numpy_array(ds, points):
    band_values = np.array([[float('nan')]*ds.RasterCount]*len(points))
    for band_n in range(ds.RasterCount):
        band = ds.GetRasterBand(band_n+1)  # 1-based index
        data = band.ReadAsArray()
        # loop through the coordinates
        for point_n, point in enumerate(points):
            x = point[0]
            y = point[1]
            # print("reading @ {},{}".format(x, y))
            xOffset, yOffset = _get_offsets(ds, x, y)
            # get individual pixel values
            try:
                value = data[yOffset][xOffset]
                # print("band {} {},{} (point#{})= {}".format(
                #     band_n, xOffset, yOffset, point_n, value
                # ))
                band_values[point_n, band_n] = value
            except IndexError:
                # print("point {},{} is out-of-image".format(yOffset, xOffset))
                pass

    return band_values


def _calibrate_ntf_w_gcps(ds):
    gcps = ds.GetGCPs()
    # assert that this:
    # gcp_transform = osr.CoordinateTransformation(gcp_s_ref, src_s_ref)
    # throws :
    # ERROR 6: No translation for an empty SRS to PROJ.4 format is known.
    # === set up the transform manually using ground control points
    # gcp_s_ref = osr.SpatialReference(wkt=ds.GetGCPProjection())

    # assume control points are corners
    assert len(gcps) == 4
    assert gcps[0].Id == 'UpperLeft'
    p1 = gcps[0]
    assert gcps[2].Id == 'LowerRight'
    p2 = gcps[2]
    # x_0 = y_0 = z_0 = 0
    # x_f = y_f = z_f = 0
    # for gcp in control_points:
    #     print("{}=({},{},{})".format(gcp.Id, gcp.GCPX, gcp.GCPY, gcp.GCPZ))
    #     x_0 = min(gcp.GCPX, x_0)
    #     y_0 = min(gcp.GCPY, y_0)
    #     z_0 = min(gcp.GCPZ, z_0)
    #     x_f = max(gcp.GCPX, x_f)
    #     y_f = max(gcp.GCPY, y_f)
    #     z_f = max(gcp.GCPZ, z_f)
    #     px_x = gcp.GCPPixel
    #     px_y = gcp.GCPLine
    # x_range = x_f - x_0
    # y_range = y_f - y_0
    # x_res = x_range / ds.RasterXSize
    # y_res = y_range / ds.RasterYSize
    # x_skew = sqrt(x_range**2 + y_range**2) / y_res  # geo_distance / ypixel
    # y_skew = sqrt(x_range**2 + y_range**2) / x_res  # geo_distance / xpixel
    # https://stackoverflow.com/questions/27166739/description-of-parameters-of-gdal-setgeotransform
    x_skew = (
        sqrt((p1.GCPX-p2.GCPX)**2 + (p1.GCPY-p2.GCPY)**2) /
        (p1.GCPLine - p2.GCPLine)
    )
    y_skew = (
        sqrt((p1.GCPX-p2.GCPX)**2 + (p1.GCPY-p2.GCPY)**2) /
        (p1.GCPPixel - p2.GCPPixel)
    )
    x_res = (p2.GCPX - p1.GCPX) / ds.RasterXSize
    y_res = (p2.GCPY - p1.GCPY) / ds.RasterYSize
    ds.SetGeoTransform([
        p1.GCPX,
        x_res,
        x_skew,
        p1.GCPY,
        y_skew,
        y_res,
    ])
    print("@ {},{} res [{}x{}]".format(
        p1.GCPX, p1.GCPY, x_res, y_res
    ))
