import imars_etl


class Area(object):
    """
    Usage:
    ```
    from regions import REGIONS
    gulf_area = ROI("gom")

    print(gulf_area.short_name)
    print(gulf_area.id)
    ```
    """
    def __init__(self, short_name):
        self.short_name = short_name
        self.id = imars_etl.id_lookup(short_name, "area")

    def __getitem__(self, n):
        """For backwards-compat w/ older scripts"""
        if n == 0:
            return self.id
        elif n == 1:
            return self.short_name
        else:
            raise IndexError("unsupported Area indexing")
