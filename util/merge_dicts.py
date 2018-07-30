def merge_dicts(x, y):
    """
    Given two dicts, merge them into a new dict as a shallow copy.
    see https://stackoverflow.com/a/26853961/1483986
    """
    z = x.copy()
    z.update(y)
    return z
