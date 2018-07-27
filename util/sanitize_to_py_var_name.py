import re


def sanitize_to_py_var_name(orig_str):
    """
    Sanitizes given string into valid python variable
    name. Based on https://stackoverflow.com/a/3303361/1483986
    """
    # Remove invalid characters
    new_str = re.sub('[^0-9a-zA-Z_]', '_', orig_str)

    # cannot start w/ number
    if new_str[0].isdigit():
        new_str = "_" + new_str

    return new_str
