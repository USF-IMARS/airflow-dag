from imars_dags.util import satfilename


def get_list_todays_l2s_cmd(exec_date, roi):
    """
    returns an ls command that lists all l2 files using the path & file
    fmt, but replaces hour/minute with wildcard *

    Used to generate an L3 over a time period by listing files by filename and
    then piping that output into a gpt call.

    TODO: replace this with imars-etl extract method?
    """
    satfilename.l2(exec_date, roi)
    fmt_str = satfilename.l2.filename_fmt.replace(
        "%M", "*"
    ).replace(
        "%H", "*"
    )
    return (
        "ls " +
        satfilename.l2.basepath(roi) +
        exec_date.strftime(fmt_str)
    )
