def list_to_sql_or(column_name, opt_list):
    """
    makes an SQL or query for given list of options

    example:
    list_to_sql_or('my_column', [1,2,10,3])
    >>> "( my_column=1 OR my_column=2 OR my_column=10 OR my_column=3)"

    NOTE: wait... can't we just do `WHERE `choices` IN (1,2,10,3)`, ?
    """
    return (
        "(" +
        "".join([
            " OR {}={}".format(column_name, pid) for pid in opt_list
        ]) +
        " )"
    ).replace(  # rm the first "OR"
        "( OR {}=".format(column_name),
        "( {}=".format(column_name)
    )
