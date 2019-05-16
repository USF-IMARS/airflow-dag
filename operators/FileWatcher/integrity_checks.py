import os.path
import socket
import subprocess

import imars_etl


def ensure_locally_accessible(file_meta):
    # ensure accessible at local
    assert os.path.isfile(file_meta['filepath'])


def ensure_ipfs_accessible(file_meta):
    # ensure accessbile over IPFS
    # TODO: adding it everytime is probably overkill
    #       but it needs to be added, not just hashed.
    IPFS_PATH = "/usr/local/bin/ipfs"
    fpath = file_meta['filepath']
    old_hash = file_meta["multihash"]
    new_hash = subprocess.check_output(
        # "ipfs add -Q --only-hash {localpath}".format(
        "ipfs add -Q --nocopy {localpath}".format(
            ipfs=IPFS_PATH,
            localpath=fpath
        ),
        shell=True
    )
    hostname = socket.gethostname()
    if old_hash is not None and old_hash != "" and old_hash != new_hash:
        raise ValueError(
            "file hash does not match db!\n\tdb_hash:{}\n\tactual:{}"
        )
    else:
        return new_hash, hostname


def check_for_duplicates(file_meta):
    """
    Checks for duplicate entries files in the database
    and tries to resolve the conflict.

    Duplicate entries is defined as files with identical
    area_d, date_time, and product_id.

    returns
    -------
    True if duplicate is successfully removed,
    False if no duplication found.
    """
    # use the query from the airflow extract
    #   and compare the (two) results?
    sql_selection = """
        product_id={pid} AND
            date_time='{dt}' AND
            area_id={aid}
        LIMIT 2
        ORDER BY last_processed
    """.format(
        pid=file_meta['?'],  # 30
        dt=file_meta['?'],  # 2016-07-27T16:00:59.016650+00:00
        aid=file_meta['?']  # 9
    )

    result = imars_etl.select(
        cols="filepath,multihash",
        sql=sql_selection,
        first=False,
    )

    if len(result) < 1:
        raise AssertionError("query returns 0 results?")
    elif len(result) == 1:
        print("One result and all is well.")
        return False
    elif len(result) == 2:
        keepfile_meta, delfile_meta = result
        fpath_i = 0
        keepfile_path = keepfile_meta[fpath_i]
        delfile_path = delfile_meta[fpath_i]
        mhash_i = 1
        if (keepfile_meta[mhash_i] == delfile_meta[mhash_i]):
            print("duplicate entries are an exact match.")
            _handle_duplicate_entries(keepfile_path, delfile_path)
            return True
        # TODO: only perform these checks on
        elif _nitf_files_are_synonyms(keepfile_path, delfile_path):
            print("duplicate entries are NITF-synonyms")
            _handle_duplicate_entries(keepfile_path, delfile_path)
            return True
        else:
            raise ValueError(
                "duplicate entries are not identical or synonymous" +
                "\n\t{}\n\t{}".format()
            )
    else:
        raise AssertionError("query resturs >2 results?")


def _nitf_files_are_synonyms(filepath1, filepath2):
    """
    returns true if the two files have "synonymous" content.

    Because NITF files such as those from Digital Globe contain metadata
    about their own filepath duplicates cannot be identified using a simple
    hash.
    So we define "NITF-synonyms" by the criteria:
        1. files are (very nearly) the same size
        2. metadata dumped using `gdalinfo` differs only in the following vars:
            * NITF_FDT
            * NITF_FTITLE
            * NITF_IID2

    assumes two given filepaths are locally accessible
    """
    # TODO: stat files
    # TODO: gdalinfo files
    raise NotImplementedError("NYI")


def _handle_duplicate_entries(keep_path, del_path):
    """
    removes del_path entry from database and deletes the file at del_path.
    """
    # TODO: rm del_path entry
    # TODO: rm file @ del_path
    raise NotImplementedError("NYI")
