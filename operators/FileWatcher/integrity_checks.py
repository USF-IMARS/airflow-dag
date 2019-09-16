import os.path
import os.remove
from os import stat
import socket
import subprocess
import difflib

import imars_etl

NITF_PRODUCT_IDS = [11, 24]  # TODO: add wv3 products


def ensure_locally_accessible(file_meta):
    # ensure accessible at local
    assert os.path.isfile(file_meta['filepath'])


def ensure_ipfs_accessible(file_meta):
    # ensure accessbile over IPFS
    IPFS_PATH = "/usr/local/bin/ipfs"
    fpath = file_meta['filepath']
    old_hash = file_meta["multihash"]
    # TODO: adding it everytime is probably overkill
    #       but it needs to be added, not just hashed.
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
        pid=file_meta['product_id'],  # 30
        dt=file_meta['date_time'],  # 2016-07-27T16:00:59.016650+00:00
        aid=file_meta['area_id']  # 9
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
        elif (
            _is_nitf_prod_id(file_meta['product_id']) and
            _nitf_files_are_synonyms(keepfile_path, delfile_path)
        ):
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


def _is_nitf_prod_id(prod_id):
    return prod_id in NITF_PRODUCT_IDS


def gdalinfo(filepath):
    return subprocess.check_output("gdalinfo {}".format(filepath), shell=True)


def same_filesize(filepath1, filepath2):
    """ returns true if files are the same size """
    FILE_SIZE_THRESHOLD = 1
    if stat(filepath1).st_size - stat(filepath2).st_size > FILE_SIZE_THRESHOLD:
        return False
    else:
        return True


def synonymous_gdalinfo(filepath1, filepath2):
    """ returns true if files have "synonymous" gdalinfo output """
    ACCEPTABLE_DIFFS = [
        'NITF_FDT', 'NITF_FTITLE', 'NITF_IID2'
    ]
    n = 0
    for s in difflib.ndiff([gdalinfo(filepath1)], [gdalinfo(filepath2)]):
        print("{} | {}".format(n, s))
        n += 1
        if s[0] == ' ':  # skip blank lines
            continue
        elif s[0] in ['-', '+']:
            print(s)
            # if diff line is okay...
            if any([s[1:].strip().startswith(d) for d in ACCEPTABLE_DIFFS]):
                continue
            else:
                print('ooops')
                print(s[1:])
                return False
        # other acceptable lines in the diff:
        elif any([s.startswith(d) for d in ['?']]):
            continue
        else:
            raise ValueError("undexpected diff line not starting w/ - or +")
    else:
        return True


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

    assumptions:
    ------------
    * the two given filepaths are locally accessible
    * the two given files are NITF format
    """
    # compare file sizes
    if not same_filesize(filepath1, filepath2):
        return False

    # gdalinfo the files
    if synonymous_gdalinfo(filepath1, filepath2):
        return True
    else:
        return False


def _handle_duplicate_entries(keep_path, del_path):
    """
    removes del_path entry from database and deletes the file at del_path.
    """
    print("="*80 + "\n !!! DUPLICATE !!! \n" + "v"*80)
    print("keeping:\n" + keep_path)
    print(del_path + "\nis being deleted")
    # === TODO: rm del_path entry in db
    DEL_STATUS_ID = 4  # ?
    # TODO: rm del_path entry
    raise NotImplementedError("NYI")
    imars-etl.sql
        "UPDATE file SET status_id={} WHERE filepath='{}'".format(
            DEL_STATUS_ID, del_path
        )
    # === TODO: rm file @ del_path
    print("^"*80 + "\n LOL JK; DO IT MANUALLY YOU FAT DINK. \n" + "="*80)
    # rm file @ del_path
    os.remove(del_path)
    # create symlink?
    os.symlink(keep_path, del_path)
