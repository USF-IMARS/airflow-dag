import os.path
import socket
import subprocess


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
