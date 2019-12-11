import os


def check_locally_accessible(file_meta):
    # ensure accessible at local
    assert os.path.isfile(file_meta['filepath'])
