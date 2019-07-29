
from datetime import datetime
from unittest import TestCase
import pytest

from imars_dags.dags.processing.wv2_classification.scripts.add_img_points_to_csv \
    import add_img_points_to_csv


class Test_add_img_points_to_csv(TestCase):

    @pytest.mark.skip(reason="relies on downloading real data")
    def test_add_img_points_to_csv(self):
        add_img_points_to_csv(
            datetime(2016, 10, 26, 15, 6, 44),
            **{'product_id': 37}
        )
