# std modules:
from unittest import TestCase
import pytest

from imars_dags.dag_classes.ingest.dhus.dhus_coverage_check \
    import dhus_coverage_check


class Test_dhus_coverage_check(TestCase):

    @pytest.mark.skip(reason="relies on downloading real data")
    def test_check_coverage_s3(self):
        """
        check coverage using dhus s3
        """
        dhus_search_kwargs = {
            'echo_collection_id': 'C1370679936-OB_DAAC',
        }
        result = dhus_coverage_check(dhus_search_kwargs)
        print(result)
        self.assertEqual(
            len(result),
            1
        )
