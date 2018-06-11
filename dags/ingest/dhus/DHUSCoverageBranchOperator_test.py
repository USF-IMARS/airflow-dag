# std modules:
from unittest import TestCase
try:
    # py2
    from mock import MagicMock
except ImportError:
    # py3
    from unittest.mock import MagicMock

from .DHUSCoverageBranchOperator import dhus_coverage_check


class Test_dhus_coverage_check(TestCase):

    # tests:
    #########################
    def test_check_coverage_s3(self):
        """
        check coverage using dhus s3
        """
        dhus_search_kwargs = {
            'echo_collection_id': 'C1370679936-OB_DAAC',
        }
        result = dhus_coverage_check({})
        print(result)
        self.assertEqual(
            len(result[0]),
            15
        )
