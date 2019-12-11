# std modules:
from unittest import TestCase

from .list_to_sql_or import list_to_sql_or


class Test_sql_or_test(TestCase):

    # tests:
    #########################
    # === python API
    def test_list_to_sql_or(self):
        """
        basic example test of list_to_sql_or
        """
        self.assertEqual(
            list_to_sql_or("test_column", ['test_val', 1, 3, '%']),
            (
                "( test_column=test_val OR "
                "test_column=1 OR test_column=3 OR "
                "test_column=% )"
            )
        )
