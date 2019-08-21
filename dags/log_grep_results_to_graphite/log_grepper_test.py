# std modules:
from unittest import TestCase
from unittest.mock import patch

# from imars_dags.dags.log_grep_results_to_graphite.log_grepper \
#     import get_grepped_log_counts


# class Test_get_grepped_log_counts(TestCase):
#
#     def test_get_grepped_log_counts(self):
#         """
#         basic example test of get_grepped_log_counts
#         """
#         self.assertEqual(
#             get_grepped_log_counts([["test_key", "sys"]], ".*py"),
#             False
#         )


class Test_get_logifiles(TestCase):

    @patch('glob.glob')
    def test_basic(self, mock_glob):
        """
        basic example test of get_logfiles
        """
        base_path = "/srv/imars-objects/airflow_tmp/logs"
        list_of_logs = [
            (
                base_path + '/fake_dag_id_1/fake_task_id_1' +
                '/2018-09-30T16:18:30.746650+00:00/1.log'
            ),
            (
                base_path + '/fake_dag_id_1/fake_task_id_1/' +
                '2018-09-30T16:18:30.746650+00:00/2.log'
            )
        ]
        mock_glob.side_effect = [
            list_of_logs
        ]
        from imars_dags.dags.log_grep_results_to_graphite.log_grepper \
            import get_logfiles

        self.assertEqual(
            list(get_logfiles(
                base_path,
                "*",
                "*"
            )),
            list_of_logs
        )
