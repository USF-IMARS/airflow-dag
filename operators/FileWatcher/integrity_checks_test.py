# std modules:
from unittest import TestCase
from unittest.mock import patch

from imars_dags.operators.FileWatcher.integrity_checks \
    import synonymous_gdalinfo


class Test_synonymous_gdalinfo(TestCase):
    @patch('imars_dags.operators.FileWatcher.integrity_checks.gdalinfo')
    def test_identical_return_true(self, mock_gdalinfo):
        """ returns true on two identical gdalinfo output """
        mock_gdalinfo.return_value = 'a string w/ multiple\n    lines'
        self.assertEqual(synonymous_gdalinfo('f1', 'f2'), True)

    @patch('imars_dags.operators.FileWatcher.integrity_checks.gdalinfo')
    def test_synonyms_return_true(self, mock_gdalinfo):
        """ returns true on two synonyms """
        mock_gdalinfo.side_effect = [(
            'NITF_FDT=20190207231221\n'
            'NITF_FTITLE=17MAY12163422-M1BS-059145537010_01_P010.NTF\n'
            'NITF_IID2=12MAY17WV021200017MAY12163422-M1BS-059145537010_01'
        ), (
            'NITF_FDT=20190103163752\n'
            'NITF_FTITLE=17MAY12163422-M1BS-058943203010_01_P010.NTF\n'
            'NITF_IID2=12MAY17WV021200017MAY12163422-M1BS-058943203010_01_'
        )]
        self.assertEqual(synonymous_gdalinfo('f1', 'f2'), True)

    @patch('imars_dags.operators.FileWatcher.integrity_checks.gdalinfo')
    def test_not_synonyms_return_false(self, mock_gdalinfo):
        """ returns false on two non-synonyms """
        mock_gdalinfo.side_effect = [
            'a string w/ multiple\n    lines',
            'a different string w/ multiple\n    lines'
        ]
        self.assertEqual(synonymous_gdalinfo('f1', 'f2'), False)
