import unittest
import xml_pro

class TestXMLPro(unittest.TestCase):
    """Unit test for all the functions."""
    def test_parse_xml(self):
        '''
        test parse_xml to check if the returned value is instance or not
        '''
        xml_file = "select.xml"
        returned_value = xml_pro.parse_xml(xml_file)
        self.assertIsInstance(returned_value, object)

    def test_get_req_data(self):
        """
        test get_req_data to check if returned value is the required list
        """
        xml_file = "select.xml"
        returned_value = xml_pro.parse_xml(xml_file)
        returened_list = xml_pro.get_req_data(returned_value)
        self.assertEqual(returened_list[0]['file_type'],"DLTINS")
        