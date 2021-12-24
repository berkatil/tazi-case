import pandas as pd
import unittest
from main import calculate_matrix_values

class TestCalculation(unittest.TestCase):
    def setUp(self):
        self.df = pd.read_csv('test_mock.csv')
    def test_upper(self):
        tp,fn,tn,fp = calculate_matrix_values(self.df)
        self.assertEqual(tp,3)
        self.assertEqual(fn,1)
        self.assertEqual(tn,5)
        self.assertEqual(fp,1)