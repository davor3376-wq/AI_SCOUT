import unittest
import numpy as np
from app.analytics.indices import calculate_nbr, calculate_ndwi

class TestIndices(unittest.TestCase):
    def test_nbr_calculation(self):
        # NBR = (NIR - SWIR) / (NIR + SWIR)
        nir = np.array([0.8, 0.1], dtype=np.float32)
        swir = np.array([0.2, 0.8], dtype=np.float32)

        # Case 1: Vegetation (High NIR, Low SWIR) -> (0.8 - 0.2) / (1.0) = 0.6
        # Case 2: Burned (Low NIR, High SWIR) -> (0.1 - 0.8) / (0.9) = -0.777

        expected = np.array([0.6, -0.7777778], dtype=np.float32)
        result = calculate_nbr(nir, swir)

        np.testing.assert_allclose(result, expected, rtol=1e-5)

    def test_ndwi_calculation(self):
        # NDWI = (Green - NIR) / (Green + NIR)
        green = np.array([0.6, 0.1], dtype=np.float32)
        nir = np.array([0.2, 0.8], dtype=np.float32)

        # Case 1: Water (High Green, Low NIR) -> (0.6 - 0.2) / (0.8) = 0.5
        # Case 2: Vegetation (Low Green, High NIR) -> (0.1 - 0.8) / (0.9) = -0.777

        expected = np.array([0.5, -0.7777778], dtype=np.float32)
        result = calculate_ndwi(green, nir)

        np.testing.assert_allclose(result, expected, rtol=1e-5)

    def test_division_by_zero(self):
        nir = np.array([0.0], dtype=np.float32)
        swir = np.array([0.0], dtype=np.float32)

        result = calculate_nbr(nir, swir)
        self.assertTrue(np.isnan(result[0]))

if __name__ == '__main__':
    unittest.main()
