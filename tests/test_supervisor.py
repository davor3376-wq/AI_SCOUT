import unittest
from unittest.mock import MagicMock, patch, AsyncMock
import os
import numpy as np
from supervisor import MissionSupervisor
from sentinelhub import BBox, CRS

class TestSupervisor(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.sup = MissionSupervisor()
        self.bbox = BBox(bbox=[16.2, 48.1, 16.5, 48.3], crs=CRS.WGS84)

    @patch('os.path.exists', return_value=True)
    @patch('rasterio.open')
    def test_quality_gate_success(self, mock_open, mock_exists):
        # Setup mock to return valid data (some vegetation)
        mock_ds = MagicMock()
        mock_ds.read.return_value = np.array([[0.5, 0.6], [0.7, 0.8]])
        mock_open.return_value.__enter__.return_value = mock_ds

        # Fake file path
        result = self.sup.quality_gate(["data/processed/test_NDVI.tif"])
        self.assertTrue(result)

    @patch('os.path.exists', return_value=True)
    @patch('rasterio.open')
    def test_quality_gate_failure_nan(self, mock_open, mock_exists):
        # Setup mock to return all NaNs
        mock_ds = MagicMock()
        mock_ds.read.return_value = np.full((2, 2), np.nan)
        mock_open.return_value.__enter__.return_value = mock_ds

        result = self.sup.quality_gate(["data/processed/test_NDVI.tif"])
        self.assertFalse(result)

    @patch('os.path.exists', return_value=True)
    @patch('rasterio.open')
    def test_quality_gate_failure_zeros(self, mock_open, mock_exists):
        # Setup mock to return all Zeros
        mock_ds = MagicMock()
        mock_ds.read.return_value = np.zeros((2, 2))
        mock_open.return_value.__enter__.return_value = mock_ds

        result = self.sup.quality_gate(["data/processed/test_NDVI.tif"])
        self.assertFalse(result)

    @patch('main.main', new_callable=AsyncMock)
    @patch('supervisor.MissionSupervisor.quality_gate')
    async def test_execute_mission_retry_success(self, mock_qg, mock_main):
        # Simulation:
        # 1. First call to main succeeds (returns files) but Quality Gate fails.
        # 2. Retry call to main succeeds.
        # 3. Quality Gate passes on second try.

        mock_main.return_value = ["file1.tif"]

        # quality_gate side_effect: First call False, Second call True
        mock_qg.side_effect = [False, True]

        await self.sup.execute_mission(self.bbox, 1)

        # Verify main was called twice (initial + retry)
        self.assertEqual(mock_main.call_count, 2)
        # Verify mission counted as completed
        self.assertEqual(self.sup.completed_missions, 1)
        self.assertEqual(len(self.sup.failed_missions), 0)

    @patch('main.main', new_callable=AsyncMock)
    async def test_execute_mission_exception_retry(self, mock_main):
        # Simulation:
        # 1. First call raises Exception.
        # 2. Retry call succeeds.

        mock_main.side_effect = [Exception("Connection Error"), ["file1.tif"]]

        # Mock quality gate to always pass if files exist
        with patch.object(self.sup, 'quality_gate', return_value=True):
            await self.sup.execute_mission(self.bbox, 2)

        self.assertEqual(mock_main.call_count, 2)
        self.assertEqual(self.sup.completed_missions, 1)

if __name__ == '__main__':
    unittest.main()
