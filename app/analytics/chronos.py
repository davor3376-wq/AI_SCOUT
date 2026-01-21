"""
Chronos: Change Detection Module.
Calculates the difference between two raster files (T2 - T1).
"""
import os
import logging
import rasterio
import numpy as np

logger = logging.getLogger("Chronos")

OUTPUT_DIR = "data/processed"

def calculate_change(current_path: str, previous_path: str) -> str:
    """
    Calculates the difference between the current image and the previous image.
    Assumes both are single-band float32 (e.g., NDVI).
    Returns the path to the difference map.
    """
    if not os.path.exists(current_path) or not os.path.exists(previous_path):
        logger.error(f"Missing input files for change detection: {current_path}, {previous_path}")
        return None

    try:
        with rasterio.open(current_path) as src_curr, rasterio.open(previous_path) as src_prev:
            # Check for compatibility
            if src_curr.shape != src_prev.shape:
                logger.warning("Shape mismatch between current and previous files. Skipping change detection.")
                # Future: Implement reprojection/resampling here.
                return None

            curr_data = src_curr.read(1)
            prev_data = src_prev.read(1)

            # Calculate difference: Current - Previous
            # Positive = Increase (Regrowth)
            # Negative = Decrease (Deforestation/Stress)
            diff = curr_data - prev_data

            # Generate filename
            curr_name = os.path.basename(current_path)
            date_str = curr_name.split("_")[0]
            output_filename = f"{date_str}_CHANGE_analysis.tif"
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            output_path = os.path.join(OUTPUT_DIR, output_filename)

            # Save
            profile = src_curr.profile.copy()
            profile.update(dtype=rasterio.float32)

            with rasterio.open(output_path, 'w', **profile) as dst:
                dst.write(diff.astype(rasterio.float32), 1)

            logger.info(f"Change detection complete: {output_path}")
            return output_path

    except Exception as e:
        logger.error(f"Change detection failed: {e}")
        return None
