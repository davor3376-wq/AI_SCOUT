"""
Analytics Processor.
Orchestrates the analysis of raw satellite imagery.
"""
import os
import glob
import logging
import rasterio
import numpy as np
from pathlib import Path

from app.analytics.indices import calculate_ndvi, calculate_ndwi, calculate_nbr
from app.analytics.masking import get_cloud_mask, calculate_cloud_coverage

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

INPUT_DIR = "data/raw"
OUTPUT_DIR = "data/processed"

def process_scene(filepath: str, requested_indices=None):
    """
    Processes a single raw Sentinel-2 TIF file.
    Calculates NDVI, NDWI, NBR if possible and requested.
    Returns a list of generated output file paths.
    """
    filename = os.path.basename(filepath)
    # Expected format: {date}_{sensor}_{tile_id}.tif
    # Example: 20231025_S2_T33UUE.tif
    parts = filename.replace(".tif", "").split("_")
    if len(parts) < 3:
        logger.warning(f"Skipping file with unexpected name format: {filename}")
        return []

    date_str = parts[0]
    sensor = parts[1]
    # tile_id = parts[2] # Unused for now

    if sensor == "S1_RTC":
        logger.info(f"Skipping analytics for S1_RTC file (Radar): {filename}")
        # Could potentially just move it to processed or do something else
        # For now, we return it as is if we want it to be part of the result set
        return [filepath]

    if sensor != "S2":
        logger.info(f"Skipping non-S2 file: {filename}")
        return []

    logger.info(f"Processing {filename}...")

    generated_files = []

    with rasterio.open(filepath) as src:
        # Check band count
        # We expect at least 3 bands: B04, B08, SCL
        if src.count < 3:
            logger.error(f"File {filename} has insufficient bands ({src.count}). Expected at least 3.")
            return []

        # Read bands
        # Note: rasterio is 1-indexed
        # Updated Ingestion Layer (s2_client.py) produces 5 bands:
        # Band 1: B04 (Red)
        # Band 2: B08 (NIR)
        # Band 3: SCL
        # Band 4: B03 (Green)
        # Band 5: B12 (SWIR)

        red = src.read(1)
        nir = src.read(2)
        scl = src.read(3)

        green = None
        swir = None

        if src.count >= 4:
            green = src.read(4)
        if src.count >= 5:
            swir = src.read(5)

        # Generate Cloud Mask
        cloud_mask = get_cloud_mask(scl)
        cloud_cover_pct = calculate_cloud_coverage(cloud_mask)
        logger.info(f"Cloud Cover: {cloud_cover_pct:.2f}%")

        if cloud_cover_pct > 20.0:
            logger.warning(f"High cloud cover detected ({cloud_cover_pct:.2f}%). Data flagged as Low Confidence.")
            # In a real system, we might update metadata here.

        # Determine indices to calculate
        # If requested_indices is None, calculate all possible
        if requested_indices is None:
            requested_indices = ["NDVI", "NDWI", "NBR"]

        results = []

        # Calculate NDVI
        if "NDVI" in requested_indices:
            logger.info("Calculating NDVI...")
            ndvi = calculate_ndvi(red, nir)
            ndvi[cloud_mask] = np.nan
            path = save_result(ndvi, src.profile, date_str, "NDVI")
            if path:
                results.append(path)

        # Calculate NDWI
        if "NDWI" in requested_indices:
            if green is not None:
                logger.info("Calculating NDWI...")
                ndwi = calculate_ndwi(green, nir)
                ndwi[cloud_mask] = np.nan
                path = save_result(ndwi, src.profile, date_str, "NDWI")
                if path:
                    results.append(path)
            else:
                logger.warning("Green band (B03) not available. Skipping NDWI calculation.")

        # Calculate NBR
        if "NBR" in requested_indices:
            if swir is not None:
                logger.info("Calculating NBR...")
                nbr = calculate_nbr(nir, swir)
                nbr[cloud_mask] = np.nan
                path = save_result(nbr, src.profile, date_str, "NBR")
                if path:
                    results.append(path)
            else:
                logger.warning("SWIR band (B12) not available. Skipping NBR calculation.")

        return results

def save_result(data: np.ndarray, profile, date_str: str, index_name: str):
    """
    Saves the calculated index to a GeoTIFF.
    Returns the path to the saved file.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Update profile for the output
    # Remove potential conflicting keys from source profile
    profile.pop('blockxsize', None)
    profile.pop('blockysize', None)

    profile.update(
        dtype=rasterio.float32,
        count=1,
        nodata=np.nan,
        driver='GTiff',
        compress='lzw',
        tiled=True,
        blockxsize=256,
        blockysize=256
    )

    output_filename = f"{date_str}_{index_name}_analysis.tif"
    output_path = os.path.join(OUTPUT_DIR, output_filename)

    try:
        with rasterio.open(output_path, 'w', **profile) as dst:
            dst.write(data.astype(rasterio.float32), 1)
        logger.info(f"Saved {index_name} to {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"Failed to save {output_path}: {e}")
        return None

def run(input_files=None, requested_indices=None):
    """
    Main entry point to process files.
    Args:
        input_files (list): Optional list of specific file paths to process.
                            If None, processes all .tif files in data/raw.
        requested_indices (list): Optional list of indices to calculate (e.g., ["NDVI", "NBR"]).
    Returns:
        list: List of paths to the processed output files.
    """
    if input_files is None:
        if not os.path.exists(INPUT_DIR):
            logger.warning(f"Input directory {INPUT_DIR} does not exist.")
            return []
        input_files = glob.glob(os.path.join(INPUT_DIR, "*.tif"))

    if not input_files:
        logger.warning(f"No TIF files found to process.")
        return []

    output_files = []
    for filepath in input_files:
        result = process_scene(filepath, requested_indices=requested_indices)
        if result:
            output_files.extend(result)

    return output_files

if __name__ == "__main__":
    run()
