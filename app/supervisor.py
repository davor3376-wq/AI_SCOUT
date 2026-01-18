import asyncio
import json
import logging
import os
import rasterio
import numpy as np
from datetime import datetime, timedelta
from sentinelhub import BBox, CRS

from app.ingestion.s2_client import S2Client
from app.analytics import processor
from app.reporting.pdf_gen import PDFReportGenerator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Supervisor")

TASK_FILE = "tasks.json"

def load_tasks():
    if not os.path.exists(TASK_FILE):
        logger.error(f"Task file {TASK_FILE} not found.")
        return []
    with open(TASK_FILE, 'r') as f:
        data = json.load(f)
    return data.get("tasks", [])

def check_alpha_health(raw_file_path, expected_bbox_coords):
    logger.info(f"Running Health Check on {raw_file_path}")

    # Checklist Item 1: File size > 0
    if not os.path.exists(raw_file_path):
        logger.error("Health Check Failed: File does not exist.")
        return False
    if os.path.getsize(raw_file_path) == 0:
        logger.error("Health Check Failed: File size is 0.")
        return False

    # Checklist Item 2: Image is not all-black (Mean > 100)
    try:
        with rasterio.open(raw_file_path) as src:
            # Check first band (B04) or all?
            # "Image is not all-black".
            # We assume at least one band should be meaningful.
            data = src.read(1)
            mean_val = np.mean(data)
            if mean_val <= 100:
                logger.error(f"Health Check Failed: Image mean {mean_val:.2f} <= 100 (All black?).")
                return False
    except Exception as e:
        logger.error(f"Health Check Failed: Error reading image: {e}")
        return False

    # Checklist Item 3: Metadata matches the BBox
    # Metadata is in _provenance.json
    meta_path = raw_file_path.replace(".tif", "_provenance.json")
    if not os.path.exists(meta_path):
        logger.error(f"Health Check Failed: Metadata file {meta_path} missing.")
        return False

    try:
        with open(meta_path, 'r') as f:
            meta = json.load(f)

        # Provenance bbox: [minx, miny, maxx, maxy]
        meta_bbox = meta.get("bbox")
        if not meta_bbox:
            logger.error("Health Check Failed: No bbox in metadata.")
            return False

        # Compare approx
        # expected_bbox_coords is [16.2, 48.1, 16.5, 48.3]
        # Allow small tolerance
        if not np.allclose(meta_bbox, expected_bbox_coords, atol=0.01):
             logger.error(f"Health Check Failed: BBox mismatch. Expected {expected_bbox_coords}, got {meta_bbox}")
             return False

    except Exception as e:
        logger.error(f"Health Check Failed: Error checking metadata: {e}")
        return False

    logger.info("Health Check PASSED.")
    return True

async def supervisor_main():
    tasks = load_tasks()
    if not tasks:
        logger.warning("No tasks found.")
        return

    client = S2Client()
    pdf_gen = PDFReportGenerator()

    # Time interval: Last 30 days
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=30)
    time_interval = (start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))

    for task in tasks:
        task_name = task["name"]
        bbox_coords = task["bbox"]
        logger.info(f"Starting Task: {task_name}")

        bbox = BBox(bbox=bbox_coords, crs=CRS.WGS84)

        # 1. Alpha (Ingestion)
        logger.info("--- Alpha: Downloading ---")
        try:
            downloaded_files = await asyncio.to_thread(
                client.download_data,
                bbox=bbox,
                time_interval=time_interval
            )
        except Exception as e:
            logger.error(f"Alpha failed for {task_name}: {e}")
            continue

        if not downloaded_files:
            logger.warning(f"No files downloaded for {task_name}.")
            continue

        for raw_file in downloaded_files:
            # 2. Health Check
            logger.info(f"--- Health Check: {os.path.basename(raw_file)} ---")
            if not check_alpha_health(raw_file, bbox_coords):
                logger.warning(f"Skipping {raw_file} due to health check failure.")
                continue

            # 3. Beta (Analytics)
            logger.info(f"--- Beta: Processing {os.path.basename(raw_file)} ---")
            try:
                processed_files = await asyncio.to_thread(
                    processor.process_scene,
                    raw_file
                )
            except Exception as e:
                logger.error(f"Beta failed for {raw_file}: {e}")
                continue

            if not processed_files:
                logger.warning("Beta produced no output.")
                continue

            # 4. Gamma (Reporting)
            logger.info(f"--- Gamma: Reporting ---")
            try:
                # Generate report for this specific tile's processed files
                # Filename: Evidence_Pack_{TileID}.pdf
                # We can extract Tile ID from filename
                # raw_file: 20231025_S2_T33UUE.tif
                parts = os.path.basename(raw_file).replace(".tif", "").split("_")
                tile_id = parts[2] if len(parts) > 2 else "unknown"
                date_str = parts[0]
                report_name = f"Evidence_Pack_{date_str}_{tile_id}.pdf"

                await asyncio.to_thread(
                    pdf_gen.generate_pdf,
                    filename=report_name,
                    specific_files=processed_files
                )
            except Exception as e:
                logger.error(f"Gamma failed: {e}")

if __name__ == "__main__":
    asyncio.run(supervisor_main())
