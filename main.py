import asyncio
import logging
import os
import argparse
from datetime import datetime, timedelta
from typing import Optional, Tuple, Union, List

from app.ingestion.s2_client import S2Client
from app.analytics import processor
from app.reporting.pdf_gen import PDFReportGenerator
from app.reporting.integrity import IntegrityChecker
from sentinelhub import BBox, CRS

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def main(
    bbox: Optional[BBox] = None,
    time_interval: Optional[Tuple[Union[str, datetime], Union[str, datetime]]] = None
) -> List[str]:
    """
    Executes the mission for a given BBox and time interval.
    Returns a list of generated analysis files (NDVI TIFs).
    """
    logger.info("Starting Squad Gamma Mission...")

    # Default BBox (Vienna) if not provided
    if bbox is None:
        # 16.2, 48.1 to 16.5, 48.3
        bbox = BBox(bbox=[16.2, 48.1, 16.5, 48.3], crs=CRS.WGS84)

    # Default Time Interval (Last 30 days) if not provided
    if time_interval is None:
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=30)
        time_interval = (start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))

    # --- Role 14: Coordinator ---

    # 1. Ingestion (Role 3 S2Client)
    logger.info("--- Step 1: Checking for new data (Ingestion) ---")
    client = S2Client()

    downloaded_files = []
    try:
        # download_data is synchronous
        downloaded_files = client.download_data(
            bbox=bbox,
            time_interval=time_interval
        )
        logger.info(f"Downloaded {len(downloaded_files)} new files.")
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        # If ingestion fails, we might stop or continue.
        # For the supervisor to detect failure, we should probably re-raise or return empty.
        # But let's re-raise so supervisor catches it.
        raise e

    if not downloaded_files:
        logger.warning("No files downloaded. Aborting mission.")
        return []

    # 2. Analytics (Role 6/8 Processor)
    logger.info("--- Step 2: Processing Analytics ---")
    processed_files = []
    try:
        # Pass the specific files we just downloaded
        processed_files = processor.run(input_files=downloaded_files)
        logger.info(f"Generated {len(processed_files)} analysis files.")
    except Exception as e:
        logger.error(f"Analytics failed: {e}")
        raise e

    # 3. Reporting (Role 11 PDF Gen)
    logger.info("--- Step 3: Generating PDF Report ---")
    try:
        # PDF Gen currently picks up all files in data/processed.
        # Ideally it should only report on what we just processed, but for now we keep it simple.
        pdf_gen = PDFReportGenerator()
        pdf_gen.generate_pdf()
    except Exception as e:
        logger.error(f"PDF Generation failed: {e}")
        # Reporting failure shouldn't necessarily fail the mission data check, but supervisor might want to know.
        # We'll log it.

    # 4. Integrity (Role 12)
    logger.info("--- Step 4: Verifying Integrity ---")
    try:
        checker = IntegrityChecker()
        checker.generate_integrity_file()
    except Exception as e:
        logger.error(f"Integrity check failed: {e}")

    logger.info("Mission Complete.")
    return processed_files

if __name__ == "__main__":
    # Simple CLI handling for basic testing
    # In a real scenario, we might use argparse to parse --bbox and --time
    asyncio.run(main())
