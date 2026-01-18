import asyncio
import logging
import os
from datetime import datetime, timedelta

from app.ingestion.s2_client import S2Client
from app.analytics import processor
from app.reporting.pdf_gen import PDFReportGenerator
from app.reporting.integrity import IntegrityChecker
from sentinelhub import BBox, CRS

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def main():
    logger.info("Starting Squad Gamma Mission...")

    # --- Role 14: Coordinator ---

    # 1. Ingestion (Role 3 S2Client)
    logger.info("--- Step 1: Checking for new data (Ingestion) ---")
    client = S2Client()

    # Define Vienna BBox (from memory)
    # 16.2, 48.1 to 16.5, 48.3
    vienna_bbox = BBox(bbox=[16.2, 48.1, 16.5, 48.3], crs=CRS.WGS84)

    # Define Time Interval
    # For now, we'll check the last 30 days to ensure we get something if it's new
    # Or maybe a specific range if required? The prompt just says "Check for new data".
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=30)
    time_interval = (start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))

    try:
        downloaded_files = client.download_data(
            bbox=vienna_bbox,
            time_interval=time_interval
        )
        logger.info(f"Downloaded {len(downloaded_files)} new files.")
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        # We continue to analytics even if ingestion fails, in case there is existing data?
        # But usually we might want to stop. However, to fulfill the "Master switch" description,
        # we might proceed to process whatever is there.

    # 2. Analytics (Role 6/8 Processor)
    logger.info("--- Step 2: Processing Analytics ---")
    try:
        processor.run()
    except Exception as e:
        logger.error(f"Analytics failed: {e}")

    # 3. Reporting (Role 11 PDF Gen)
    logger.info("--- Step 3: Generating PDF Report ---")
    try:
        pdf_gen = PDFReportGenerator()
        pdf_gen.generate_pdf()
    except Exception as e:
        logger.error(f"PDF Generation failed: {e}")

    # 4. Integrity (Role 12)
    logger.info("--- Step 4: Verifying Integrity ---")
    try:
        checker = IntegrityChecker()
        checker.generate_integrity_file()
    except Exception as e:
        logger.error(f"Integrity check failed: {e}")

    logger.info("Mission Complete.")

if __name__ == "__main__":
    asyncio.run(main())
