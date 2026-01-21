import os
import logging
from sentinelhub import BBox
from app.ingestion.s2_client import S2Client
from app.ingestion.era5_climate import Era5Client
from app.ingestion.stac_catalog import StacCatalogManager
from app.analytics import processor
from app.analytics.chronos import calculate_change
from app.reporting.pdf_gen import PDFReportGenerator
from app.api.job_manager import JobManager
import datetime

logger = logging.getLogger("MissionExecutor")

def process_mission_task(job_id: str, bbox: BBox, time_interval: tuple, sensor: str):
    """
    Background task to execute the mission pipeline.
    """
    jm = JobManager()
    jm.update_job_status(job_id, "RUNNING")

    try:
        # 1. Download (Ingestion)
        raw_files = []
        if sensor == "OPTICAL":
            client = S2Client()
            # download_data is blocking
            raw_files = client.download_data(bbox=bbox, time_interval=time_interval)
        else:
            # Placeholder for RADAR or other sensors
            pass

        # Atmosphere: Download Weather Context (ERA5)
        # We do this regardless of sensor for better context
        try:
            era5_client = Era5Client()
            # Use same bbox and time interval
            weather_files = era5_client.download_data(bbox=bbox, time_interval=time_interval)
            raw_files.extend(weather_files)
        except Exception as e:
            logger.warning(f"Failed to fetch ERA5 weather data: {e}")

        if not raw_files:
            jm.update_job_status(job_id, "FAILED", error="No data downloaded (or sensor not supported for processing)")
            return

        # 2. Process (Analytics)
        processed_files = processor.run(input_files=raw_files)

        if not processed_files:
             jm.update_job_status(job_id, "FAILED", error="No data processed (Analytics produced no output)")
             return

        # Chronos: Change Detection (If recurring)
        job = jm.get_job(job_id)
        parent_id = job.get("parent_job_id")
        if parent_id:
            parent_job = jm.get_job(parent_id)
            # Find last valid output from parent's history?
            # Or simplified: Check if parent has a 'last_run' output.
            # Actually, we need the *previous* run's output.
            # Since 'parent_job' just stores metadata, we might need to search for previous jobs.
            # For this MVP, let's search for the most recent NDVI file in 'data/processed' that isn't the current one.
            # This is a heuristic.

            ndvi_files = [f for f in processed_files if "NDVI" in f]
            if ndvi_files:
                current_ndvi = ndvi_files[0]

                # Find previous NDVI
                import glob
                all_ndvis = glob.glob("data/processed/*_NDVI_analysis.tif")
                all_ndvis.sort(key=os.path.getmtime, reverse=True)

                # The current one should be first. The next one is previous.
                previous_ndvi = None
                for p in all_ndvis:
                    if os.path.basename(p) != os.path.basename(current_ndvi):
                         previous_ndvi = p
                         break

                if previous_ndvi:
                    change_map = calculate_change(current_ndvi, previous_ndvi)
                    if change_map:
                        processed_files.append(change_map)

        # 3. Nexus: STAC Registration
        try:
            stac_mgr = StacCatalogManager()
            for filepath in processed_files:
                # Basic metadata
                item_id = os.path.basename(filepath).split(".")[0]
                # Assuming simple date parsing from filename or using current time
                # Ideally, we pass the acquisition time from raw_files provenance
                acq_time = datetime.datetime.now() # Fallback

                # Check for weather data to add as properties
                props = {}
                # weather_files is local variable in try/except above.
                # We need to ensure it's accessible or check raw_files
                if any("ERA5" in f for f in raw_files):
                    props["weather_context"] = True

                stac_mgr.add_item(item_id, filepath, bbox, acq_time, props)
        except Exception as e:
            logger.error(f"STAC Registration failed: {e}")

        # 4. Report (Reporting)
        # Include weather files in specific_files for the report generator
        # Only if ERA5 files are in raw_files
        era5_files = [f for f in raw_files if "ERA5" in f]
        report_files = processed_files + era5_files

        report_name = f"Evidence_Pack_{job_id}.pdf"
        pdf_gen = PDFReportGenerator()
        # generate_pdf is blocking
        pdf_gen.generate_pdf(filename=report_name, specific_files=report_files)

        report_path = os.path.join("results", report_name)

        # 5. Finish
        results = {
            "raw_files": raw_files,
            "processed_files": processed_files,
            "pdf_report": report_path
        }
        jm.update_job_status(job_id, "COMPLETED", results=results)

    except Exception as e:
        logger.error(f"Job {job_id} failed: {e}")
        jm.update_job_status(job_id, "FAILED", error=str(e))
