import os
import logging
from sentinelhub import BBox
from app.ingestion.s2_client import S2Client
from app.analytics import processor
from app.reporting.pdf_gen import PDFReportGenerator
from app.api.job_manager import JobManager

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

        if not raw_files:
            jm.update_job_status(job_id, "FAILED", error="No data downloaded (or sensor not supported for processing)")
            return

        # 2. Process (Analytics)
        processed_files = processor.run(input_files=raw_files)

        if not processed_files:
             jm.update_job_status(job_id, "FAILED", error="No data processed (Analytics produced no output)")
             return

        # 3. Report (Reporting)
        report_name = f"Evidence_Pack_{job_id}.pdf"
        pdf_gen = PDFReportGenerator()
        # generate_pdf is blocking
        pdf_gen.generate_pdf(filename=report_name, specific_files=processed_files)

        report_path = os.path.join("results", report_name)

        # 4. Finish
        results = {
            "raw_files": raw_files,
            "processed_files": processed_files,
            "pdf_report": report_path
        }
        jm.update_job_status(job_id, "COMPLETED", results=results)

    except Exception as e:
        logger.error(f"Job {job_id} failed: {e}")
        jm.update_job_status(job_id, "FAILED", error=str(e))
