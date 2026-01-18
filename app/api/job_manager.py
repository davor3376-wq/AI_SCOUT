import json
import os
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("JobManager")

JOBS_FILE = "jobs.json"

class JobManager:
    def __init__(self, filepath: str = JOBS_FILE):
        self.filepath = filepath
        self._ensure_file()

    def _ensure_file(self):
        if not os.path.exists(self.filepath):
            with open(self.filepath, 'w') as f:
                json.dump({"jobs": {}}, f)

    def _load(self) -> Dict[str, Any]:
        try:
            with open(self.filepath, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            return {"jobs": {}}

    def _save(self, data: Dict[str, Any]):
        with open(self.filepath, 'w') as f:
            json.dump(data, f, indent=4)

    def create_job(self, metadata: Dict[str, Any]) -> str:
        """
        Creates a new job with the provided metadata.
        Returns the Job ID.
        """
        job_id = f"JOB-{uuid.uuid4().hex[:8].upper()}"
        data = self._load()

        # Handle recurrence if present in metadata
        recurrence = metadata.get("recurrence")

        job = {
            "id": job_id,
            "status": "PENDING",
            "created_at": datetime.utcnow().isoformat(),
            "recurrence": recurrence, # None, "DAILY", etc.
            "last_run": None,
            **metadata,
            "results": {
                "raw_files": [],
                "processed_files": [],
                "pdf_report": None
            }
        }

        data["jobs"][job_id] = job
        self._save(data)
        logger.info(f"Created job {job_id}")
        return job_id

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        data = self._load()
        return data["jobs"].get(job_id)

    def list_jobs(self) -> List[Dict[str, Any]]:
        data = self._load()
        jobs = list(data["jobs"].values())
        # Sort by created_at desc
        jobs.sort(key=lambda x: x.get("created_at", ""), reverse=True)
        return jobs

    def update_job_status(self, job_id: str, status: str, results: Optional[Dict[str, Any]] = None, error: Optional[str] = None):
        data = self._load()
        if job_id in data["jobs"]:
            data["jobs"][job_id]["status"] = status
            if results:
                # Merge results
                current_results = data["jobs"][job_id].get("results", {})
                current_results.update(results)
                data["jobs"][job_id]["results"] = current_results
            if error:
                data["jobs"][job_id]["error"] = error

            data["jobs"][job_id]["updated_at"] = datetime.utcnow().isoformat()
            self._save(data)
            logger.info(f"Updated job {job_id} to {status}")
        else:
            logger.error(f"Job {job_id} not found for update.")

    def update_last_run(self, job_id: str, last_run: datetime):
        data = self._load()
        if job_id in data["jobs"]:
            data["jobs"][job_id]["last_run"] = last_run.isoformat()
            self._save(data)
