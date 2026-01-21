import os
import io
import glob
from datetime import datetime
from typing import Dict, Any, List, Optional
from fastapi import FastAPI, HTTPException, Query, Response, BackgroundTasks
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sentinelhub import SentinelHubCatalog, BBox, CRS, DataCollection
from app.ingestion.auth import SentinelHubAuth
import mercantile
import rasterio
from rasterio.warp import reproject
from rasterio.enums import Resampling
from rasterio.transform import from_bounds
import numpy as np
from PIL import Image
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

import asyncio
from app.api.job_manager import JobManager
from app.ingestion.s2_client import S2Client
from app.core.mission_executor import process_mission_task
from app.core.scheduler import MissionScheduler

app = FastAPI()
scheduler = MissionScheduler()

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(scheduler.start())

@app.on_event("shutdown")
def shutdown_event():
    scheduler.stop()

# Add CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class MissionRequest(BaseModel):
    geometry: Dict[str, Any]
    start_date: str
    end_date: str
    sensor: str # "OPTICAL" or "RADAR"
    recurrence: Optional[str] = None # "DAILY"

@app.post("/launch_custom_mission")
async def launch_mission(request: MissionRequest, background_tasks: BackgroundTasks):
    auth = SentinelHubAuth()
    catalog = SentinelHubCatalog(config=auth.config)

    # Geometry parsing
    try:
        geom_type = request.geometry.get('type')
        if geom_type == 'Point':
            lon, lat = request.geometry['coordinates']
            delta = 0.05
            bbox = BBox(bbox=[lon - delta, lat - delta, lon + delta, lat + delta], crs=CRS.WGS84)
        elif geom_type == 'Polygon':
            coords = request.geometry['coordinates'][0]
            lons = [c[0] for c in coords]
            lats = [c[1] for c in coords]
            bbox = BBox(bbox=[min(lons), min(lats), max(lons), max(lats)], crs=CRS.WGS84)
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported geometry type: {geom_type}")
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=400, detail="Invalid geometry structure")

    time_interval = (request.start_date, request.end_date)
    sensor = request.sensor
    tag = None
    results = []

    # 1. Quick Search (Metadata only)
    search_results = {}

    # OPTICAL LOGIC
    if sensor == "OPTICAL":
        s2_collection = DataCollection.SENTINEL2_L2A.define_from(
            "CDSE_S2_L2A",
            service_url="https://sh.dataspace.copernicus.eu"
        )
        try:
            search_iterator = catalog.search(
                collection=s2_collection,
                bbox=bbox,
                time_interval=time_interval,
                fields=["id", "properties", "datetime", "assets", "bbox"]
            )
            items = list(search_iterator)
        except Exception as e:
            print(f"S2 Search Error: {e}")
            items = []

        if items:
            items.sort(key=lambda x: x['datetime'], reverse=True)
            latest_item = items[0]
            cloud_cover = latest_item['properties'].get('eo:cloud_cover', 100)

            if cloud_cover > 80:
                sensor = "RADAR"
                tag = "CLOUD_PIERCED"
            else:
                for item in items:
                    assets = item.get('assets', {})
                    preview = assets.get('thumbnail', {}).get('href') or assets.get('visual', {}).get('href')
                    results.append({
                        "tile_id": item["id"],
                        "date": item["datetime"],
                        "preview_url": preview,
                        "bbox": item.get("bbox")
                    })
                search_results = {
                    "tile_id": latest_item["id"],
                    "preview_url": results[0]["preview_url"],
                    "results": results,
                    "bbox": latest_item.get("bbox")
                }
        else:
             sensor = "RADAR"
             tag = "NO_OPTICAL_DATA"

    # RADAR LOGIC
    if sensor == "RADAR":
        try:
            search_iterator = catalog.search(
                collection=DataCollection.SENTINEL1_IW,
                bbox=bbox,
                time_interval=time_interval,
                fields=["id", "properties", "datetime", "assets", "bbox"]
            )
            items = list(search_iterator)
        except Exception as e:
            print(f"S1 Search Error: {e}")
            items = []

        if items:
            items.sort(key=lambda x: x['datetime'], reverse=True)
            for item in items:
                assets = item.get('assets', {})
                preview = assets.get('thumbnail', {}).get('href')
                results.append({
                    "tile_id": item["id"],
                    "date": item["datetime"],
                    "preview_url": preview,
                    "bbox": item.get("bbox")
                })
            search_results = {
                "tile_id": items[0]["id"],
                "preview_url": results[0]["preview_url"],
                "tag": tag,
                "results": results,
                "bbox": items[0].get("bbox")
            }
        else:
            pass

    if not search_results and not results:
         raise HTTPException(status_code=404, detail="No data found for the given criteria.")

    # 2. Create Job
    jm = JobManager()

    # Store bbox as list for JSON serialization
    bbox_list = [bbox.lower_left[0], bbox.lower_left[1], bbox.upper_right[0], bbox.upper_right[1]]

    job_metadata = {
        "sensor": sensor,
        "start_date": request.start_date,
        "end_date": request.end_date,
        "bbox": bbox_list,
        "recurrence": request.recurrence,
        "preview_url": search_results.get("preview_url"),
        "search_results": search_results, # Store full search results
        "tag": tag
    }

    job_id = jm.create_job(job_metadata)

    # 3. Schedule Processing
    background_tasks.add_task(process_mission_task, job_id, bbox, time_interval, sensor)

    # 4. Return
    response = search_results.copy()
    response["job_id"] = job_id
    response["status"] = "PENDING"
    return response

@app.get("/jobs")
async def list_jobs():
    jm = JobManager()
    return jm.list_jobs()

@app.get("/jobs/{job_id}")
async def get_job(job_id: str):
    jm = JobManager()
    job = jm.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job

@app.get("/jobs/{job_id}/pdf")
async def download_pdf(job_id: str):
    jm = JobManager()
    job = jm.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job["status"] != "COMPLETED":
        raise HTTPException(status_code=400, detail="Job not complete")

    pdf_path = job.get("results", {}).get("pdf_report")
    if not pdf_path or not os.path.exists(pdf_path):
        raise HTTPException(status_code=404, detail="PDF Report not found")

    return FileResponse(pdf_path, media_type="application/pdf", filename=os.path.basename(pdf_path))

@app.get("/stac")
async def get_stac_catalog():
    """
    Returns the root STAC Catalog.
    """
    catalog_path = "data/stac_catalog/catalog.json"
    if not os.path.exists(catalog_path):
        raise HTTPException(status_code=404, detail="STAC Catalog not found.")
    return FileResponse(catalog_path, media_type="application/json")

@app.get("/stac/{item_id}")
async def get_stac_item(item_id: str):
    """
    Returns a specific STAC Item.
    """
    # Assuming flat structure for now or search
    # Helper to find item file
    item_path = os.path.join("data/stac_catalog", item_id, f"{item_id}.json")
    if not os.path.exists(item_path):
         raise HTTPException(status_code=404, detail="STAC Item not found.")
    return FileResponse(item_path, media_type="application/json")

@app.get("/tiles/{z}/{x}/{y}")
async def get_tile(z: int, x: int, y: int, file: Optional[str] = None, job_id: Optional[str] = None):
    """
    Serves a 256x256 PNG tile.
    Can specify `file` directly OR `job_id`.
    If `job_id` is specified, it uses the latest processed file from that job.
    """
    tif_path = None

    if job_id:
        jm = JobManager()
        job = jm.get_job(job_id)
        if job and job.get("status") == "COMPLETED":
            processed = job.get("results", {}).get("processed_files", [])
            if processed:
                ndvi_files = [p for p in processed if "NDVI" in p]
                if ndvi_files:
                    tif_path = ndvi_files[0]
                else:
                    tif_path = processed[0]

        if not tif_path:
             pass

    if not tif_path and file:
        safe_filename = os.path.basename(file)
        p1 = os.path.join("results", safe_filename)
        p2 = os.path.join("data", "processed", safe_filename)
        if os.path.exists(p1):
            tif_path = p1
        elif os.path.exists(p2):
            tif_path = p2

    if not tif_path and not job_id and not file:
        # Latest global logic (Legacy)
        search_paths = [
            os.path.join("results", "*.tif"),
            os.path.join("data", "processed", "*.tif")
        ]
        candidates = []
        for sp in search_paths:
            candidates.extend(glob.glob(sp))
        if candidates:
            candidates.sort(key=os.path.getmtime, reverse=True)
            tif_path = candidates[0]

    if not tif_path:
        # Return transparent if nothing found
        img = Image.new('RGBA', (256, 256), (0, 0, 0, 0))
        buf = io.BytesIO()
        img.save(buf, format='PNG')
        return Response(content=buf.getvalue(), media_type="image/png")

    # Render Tile
    bounds = mercantile.xy_bounds(x, y, z)
    left, bottom, right, top = bounds.left, bounds.bottom, bounds.right, bounds.top

    try:
        with rasterio.open(tif_path) as src:
            dst_transform = from_bounds(left, bottom, right, top, 256, 256)
            dst_crs = 'EPSG:3857'
            data = np.full((src.count, 256, 256), np.nan, dtype=np.float32)

            for i in range(src.count):
                reproject(
                    source=rasterio.band(src, i+1),
                    destination=data[i],
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=dst_transform,
                    dst_crs=dst_crs,
                    resampling=Resampling.bilinear
                )

            if np.all(np.isnan(data)):
                img = Image.new('RGBA', (256, 256), (0, 0, 0, 0))
                buf = io.BytesIO()
                img.save(buf, format='PNG')
                return Response(content=buf.getvalue(), media_type="image/png")

            # Visualization
            if data.shape[0] == 1:
                band = data[0]
                norm = plt.Normalize(vmin=-0.2, vmax=1.0)
                cmap = plt.get_cmap('RdYlGn')
                colored = cmap(norm(band))
                mask = np.isnan(band)
                colored[mask] = [0, 0, 0, 0]
                img_data = (colored * 255).astype(np.uint8)
                img = Image.fromarray(img_data, 'RGBA')

            elif data.shape[0] >= 3:
                img_data = np.moveaxis(data[:3], 0, -1)
                max_val = np.nanmax(img_data)
                if max_val is not None and max_val <= 1.5:
                     img_data = np.clip(img_data, 0.0, 1.0) * 255
                img_data = np.nan_to_num(img_data).astype(np.uint8)
                alpha = np.full((256, 256), 255, dtype=np.uint8)
                mask = np.all(img_data == 0, axis=2)
                alpha[mask] = 0
                img_rgba = np.dstack((img_data, alpha))
                img = Image.fromarray(img_rgba, 'RGBA')
            else:
                 img = Image.new('RGBA', (256, 256), (0, 0, 0, 0))

            buf = io.BytesIO()
            img.save(buf, format='PNG')
            return Response(content=buf.getvalue(), media_type="image/png")

    except Exception as e:
        print(f"Tile Error: {e}")
        img = Image.new('RGBA', (256, 256), (0, 0, 0, 0))
        buf = io.BytesIO()
        img.save(buf, format='PNG')
        return Response(content=buf.getvalue(), media_type="image/png")

# Mount Static Files (Must be last to avoid overriding API routes)
app.mount("/", StaticFiles(directory="app/platform", html=True), name="platform")
