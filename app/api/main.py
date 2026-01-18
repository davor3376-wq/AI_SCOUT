import os
import io
import glob
from datetime import datetime
from typing import Dict, Any, List, Optional
from fastapi import FastAPI, HTTPException, Query, Response
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

app = FastAPI()

# Add CORS to be safe
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

@app.post("/launch_custom_mission")
async def launch_mission(request: MissionRequest):
    auth = SentinelHubAuth()
    catalog = SentinelHubCatalog(config=auth.config)

    # Geometry parsing (Polygon or Point)
    try:
        geom_type = request.geometry.get('type')
        if geom_type == 'Point':
            lon, lat = request.geometry['coordinates']
            delta = 0.05  # Approx 5km buffer
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

    # OPTICAL LOGIC
    if sensor == "OPTICAL":
        s2_collection = DataCollection.SENTINEL2_L2A.define_from(
            "CDSE_S2_L2A",
            service_url="https://sh.dataspace.copernicus.eu"
        )

        # Search for items
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
            # Sort by date descending
            items.sort(key=lambda x: x['datetime'], reverse=True)
            latest_item = items[0]
            cloud_cover = latest_item['properties'].get('eo:cloud_cover', 100)

            if cloud_cover > 80:
                sensor = "RADAR"
                tag = "CLOUD_PIERCED"
            else:
                # Prepare results
                for item in items:
                    assets = item.get('assets', {})
                    preview = assets.get('thumbnail', {}).get('href') or assets.get('visual', {}).get('href')

                    results.append({
                        "tile_id": item["id"],
                        "date": item["datetime"],
                        "preview_url": preview,
                        "bbox": item.get("bbox")
                    })

                return {
                    "tile_id": latest_item["id"],
                    "preview_url": results[0]["preview_url"],
                    "results": results,
                    "bbox": latest_item.get("bbox")
                }
        else:
             # No optical data found, try radar
             sensor = "RADAR"
             tag = "NO_OPTICAL_DATA"

    # RADAR LOGIC (Fallback or Primary)
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

        if not items:
            raise HTTPException(status_code=404, detail="No data found for the given criteria.")

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

        return {
            "tile_id": items[0]["id"],
            "preview_url": results[0]["preview_url"],
            "tag": tag,
            "results": results,
            "bbox": items[0].get("bbox")
        }


@app.get("/tiles/{z}/{x}/{y}")
async def get_tile(z: int, x: int, y: int, file: Optional[str] = None):
    """
    Serves a 256x256 PNG tile for the requested Web Mercator tile (z, x, y).
    Looks for the latest .tif in results/ or data/processed/.
    """

    # Locate the TIF file
    tif_path = None

    if file:
        # Check explicit path in both dirs
        safe_filename = os.path.basename(file)
        p1 = os.path.join("results", safe_filename)
        p2 = os.path.join("data", "processed", safe_filename)
        if os.path.exists(p1):
            tif_path = p1
        elif os.path.exists(p2):
            tif_path = p2
    else:
        # Find latest TIF
        search_paths = [
            os.path.join("results", "*.tif"),
            os.path.join("data", "processed", "*.tif")
        ]
        candidates = []
        for sp in search_paths:
            candidates.extend(glob.glob(sp))

        if candidates:
            # Sort by modification time
            candidates.sort(key=os.path.getmtime, reverse=True)
            tif_path = candidates[0]

    if not tif_path:
        # Return empty/transparent tile if no data found?
        # Or 404. 404 is better for debugging.
        raise HTTPException(status_code=404, detail="No processed data found.")

    # Calculate tile bounds in EPSG:3857
    bounds = mercantile.xy_bounds(x, y, z)
    left, bottom, right, top = bounds.left, bounds.bottom, bounds.right, bounds.top

    try:
        with rasterio.open(tif_path) as src:
            # Prepare destination
            dst_transform = from_bounds(left, bottom, right, top, 256, 256)
            dst_crs = 'EPSG:3857'

            data = np.full((src.count, 256, 256), np.nan, dtype=np.float32)

            # Reproject
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

            # If all data is NaN/nodata, return empty tile
            if np.all(np.isnan(data)):
                # Return transparent PNG
                img = Image.new('RGBA', (256, 256), (0, 0, 0, 0))
                buf = io.BytesIO()
                img.save(buf, format='PNG')
                return Response(content=buf.getvalue(), media_type="image/png")

            # Visualization Logic
            # If 1 band, apply colormap
            if data.shape[0] == 1:
                band = data[0]
                # Normalize or clip? NDVI is -1 to 1.
                # 'RdYlGn' colormap
                # Normalize -1 to 1 to 0-1 for colormap
                # But NDVI usually 0 to 1 for vegetation, <0 water/snow

                norm = plt.Normalize(vmin=-0.2, vmax=1.0)
                cmap = plt.get_cmap('RdYlGn')

                # Apply colormap
                colored = cmap(norm(band)) # Returns (256, 256, 4) float

                # Handle NaNs (transparency)
                mask = np.isnan(band)
                colored[mask] = [0, 0, 0, 0] # Transparent

                # Convert to UINT8
                img_data = (colored * 255).astype(np.uint8)
                img = Image.fromarray(img_data, 'RGBA')

            elif data.shape[0] >= 3:
                # RGB
                img_data = np.moveaxis(data[:3], 0, -1)

                # Check range and normalize if necessary
                # If data is float (it is, per initialization) and values are small (<=1.0), scale them.
                max_val = np.nanmax(img_data)
                if max_val is not None and max_val <= 1.5:
                     img_data = np.clip(img_data, 0.0, 1.0) * 255

                img_data = np.nan_to_num(img_data).astype(np.uint8)

                # Add alpha channel for transparency of [0,0,0] pixels
                alpha = np.full((256, 256), 255, dtype=np.uint8)
                mask = np.all(img_data == 0, axis=2)
                alpha[mask] = 0

                img_rgba = np.dstack((img_data, alpha))
                img = Image.fromarray(img_rgba, 'RGBA')
            else:
                # Fallback
                 img = Image.new('RGBA', (256, 256), (0, 0, 0, 0))

            buf = io.BytesIO()
            img.save(buf, format='PNG')
            return Response(content=buf.getvalue(), media_type="image/png")

    except Exception as e:
        print(f"Tile Error: {e}")
        # Return transparent on error or 500?
        # Better transparent to not break map
        img = Image.new('RGBA', (256, 256), (0, 0, 0, 0))
        buf = io.BytesIO()
        img.save(buf, format='PNG')
        return Response(content=buf.getvalue(), media_type="image/png")
