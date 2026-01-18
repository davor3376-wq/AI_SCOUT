import os
from datetime import datetime
from typing import Dict, Any, List, Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sentinelhub import SentinelHubCatalog, BBox, CRS, DataCollection
from app.ingestion.auth import SentinelHubAuth

app = FastAPI()

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
