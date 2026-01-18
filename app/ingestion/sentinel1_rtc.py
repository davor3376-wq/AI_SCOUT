"""
Sentinel-1 RTC Client (Role 2 - Enhanced).
Responsible for downloading Sentinel-1 GRD (VV, VH) data with Radiometric Terrain Correction.
"""
import os
from datetime import datetime
from typing import List, Tuple, Union

import numpy as np
import rasterio
from rasterio.transform import from_bounds
from sentinelhub import (
    BBox,
    DataCollection,
    MimeType,
    SentinelHubRequest,
    SentinelHubCatalog,
)

from app.ingestion.auth import SentinelHubAuth
from app.ingestion.metadata import generate_provenance


class S1RTCClient:
    """
    Client for downloading Sentinel-1 IW GRD data with RTC.
    """

    def __init__(self):
        """
        Initialize the S1RTCClient.
        """
        self.auth = SentinelHubAuth()
        self.config = self.auth.config

        # Define DataCollection for CDSE S1 IW
        self.data_collection = DataCollection.SENTINEL1_IW.define_from(
            "CDSE_S1_IW",
            service_url="https://sh.dataspace.copernicus.eu"
        )

    def download_data(
        self,
        bbox: BBox,
        time_interval: Tuple[Union[str, datetime], Union[str, datetime]],
        resolution: int = 10,
    ) -> List[str]:
        """
        Downloads Sentinel-1 data.
        """
        # Evalscript for VV, VH (Linear gamma0)
        evalscript = """
        //VERSION=3
        function setup() {
          return {
            input: ["VV", "VH", "dataMask"],
            output: { bands: 2, sampleType: "FLOAT32" }
          };
        }

        function evaluatePixel(sample) {
          return [sample.VV, sample.VH];
        }
        """

        catalog = SentinelHubCatalog(config=self.config)

        search_iterator = catalog.search(
            collection=self.data_collection,
            bbox=bbox,
            time_interval=time_interval,
            fields=["id", "properties", "datetime"]
        )

        output_files = []
        try:
            results = list(search_iterator)
        except Exception as e:
            print(f"Catalog search failed: {e}")
            return []

        for item in results:
            props = item["properties"]
            acquisition_time = props["datetime"]
            orbit_id = props.get("sat:absolute_orbit")
            tile_id = item["id"]

            dt = datetime.fromisoformat(acquisition_time.replace("Z", "+00:00"))
            date_str = dt.strftime("%Y%m%d")
            sensor = "S1_RTC"

            filename = f"{date_str}_{sensor}_{tile_id}.tif"
            filepath = os.path.join("data", "raw", filename)

            # Request specific time
            req_interval = (dt, dt)

            # Processing options for RTC
            # Gamma0 Terrain Correction
            processing_options = {
                "backCoeff": "GAMMA0_TERRAIN",
                "orthorectify": True
            }

            request = SentinelHubRequest(
                evalscript=evalscript,
                input_data=[
                    SentinelHubRequest.input_data(
                        data_collection=self.data_collection,
                        time_interval=req_interval,
                        processing=processing_options
                    )
                ],
                responses=[
                    SentinelHubRequest.output_response("default", MimeType.TIFF)
                ],
                bbox=bbox,
                resolution=resolution,
                config=self.config,
            )

            try:
                data_list = request.get_data()
            except Exception as e:
                print(f"Failed to download {tile_id}: {e}")
                continue

            if not data_list:
                continue

            image_data = data_list[0]
            height, width, bands = image_data.shape

            transform = from_bounds(
                bbox.lower_left[0], bbox.lower_left[1],
                bbox.upper_right[0], bbox.upper_right[1],
                width, height
            )

            os.makedirs(os.path.dirname(filepath), exist_ok=True)

            with rasterio.open(
                filepath,
                'w',
                driver='GTiff',
                height=height,
                width=width,
                count=bands,
                dtype=image_data.dtype,
                crs=bbox.crs.pyproj_crs(),
                transform=transform,
            ) as dst:
                for b in range(bands):
                    dst.write(image_data[:, :, b], b + 1)

            output_files.append(filepath)

            # Generate Metadata
            meta_filepath = filepath.replace(".tif", "_provenance.json")
            provenance = {
                "orbit_id": orbit_id,
                "acquisition_time": acquisition_time,
                "sensor": sensor,
                "bbox": [bbox.lower_left[0], bbox.lower_left[1], bbox.upper_right[0], bbox.upper_right[1]],
                "item_id": tile_id,
                "processing": processing_options
            }
            generate_provenance(meta_filepath, provenance)

        return output_files
