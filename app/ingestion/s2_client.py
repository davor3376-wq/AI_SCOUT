"""
Sentinel-2 Client (Role 3).
Responsible for downloading Sentinel-2 L2A (B04, B08, SCL) data and generating metadata.
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


class S2Client:
    """
    Client for downloading Sentinel-2 L2A data.
    """

    def __init__(self):
        """
        Initialize the S2Client.
        """
        self.auth = SentinelHubAuth()
        self.config = self.auth.config

        # Define DataCollection for CDSE as per requirements
        self.data_collection = DataCollection.SENTINEL2_L2A.define_from(
            "CDSE_S2_L2A",
            service_url="https://sh.dataspace.copernicus.eu"
        )

    def download_data(
        self,
        bbox: BBox,
        time_interval: Tuple[Union[str, datetime], Union[str, datetime]],
        resolution: int = 10,
    ) -> List[str]:
        """
        Downloads Sentinel-2 data for the given BBox and time interval.

        Args:
            bbox: Bounding box of the area of interest.
            time_interval: Tuple of (start_date, end_date).
            resolution: Resolution in meters (default 10).

        Returns:
            List of paths to downloaded files.
        """
        # Evalscript for B03, B04, B08, SCL, and QA60
        # B03: Green (for NDWI)
        # B04: Red (for NDVI)
        # B08: NIR (for NDVI/NDWI)
        # SCL: Scene Classification Layer
        # QA60: Quality Assurance band (for Cloud Masking)
        evalscript = """
        //VERSION=3
        function setup() {
          return {
            input: ["B03", "B04", "B08", "SCL", "QA60"],
            output: { bands: 5, sampleType: "UINT16" }
          };
        }

        function evaluatePixel(sample) {
          return [sample.B03, sample.B04, sample.B08, sample.SCL, sample.QA60];
        }
        """

        # Use Catalog to get available scenes and metadata
        catalog = SentinelHubCatalog(config=self.config)

        # Search using the CDSE collection definition if possible,
        # or use standard collection for catalog search if compatible?
        # SentinelHubCatalog usually works with standard collections or collection IDs.
        # DataCollection.SENTINEL2_L2A should map to the correct collection ID "sentinel-2-l2a".
        # The define_from changes the service URL, which is crucial for download.
        # For Catalog search, the config should already handle the base URL if set correctly.
        # Let's try using the self.data_collection.

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
            # Fallback or re-raise. CDSE Catalog might need specific handling?
            # If search fails, we might have issues.
            print(f"Catalog search failed: {e}")
            return []

        for item in results:
            props = item["properties"]
            acquisition_time = props["datetime"]

            # Additional S2 metadata
            orbit_id = props.get("sat:absolute_orbit") # Might be null or different key
            cloud_cover = props.get("eo:cloud_cover")

            tile_id = item["id"]

            dt = datetime.fromisoformat(acquisition_time.replace("Z", "+00:00"))
            date_str = dt.strftime("%Y%m%d")
            sensor = "S2"

            filename = f"{date_str}_{sensor}_{tile_id}.tif"
            filepath = os.path.join("data", "raw", filename)

            # Request specific time
            req_interval = (dt, dt)

            request = SentinelHubRequest(
                evalscript=evalscript,
                input_data=[
                    SentinelHubRequest.input_data(
                        data_collection=self.data_collection,
                        time_interval=req_interval,
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

            # SCL is usually UINT8, but B04/B08 are UINT16.
            # Output sampleType is UINT16, so SCL will be cast to UINT16.
            # This is fine.

            transform = from_bounds(
                bbox.lower_left[0], bbox.lower_left[1],
                bbox.upper_right[0], bbox.upper_right[1],
                width, height
            )

            # Ensure dir exists
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
                "item_id": item["id"],
                "cloud_cover": cloud_cover,
                "properties": props
            }
            generate_provenance(meta_filepath, provenance)

        return output_files
