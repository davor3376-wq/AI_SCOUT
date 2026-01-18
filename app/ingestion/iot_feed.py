"""
IoT Feed Handler (Ingestion).
Responsible for ingesting real-time data from IoT sensors (e.g., ground stations, drones).
"""
import os
import json
from datetime import datetime
from typing import List, Dict, Any

class IotFeedHandler:
    """
    Handles ingestion of IoT data streams.
    """

    def __init__(self, endpoint: str = None):
        self.endpoint = endpoint

    def process_message(self, message: str) -> Dict[str, Any]:
        """
        Parses an incoming IoT message (JSON string).
        """
        try:
            data = json.loads(message)
            # Validate schema
            required_keys = ["device_id", "timestamp", "location", "sensors"]
            if not all(k in data for k in required_keys):
                raise ValueError("Invalid IoT message schema")
            return data
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON")

    def ingest_batch(self, messages: List[str]) -> str:
        """
        Ingests a batch of messages and saves them.
        """
        processed_data = []
        for msg in messages:
            try:
                processed_data.append(self.process_message(msg))
            except ValueError as e:
                print(f"Skipping bad message: {e}")

        if not processed_data:
            return None

        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"{timestamp}_iot_batch.json"
        filepath = os.path.join("data", "raw", filename)

        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        with open(filepath, 'w') as f:
            json.dump(processed_data, f, indent=2)

        return filepath
