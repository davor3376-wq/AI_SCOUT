import numpy as np

def calculate_alert_level(ndvi: np.ndarray, cloud_cover_pct: float) -> str:
    """
    Determines the Alert Level based on NDVI and Cloud Cover.

    Returns: "LOW", "MEDIUM", "HIGH"
    """
    if cloud_cover_pct > 50.0:
        return "LOW" # Too cloudy to be sure

    mean_ndvi = np.nanmean(ndvi)

    # Watchdog Logic:
    # If vegetation health is critically low (e.g. deforestation), Alert HIGH.
    # Assuming standard vegetation NDVI > 0.4.
    # If mean NDVI drops below 0.2 in a monitored area, it's a concern.

    if mean_ndvi < 0.2:
        return "HIGH"
    elif mean_ndvi < 0.4:
        return "MEDIUM"

    return "LOW"
