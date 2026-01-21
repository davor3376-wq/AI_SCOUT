import os
import glob
import logging
import numpy as np
import rasterio
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
import matplotlib.cm as cm
import matplotlib.colors as colors
from PIL import Image
import io

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PDFReportGenerator:
    def __init__(self, input_dir="data/processed", output_dir="results"):
        self.input_dir = input_dir
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def generate_pdf(self, filename="report.pdf", specific_files=None):
        output_path = os.path.join(self.output_dir, filename)
        c = canvas.Canvas(output_path, pagesize=letter)
        width, height = letter

        # Title
        c.setFont("Helvetica-Bold", 24)
        c.drawString(50, height - 50, "Project Gaia - Environmental Report")

        c.setFont("Helvetica", 12)
        c.drawString(50, height - 80, "Analysis & Evidence Pack")

        y_position = height - 120

        # Find TIF files
        if specific_files:
            tif_files = specific_files
        else:
            tif_files = glob.glob(os.path.join(self.input_dir, "*_NDVI_analysis.tif"))

        # Separate Weather and Analysis files
        weather_file = None
        analysis_files = []

        for f in tif_files:
            if "ERA5" in f:
                weather_file = f
            else:
                analysis_files.append(f)

        # Weather Section (Atmosphere)
        if weather_file:
            try:
                 with rasterio.open(weather_file) as src:
                     temp_band = src.read(1)
                     precip_band = src.read(2)

                     # Simple average stats
                     avg_temp = np.nanmean(temp_band) - 273.15 # Kelvin to Celsius
                     avg_precip = np.nanmean(precip_band) * 1000 # m to mm

                     c.setFont("Helvetica-Bold", 14)
                     c.drawString(50, y_position, "Atmospheric Context (ERA5)")
                     y_position -= 20
                     c.setFont("Helvetica", 12)
                     c.drawString(50, y_position, f"Avg Temperature: {avg_temp:.2f} °C")
                     y_position -= 15
                     c.drawString(50, y_position, f"Avg Precipitation: {avg_precip:.2f} mm")
                     y_position -= 40
            except Exception as e:
                logger.warning(f"Failed to read weather file {weather_file}: {e}")

        if not analysis_files:
            logger.warning("No Analysis files found to generate report.")
            c.drawString(50, y_position, "No analysis data available.")
            c.save()
            return

        for filepath in analysis_files:
            try:
                img_buffer = self._create_image_from_tif(filepath)

                if y_position < 300: # New page if low on space
                    c.showPage()
                    y_position = height - 50

                # Draw Image
                img_reader = ImageReader(img_buffer)
                img_w, img_h = 256, 256 # Fixed display size

                # Centered image
                x_pos = (width - img_w) / 2
                c.drawImage(img_reader, x_pos, y_position - img_h, width=img_w, height=img_h)

                # Caption
                c.setFont("Helvetica-Bold", 10)
                filename_only = os.path.basename(filepath)
                if "CHANGE" in filename_only:
                    c.setFillColorRGB(0.8, 0, 0) # Red for Chronos
                    c.drawString(50, y_position + 10, f"Chronos Change Detection: {filename_only}")
                    c.setFillColorRGB(0, 0, 0)
                else:
                    c.drawString(50, y_position + 10, f"Analysis: {filename_only}")

                y_position -= (img_h + 50)

            except Exception as e:
                logger.error(f"Failed to process {filepath} for PDF: {e}")
                c.drawString(50, y_position, f"Error processing {os.path.basename(filepath)}")
                y_position -= 30

        c.save()
        logger.info(f"PDF Report generated at {output_path}")

    def _create_image_from_tif(self, filepath):
        with rasterio.open(filepath) as src:
            data = src.read(1)

            # Handling 10,000x Integer Scaling Check
            # If the data is float but clearly has values >> 1 (like 2000, 5000), it's likely scaled.
            # Normal NDVI is -1 to 1.
            # If we see values > 100, we assume it's scaled by 10000.
            # (Using 100 as a safe threshold, assuming no valid NDVI is 100).
            # Note: TIF nodata is often NaN, but could be a specific value.

            # Filter out NaN for min/max check
            valid_data = data[~np.isnan(data)]
            if valid_data.size > 0:
                max_val = np.max(valid_data)
                if max_val > 100: # Arbitrary threshold for "this is definitely not -1 to 1"
                    logger.info(f"Detected scaled integer data in {filepath} (max={max_val}). Dividing by 10,000.")
                    data = data / 10000.0

            # Apply colormap RdYlGn
            # Normalize data to 0-1 range for colormap
            # RdYlGn: Red (low) -> Green (high).
            # NDVI: -1 (water/snow) to 1 (vegetation).
            # Usually we map -1..1 to 0..1.

            norm = colors.Normalize(vmin=-1.0, vmax=1.0)
            cmap = cm.get_cmap('RdYlGn')

            # Map data to colors. masked=True handles NaNs usually, but we need to check
            # if we need to explicitly handle NaNs (make them transparent or specific color)

            # Using matplotlib to map
            colored_data = cmap(norm(data))

            # colored_data is (H, W, 4) float array (0..1)

            # Handle NaNs: make them transparent or white
            # data is the original array.
            # Create a mask for NaN
            nan_mask = np.isnan(data)
            colored_data[nan_mask] = [0, 0, 0, 0] # Transparent

            # Convert to uint8 (0-255)
            img_data = (colored_data * 255).astype(np.uint8)

            # Create PIL Image
            img = Image.fromarray(img_data, 'RGBA')

            # Save to buffer
            buffer = io.BytesIO()
            img.save(buffer, format='PNG')
            buffer.seek(0)

            return buffer

if __name__ == "__main__":
    # Test run
    gen = PDFReportGenerator()
    gen.generate_pdf()
