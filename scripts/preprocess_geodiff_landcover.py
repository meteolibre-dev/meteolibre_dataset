"""
python scripts to preprocess geodata (in order to have a map of height)
"""

import os
import rasterio
import numpy as np
import matplotlib.pyplot as plt
from rasterio.crs import CRS
from rasterio.warp import reproject, Resampling, calculate_default_transform, transform_bounds
from rasterio.windows import from_bounds
from pyproj import Transformer

# --- Global Constants ---
EPSG = 32630
TARGET_RESOLUTION = 500  # meters
IMAGE_SIZE = 3472  # pixels
DST_CRS = f"EPSG:{EPSG}"

# Define France bounding box in WGS84 (lat/lon)
FRANCE_BOUNDS_WGS84 = {
    'west': -7.8,
    'east': 14.2,
    'south': 37.9,
    'north': 53.9
}

def process_landcover_file(class_number):
    """
    Preprocesses a landcover GeoTIFF file for a given class number.

    The process includes:
    1. Cropping the source raster to the bounds of France.
    2. Reprojecting the cropped raster to the target CRS (EPSG:32630).
    3. Saving the reprojected raster as a new GeoTIFF file.
    4. Generating and saving a visualization of the reprojected data.
    5. Cropping the reprojected data to a fixed size around a central point.
    6. Generating and saving a visualization of the final cropped data.

    Args:
        class_number (int): The class number of the landcover file to process.
    """
    input_path = f"../data/assets/consensus_full_class_{class_number}.tif"
    output_path = f"consensus_full_class_{class_number}_32630_france.tif"
    output_plot_path = f"consensus_full_class_{class_number}_france.png"
    output_cropped_plot_path = f"consensus_full_class_{class_number}_france_padded.png"
    output_cropped_plot_path_npz = f"consensus_full_class_{class_number}_france_padded.npz"

    print(f"--- Starting processing for landcover class {class_number} ---")
    print(f"Input file: {input_path}")

    # In a real-world scenario, you'd check for file existence:
    # if not os.path.exists(input_path):
    #     print(f"Warning: Input file not found at {input_path}. Skipping.")
    #     return

    # --- 1. Crop to France & 2. Reproject ---
    with rasterio.open(input_path) as src:
        print(f"Original raster size: {src.width} x {src.height}, CRS: {src.crs}")

        france_bounds_src = transform_bounds(
            CRS.from_epsg(4326), src.crs,
            FRANCE_BOUNDS_WGS84['west'], FRANCE_BOUNDS_WGS84['south'],
            FRANCE_BOUNDS_WGS84['east'], FRANCE_BOUNDS_WGS84['north']
        )
        france_window = from_bounds(*france_bounds_src, src.transform)
        france_data = src.read(1, window=france_window)
        france_transform = src.window_transform(france_window)
        print(f"Cropped France data shape: {france_data.shape}")

        dst_transform, dst_width, dst_height = calculate_default_transform(
            src.crs, DST_CRS, france_window.width, france_window.height,
            *france_bounds_src, resolution=(TARGET_RESOLUTION, TARGET_RESOLUTION)
        )
        print(f"Destination size after reprojection: {dst_width} x {dst_height}")

        dst_meta = src.meta.copy()
        dst_meta.update({
            "crs": DST_CRS, "transform": dst_transform,
            "width": dst_width, "height": dst_height,
            'dtype': src.dtypes[0]
        })

        dst_array = np.empty((dst_height, dst_width), dtype=src.dtypes[0])
        reproject(
            source=france_data, destination=dst_array,
            src_transform=france_transform, src_crs=src.crs,
            dst_transform=dst_transform, dst_crs=DST_CRS,
            resampling=Resampling.bilinear
        )

    # --- 3. Save Reprojected GeoTIFF ---
    with rasterio.open(output_path, "w", **dst_meta) as dst:
        dst.write(dst_array, 1)
    print(f"Reprojected raster saved to: {output_path}")

    # --- 4. Plot Reprojected Data ---
    plt.figure(figsize=(12, 8))
    plt.imshow(dst_array, cmap="terrain")
    plt.colorbar(label="Landcover Class")
    plt.title(f"Reprojected Landcover Class {class_number} - France (EPSG:{EPSG})")
    plt.xlabel("Easting (m)"); plt.ylabel("Northing (m)")
    plt.savefig(output_plot_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Saved plot to {output_plot_path}")

    # --- 5. Crop to Final Size ---
    with rasterio.open(output_path) as src:
        bounds = src.bounds
        data = src.read(1)

        transformer = Transformer.from_crs("EPSG:4326", DST_CRS, always_xy=True)
        center_map_x, center_map_y = transformer.transform(3.2, 45.9)

        target_half_size = IMAGE_SIZE * TARGET_RESOLUTION / 2
        target_bounds = {
            'left': center_map_x - target_half_size,
            'right': center_map_x + target_half_size,
            'bottom': center_map_y - target_half_size,
            'top': center_map_y + target_half_size,
        }

        # Correctly calculate pixel offsets for cropping
        y_start = round((bounds.top - target_bounds['top']) / TARGET_RESOLUTION)
        y_end = y_start + IMAGE_SIZE
        x_start = round((target_bounds['left'] - bounds.left) / TARGET_RESOLUTION)
        x_end = x_start + IMAGE_SIZE

        print(f"Cropping window (pixels): y=[{y_start}:{y_end}], x=[{x_start}:{x_end}]")

        # Ensure window is within data bounds
        y_start_clipped, y_end_clipped = max(0, y_start), min(data.shape[0], y_end)
        x_start_clipped, x_end_clipped = max(0, x_start), min(data.shape[1], x_end)

        # Create a zero-padded array and place the cropped data inside
        final_image = np.zeros((IMAGE_SIZE, IMAGE_SIZE), dtype=data.dtype)
        
        paste_y_start = max(0, -y_start)
        paste_y_end = paste_y_start + (y_end_clipped - y_start_clipped)
        paste_x_start = max(0, -x_start)
        paste_x_end = paste_x_start + (x_end_clipped - x_start_clipped)

        final_image[paste_y_start:paste_y_end, paste_x_start:paste_x_end] = \
            data[y_start_clipped:y_end_clipped, x_start_clipped:x_end_clipped]

        print(f"Final image shape: {final_image.shape}")

    # --- 6. Plot Final Cropped Data ---
    plt.figure(figsize=(12, 8))
    plt.imshow(final_image, cmap="terrain")
    plt.colorbar(label="Landcover Class")
    plt.title(f"Final Cropped Landcover Class {class_number} (EPSG:{EPSG})")
    plt.xlabel("Easting (m)"); plt.ylabel("Northing (m)")
    plt.savefig(output_cropped_plot_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Saved final plot to {output_cropped_plot_path}")
    print(f"--- Finished processing for landcover class {class_number} ---\n")

    # also save the image as a compressed version
    np.savez_compressed(output_cropped_plot_path_npz, final_image)


def main():
    """
    Main function to iterate through and process all specified landcover classes.
    """
    landcover_classes = [4, 7, 9, 12]
    for class_number in landcover_classes:
        try:
            process_landcover_file(class_number)
        except Exception as e:
            print(f"An error occurred while processing class {class_number}: {e}")
            # Continue to the next class
            continue
    print("All processing complete!")

if __name__ == "__main__":
    main()