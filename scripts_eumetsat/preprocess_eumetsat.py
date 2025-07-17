"""
Python module to preprocess EUMETSAT data.
"""

import os
import rasterio
import numpy as np
import matplotlib.pyplot as plt
from rasterio.crs import CRS
from rasterio.warp import reproject, Resampling
from rasterio import Affine
from pyproj import Transformer

# --- Global Constants ---
EPSG = 32630
TARGET_RESOLUTION = 500  # meters
IMAGE_SIZE = 3472  # pixels
DST_CRS = f"EPSG:{EPSG}"
CENTER_LON, CENTER_LAT = 3.2, 45.9 # Center point for the final image

def preprocess_eumetsat_file(input_path, output_dir="."):
    """
    Preprocesses a EUMETSAT GeoTIFF file by reprojecting it directly
    to a target grid centered over France in EPSG:32630.

    The process includes:
    1. Defining a target grid (dimensions, CRS, transform).
    2. Reprojecting the source data directly into the target grid.
    3. Saving the final data as a compressed NumPy array.
    4. Generating and saving a visualization of the final data (first band).
    5. Saving the reprojected data as a GeoTIFF for verification.

    Args:
        input_path (str): The path to the EUMETSAT GeoTIFF file.
        output_dir (str): Directory to save the output files.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    base_name = os.path.splitext(os.path.basename(input_path))[0]
    output_geotiff_path = os.path.join(output_dir, f"{base_name}_epsg{EPSG}_france.tif")
    output_plot_path = os.path.join(output_dir, f"{base_name}_epsg{EPSG}_france.png")
    output_npz_path = os.path.join(output_dir, f"{base_name}_epsg{EPSG}_france.npz")

    print(f"--- Starting processing for EUMETSAT file ---")
    print(f"Input file: {input_path}")

    # --- 1. Define Target Grid ---
    # Transformer to get the center point in the target CRS
    transformer = Transformer.from_crs("EPSG:4326", DST_CRS, always_xy=True)
    center_x, center_y = transformer.transform(CENTER_LON, CENTER_LAT)

    # Calculate the top-left corner of the target grid
    half_size_meters = IMAGE_SIZE * TARGET_RESOLUTION / 2
    left = center_x - half_size_meters
    top = center_y + half_size_meters

    # Define the target transform (affine transformation)
    dst_transform = Affine(TARGET_RESOLUTION, 0.0, left,
                           0.0, -TARGET_RESOLUTION, top)

    # --- 2. Reproject Data ---
    with rasterio.open(input_path) as src:
        print(f"Original raster size: {src.width}x{src.height}, CRS: {src.crs}, Bands: {src.count}")

        # Create an empty array for the destination data
        dst_array = np.empty((src.count, IMAGE_SIZE, IMAGE_SIZE), dtype=src.dtypes[0])

        reproject(
            source=rasterio.band(src, range(1, src.count + 1)),
            destination=dst_array,
            src_transform=src.transform,
            src_crs=src.crs,
            dst_transform=dst_transform,
            dst_crs=DST_CRS,
            resampling=Resampling.bilinear
        )
        print(f"Final image shape after reprojection: {dst_array.shape}")

        # Copy metadata for the output GeoTIFF
        dst_meta = src.meta.copy()
        dst_meta.update({
            "crs": DST_CRS,
            "transform": dst_transform,
            "width": IMAGE_SIZE,
            "height": IMAGE_SIZE,
            "count": src.count,
            "driver": "GTiff"
        })

    breakpoint()
    
    # replace -9999 with -10
    dst_array[dst_array == -9999] = -10


    # --- 3. Save Final Data (NumPy Array) ---
    np.savez_compressed(output_npz_path, dst_array)
    print(f"Saved final data to: {output_npz_path}")

    # --- 4. Plot Final Data (First Band) ---
    plt.figure(figsize=(12, 10))
    plt.imshow(dst_array[0], cmap="gray")
    plt.colorbar(label="Channel Value")
    plt.title(f"Final EUMETSAT Data (EPSG:{EPSG})")
    plt.xlabel("Easting (m)"); plt.ylabel("Northing (m)")
    plt.savefig(output_plot_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Saved final plot to: {output_plot_path}")

    # --- 5. Save Reprojected GeoTIFF ---
    with rasterio.open(output_geotiff_path, "w", **dst_meta) as dst:
        dst.write(dst_array)
    print(f"Reprojected GeoTIFF saved to: {output_geotiff_path}")
    print(f"--- Finished processing for EUMETSAT file ---\n")


def main():
    """
    Main function to process the EUMETSAT file.
    """
    tiff_file = "/home/adrienbufort/meteolibre_dataset/scripts_eumetsat/results_geotif/20250110090000_channelssection1_merged.tif"
    output_directory = "preprocessed_eumetsat"
    
    try:
        preprocess_eumetsat_file(tiff_file, output_directory)
    except Exception as e:
        print(f"An error occurred while processing {tiff_file}: {e}")
    
    print("All processing complete!")

if __name__ == "__main__":
    main()
