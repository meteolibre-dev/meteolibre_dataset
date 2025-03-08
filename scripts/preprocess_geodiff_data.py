"""
python scripts to preprocess geodata (in order to have a map of height)
"""

import os
import rasterio
import numpy as np
import matplotlib.pyplot as plt
from rasterio.crs import CRS
from rasterio.warp import reproject, Resampling, calculate_default_transform
from pyproj import Transformer

# now get the array and plot it
import matplotlib.pyplot as plt


path = "/home/adrienbufort/data_assets/gebco_2024_n53.0_s41.0_w-8.0_e10.0.tif"
output_path = "reprojected_gebco_32630.tif"  # Define output file path
dst_crs = "EPSG:32630"  # Define target CRS

target_resolution = 500  # 500 meters
IMAGE_SIZE = 3472

# Open the raster file
with rasterio.open(path) as src:
    # Read the raster data
    data = src.read(1)
    # Get the metadata
    meta = src.meta

    # Update metadata for reprojection
    dst_crs = CRS.from_epsg(32630)  # Define target CRS object
    dst_transform, dst_width, dst_height = calculate_default_transform(
        src.crs,
        dst_crs,
        src.width,
        src.height,
        *src.bounds,
        resolution=(target_resolution, target_resolution),  # Specify 500m resolution
    )

    dst_meta = meta.copy()
    dst_meta.update(
        {
            "crs": dst_crs,
            "transform": dst_transform,
            "width": dst_width,
            "height": dst_height,
        }
    )

    # Create a NumPy array to hold the reprojected data
    dst_array = np.empty((dst_height, dst_width), dtype=src.dtypes[0])

    # Perform the reprojection
    reproject(
        source=rasterio.band(src, 1),
        destination=dst_array,
        src_transform=src.transform,
        src_crs=src.crs,
        dst_transform=dst_transform,
        dst_crs=dst_crs,
        resampling=Resampling.bilinear,  # Choose a resampling method (e.g., bilinear)
    )

    # Write the reprojected raster to a new file
    with rasterio.open(output_path, "w", **dst_meta) as dst:
        dst.write(dst_array, 1)

    print(f"Reprojected raster saved to: {output_path}")

print("shape dst_array", dst_array.shape)

# Open the reprojected raster file
with rasterio.open(output_path) as src:
    # Read the raster data
    data = src.read(1)

    # Plot the raster data
    plt.imshow(data, cmap="terrain")
    plt.colorbar(label="Elevation (meters)")
    plt.title("Reprojected GEBCO Data")
    plt.show()

    plt.savefig("reprojected_gebco_32630_500m.png")

# get the bound of the image
with rasterio.open(output_path) as src:
    bounds = src.bounds
    print(bounds)

# now we need to extand the image
# but first we need to compute what it the maximal extand of the image
EPSG = 32630

transformer_gps_to_lambert = Transformer.from_crs("EPSG:4326", f"EPSG:{EPSG}")
center_map_x, center_map_y = transformer_gps_to_lambert.transform(45.9, 3.2)

bottom_left_x = center_map_x - IMAGE_SIZE * target_resolution / 2
bottom_left_y = center_map_y - IMAGE_SIZE * target_resolution / 2
top_right_x = center_map_x + IMAGE_SIZE * target_resolution / 2
top_right_y = center_map_y + IMAGE_SIZE * target_resolution / 2
print(bottom_left_x, bottom_left_y, top_right_x, top_right_y)

# as we know the image should be IMAGE_SIZExIMAGE_SIZE pixels with target_resolutionm resolution and center at 3.2, 45.9
# we can compute the extand of the image
delta_left = (bottom_left_x - bounds.left) / target_resolution
delta_right = (bounds.right - top_right_x) / target_resolution
delta_bottom = (bottom_left_y - bounds.bottom) / target_resolution
delta_top = (bounds.top - top_right_y) / target_resolution
print(delta_left, delta_right, delta_bottom, delta_top)

# convert to int
delta_left = int(delta_left)
delta_right = int(delta_right)
delta_bottom = int(delta_bottom)
delta_top = int(delta_top)

# ok now we can pad the image with the delta values
final_image = np.zeros((IMAGE_SIZE, IMAGE_SIZE), dtype=np.uint16)
final_image[(-delta_top + 1):(IMAGE_SIZE + delta_bottom), :(IMAGE_SIZE + delta_right)] = data[:, delta_left:]

# then we put every negative value to 0
final_image[final_image < 0] = 0

# value > 100000 to 0
final_image[final_image > 10000] = 0

# save the image
plt.imshow(final_image, cmap="terrain")
plt.colorbar(label="Elevation (meters)")
plt.title("Reprojected GEBCO Data")
plt.savefig("reprojected_gebco_32630_500m_padded.png")

# also save the array in numpy format
np.save("reprojected_gebco_32630_500m_padded.npy", final_image)