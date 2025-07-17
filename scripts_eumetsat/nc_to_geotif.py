import netCDF4 as nc
import numpy as np
import os
from osgeo import gdal, osr
import argparse

def find_variable(group, var_name):
    """Recursively search for a variable in a group and its subgroups."""
    if var_name in group.variables:
        return group.variables[var_name]
    for subgroup in group.groups.values():
        found = find_variable(subgroup, var_name)
        if found is not None:
            return found
    return None

def create_srs_from_grid_mapping(grid_mapping_var):
    """Creates an OSR SpatialReference from a CF-compliant grid_mapping variable."""
    srs = osr.SpatialReference()
    
    attrs = grid_mapping_var.ncattrs()

    if 'crs_wkt' in attrs:
        srs.ImportFromWkt(grid_mapping_var.crs_wkt)
        return srs

    if 'grid_mapping_name' in attrs and grid_mapping_var.grid_mapping_name == 'geostationary':

        # The SetGeostationary method is deprecated. We build a Proj string instead.
        proj_string = (
            f"+proj=geos "
            f"+h={grid_mapping_var.perspective_point_height} "
            f"+lon_0={grid_mapping_var.longitude_of_projection_origin} "
            f"+sweep={grid_mapping_var.sweep_angle_axis} "
            f"+a={grid_mapping_var.semi_major_axis} "
            f"+b={grid_mapping_var.semi_minor_axis} "
            f"+units=m +no_defs"
        )
        srs.ImportFromProj4(proj_string)
    else:
        raise ValueError("Unsupported or missing grid mapping information.")
    return srs

def nc_to_geotiff(nc_file, channel_names, output_file, lon_min=-10.0, lon_max=12.0):
    """
    Converts specific channels from an MTG L1C NetCDF file to a georeferenced GeoTIFF
    by reprojecting the data to a standard Lat/Lon grid (EPSG:4326) and cropping
    to the specified longitude range.
    """
    stacked_radiance_data = None
    native_srs = None
    x, y = None, None

    with nc.Dataset(nc_file, 'r') as dataset:
        print("Reading data from NetCDF...")
        # ... (The data reading part remains the same) ...
        main_data_group = dataset.groups.get('data')
        if not main_data_group:
            raise ValueError("Main data group 'data' not found in the NetCDF file.")

        all_radiance_data = []
        for channel_name in channel_names:
            channel_group = main_data_group.groups.get(channel_name)
            if not channel_group:
                print(f"Warning: Channel group '{channel_name}' not found. Skipping.")
                continue

            if "measured" in channel_group.groups:
                data_source_group = channel_group.groups["measured"]
            else:
                data_source_group = channel_group
            
            if 'effective_radiance' not in data_source_group.variables:
                print(f"Warning: 'effective_radiance' not found in {channel_name}. Skipping.")
                continue
            
            radiance_var = data_source_group.variables['effective_radiance']
            radiance_data = radiance_var[:]
            all_radiance_data.append(radiance_data)

        if not all_radiance_data:
            print("Error: No valid channel data was loaded.")
            return

        stacked_radiance_data = np.ma.stack(all_radiance_data, axis=0)

        first_channel_group = main_data_group.groups.get(channel_names[0])
        if "measured" in first_channel_group.groups:
            coord_source_group = first_channel_group.groups["measured"]
        else:
            coord_source_group = first_channel_group

        x_rad = - coord_source_group.variables['x'][:]  # Assume these are radians
        y_rad = coord_source_group.variables['y'][:]  # Assume these are radians
        radiance_var_ref = coord_source_group.variables['effective_radiance']
        grid_mapping_name = radiance_var_ref.grid_mapping
        grid_mapping_var = find_variable(dataset, grid_mapping_name)
        
        # --- FIX: Convert coordinates from radians to meters ---
        h = grid_mapping_var.perspective_point_height
        x = x_rad * h
        y = y_rad * h
        # --- END FIX ---

        print("Creating source spatial reference...")
        native_srs = create_srs_from_grid_mapping(grid_mapping_var)

    # Now we use the corrected x and y (in meters) to build the geotransform
    num_bands, height, width = stacked_radiance_data.shape
    mem_driver = gdal.GetDriverByName('MEM')
    src_ds = mem_driver.Create('', width, height, num_bands, gdal.GDT_Float32)

    # The geotransform calculation now uses the correct meter-based coordinates
    x_res = (x[-1] - x[0]) / (len(x) - 1)
    y_res = (y[-1] - y[0]) / (len(y) - 1) # This will be negative, which is correct
    geotransform = (x[0] - x_res/2, x_res, 0, y[0] - y_res/2, 0, y_res)
    
    src_ds.SetGeoTransform(geotransform)
    src_ds.SetProjection(native_srs.ExportToWkt())

    # ... (The rest of the script for writing data and warping remains the same) ...
    nodata_val = -9999.0
    for i in range(num_bands):
        band = src_ds.GetRasterBand(i + 1)
        band_data = stacked_radiance_data[i, :, :].filled(nodata_val)
        band.WriteArray(band_data)
        band.SetNoDataValue(nodata_val)
        band.SetDescription(channel_names[i])

    print(f"Warping and cropping image to longitude range [{lon_min}, {lon_max}] -> {output_file}...")
    
    # Define output bounds for cropping: [min_x, min_y, max_x, max_y] in target CRS (EPSG:4326)
    # For longitude cropping, we set a wide latitude range to include all available data
    output_bounds = [lon_min, -90.0, lon_max, 90.0]
    
    gdal.Warp(
        output_file,
        src_ds,
        format='GTiff',
        dstSRS='EPSG:4326',
        outputBounds=output_bounds,
        resampleAlg=gdal.GRIORA_Bilinear,
        dstNodata=nodata_val,
        creationOptions=['COMPRESS=LZW', 'TILED=YES']
    )
    
    src_ds = None
    
    print(f"✅ Successfully created final GeoTIFF: {output_file}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Correctly process chunked EUMETSAT L1C NetCDF to a cropped GeoTIFF.')
    parser.add_argument('--file', type=str, help='Path to the NetCDF file.', required=True)
    parser.add_argument('--channels', nargs='+', type=str, default=['vis_04', 'vis_09', "nir_13", "nir_16"], help='List of channels to convert.')
    parser.add_argument('--output', type=str, help='Output file name for the GeoTIFF.')
    parser.add_argument('--lon_min', type=float, default=-10.0, help='Minimum longitude for crop.')
    parser.add_argument('--lon_max', type=float, default=12.0, help='Maximum longitude for crop.')

    args = parser.parse_args()
    output_file = args.output
    if not output_file:
        base_name = os.path.basename(args.file)
        file_name, _ = os.path.splitext(base_name)
        channels_str = "_".join(args.channels)
        output_file = f"{file_name}_{channels_str}_final.tif"

    nc_to_geotiff(args.file, args.channels, output_file, args.lon_min, args.lon_max)
