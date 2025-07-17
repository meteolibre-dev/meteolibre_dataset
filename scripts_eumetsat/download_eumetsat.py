"""
This is a python module to download and preprocess eumetsat dataset

This will download and preprocess the different information about eumetsat satellite data.
Put it simply this script will :
1. Download satellite data for a specific time 
2. Preprocess it to get only the interesting part (europe)
3. save it somewhere (bucket)
"""
import datetime
import os
import eumdac
import shutil
import pandas as pd
import zipfile
import glob
from nc_to_geotif import nc_to_geotiff
import rasterio
from rasterio.merge import merge

begin_date = datetime.datetime(2025, 1, 10, 9, 0) # start of 2025
end_date = datetime.datetime(2025, 1, 12, 15, 0) # end of 2025

# Insert your personal key and secret
consumer_key = os.environ.get('CONSUMER_KEY')
consumer_secret = os.environ.get('CONSUMER_SECRET')

bounding_box = '-10, 39, 12, 52'

credentials = (consumer_key, consumer_secret)

token = eumdac.AccessToken(credentials)

datastore = eumdac.DataStore(token)

selected_collection = datastore.get_collection('EO:EUM:DAT:0662')

print(selected_collection)

# Filter by satellite/platform
satellite = 'MTG'


# Retrieve datasets that match our filter
products = selected_collection.search(
    bbox=bounding_box,
    dtstart=begin_date, 
    dtend=end_date,
    )
    
print(f'Found Datasets: {products.total_results} datasets for the given time range')

list_product_str = []
list_product = []

prefix = "W_XX-EUMETSAT-Darmstadt,IMG+SAT,MTI1+FCI-1C-RRAD-FDHSI-FD--x-x---x_C_EUMT_"

for product in products:
    if prefix in str(product):
        list_product_str.append(str(product))
        list_product.append(product)

df = pd.DataFrame({"product": list_product_str, "product_class" : list_product})
df["date"] = df["product"].str[len(prefix):(len(prefix) + 14)]

# convert 20250131235310 date format into proper datetime
df["date"] = pd.to_datetime(df["date"], format="%Y%m%d%H%M%S")

# now we want to create a array of time series between begin_date and end_date with 30min of interval
# and then match this time with the closest element in product (closest time wise)

# Create a DataFrame with the desired time series
time_index = pd.date_range(begin_date, end_date, freq='30min')
time_df = pd.DataFrame(time_index, columns=['date'])

# Sort both dataframes by date
df = df.sort_values('date')
time_df = time_df.sort_values('date')

# Use merge_asof to find the nearest product for each time step
matched_df = pd.merge_asof(time_df, df, on='date', direction='nearest')

# save the file somewhere in parquet format
# matched_df.to_parquet('matched_df.parquet')

# Now matched_df contains the list of products that are closest to each 30-minute interval
print(matched_df)

# now we loop over the product and we download them one per one (and preprocess them to avoid that they take too much space)
unique_products_df = matched_df.dropna(subset=['product_class']).drop_duplicates(subset=['product'])

channels_section1 = ['vis_04', 'vis_09', "nir_13", "nir_16", ]
channels_section2 = ["wv_63", "ir_38", "ir_87"]

for index, row in unique_products_df.iterrows():
    product = row['product_class']
    try:
        print(f"Downloading product: {product}")
        with product.open() as fsrc, open(fsrc.name, mode='wb') as fdst:
            shutil.copyfileobj(fsrc, fdst)
            pass
        print(f"Product {product} downloaded to {fsrc.name}.")

        # Preprocessing steps
        unzip_dir = 'tmp_unzip/'
        if fsrc.name.endswith('.zip'):
            # 1. Unzip the archive if the product is a ZIP file.
            if os.path.exists(unzip_dir):
                shutil.rmtree(unzip_dir)
            os.makedirs(unzip_dir)

            with zipfile.ZipFile(fsrc.name, 'r') as zip_ref:
                zip_ref.extractall(unzip_dir)
            print(f"Unzipped {fsrc.name} to {unzip_dir}")

            # 2. Open the data file (e.g., using xarray, rasterio, or other relevant libraries).
            # We are looking for .nc files ending with specific numbers as requested.
            nc_files_to_process = []
            for suffix in ['34', '35', '36', '37', '38']:
                suffix_nc = suffix + '.nc'
                # Search for files recursively in the unzipped directory
                pattern = os.path.join(unzip_dir, '**', f'*{suffix_nc}')
                nc_files_to_process.extend(glob.glob(pattern, recursive=True))
            
            tif_files_to_merge = []
            for nc_file in nc_files_to_process:

                # 3. Crop the data to the desired bounding box.
                # 4. Save the preprocessed file, possibly in a different format like NetCDF or Zarr.
                # The nc_to_tif function handles cropping and saving as a GeoTIFF.
                # Using 'vis_04' as a default channel as it is not specified in the requirements.
                nc_file_idx = nc_file.split("_")[-1].split(".")[0]
   
                tif_name = row['date'].strftime("%Y%m%d%H%M%S") + "_channelssection1_" + nc_file_idx + ".tif"
                tif_path = "results_geotif/" + tif_name
                
                print(f"Processing file: {nc_file}")
                nc_to_geotiff(nc_file, channels_section1, tif_path)
                tif_files_to_merge.append(tif_path)
                print(f"Processed file: {nc_file}")


            # Merge the GeoTIFF files
            if len(tif_files_to_merge) > 1:
                src_files_to_mosaic = []
                for tif_file in tif_files_to_merge:
                    src = rasterio.open(tif_file)
                    src_files_to_mosaic.append(src)
                
                mosaic, out_trans = merge(src_files_to_mosaic)

                # Define the output file path
                merged_tif_name = row['date'].strftime("%Y%m%d%H%M%S") + "_channelssection1_merged.tif"
                out_path = "results_geotif/" + merged_tif_name

                # Copy the metadata
                out_meta = src_files_to_mosaic[0].meta.copy()

                # Update the metadata
                out_meta.update({"driver": "GTiff",
                                 "height": mosaic.shape[1],
                                 "width": mosaic.shape[2],
                                 "transform": out_trans,
                                 })

                # Write the mosaic raster to disk
                with rasterio.open(out_path, "w", **out_meta) as dest:
                    dest.write(mosaic)

                # Close the source files
                for src in src_files_to_mosaic:
                    src.close()
                
                print(f"Merged {len(tif_files_to_merge)} files into {out_path}")

            # 6. Clean up the original downloaded file and any extracted files to save space.
            shutil.rmtree(unzip_dir)
            os.remove(fsrc.name)
            print(f"Cleaned up {fsrc.name} and {unzip_dir}")
        else:
            print(f"Downloaded file {fsrc.name} is not a zip file, skipping preprocessing.")
            # 6. Clean up the original downloaded file.
            os.remove(fsrc.name)
            
        break

    except eumdac.errors.ProductNotFoundError as e:
        print(f"Could not find product {product}: {e}")
    except Exception as e:
        print(f"An error occurred while processing {product}: {e}")
