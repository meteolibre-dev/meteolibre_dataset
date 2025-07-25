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
import time
import eumdac
import shutil
import pandas as pd
import zipfile
import glob
from nc_to_geotif import nc_to_geotiff
import rasterio
from rasterio.merge import merge
from google.cloud import storage
from urllib3.exceptions import ProtocolError

from preprocess_eumetsat import preprocess_eumetsat_file


def download_with_retry(product, max_retries=3, delay=10):
    """Downloads a product with a retry mechanism for network and file corruption errors."""

    with product.open() as fsrc, open(fsrc.name, mode='wb') as fdst:
        fsrc_name = fsrc.name
        shutil.copyfileobj(fsrc, fdst)
    print(f"Product {product} downloaded to {fsrc_name}.")

    return fsrc_name



def upload_to_gcp(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )


def download_and_process_eumetsat_data(begin_date, end_date, bounding_box, gcp_bucket_name):
    """
    Downloads, processes, and uploads EUMETSAT data for a given time range and bounding box.

    Args:
        begin_date (datetime.datetime): The start date for the data search.
        end_date (datetime.datetime): The end date for the data search.
        bounding_box (str): The bounding box for the data search.
        gcp_bucket_name (str): The name of the GCP bucket to upload to.
    """
    # Insert your personal key and secret
    consumer_key = os.environ.get('CONSUMER_KEY')
    consumer_secret = os.environ.get('CONSUMER_SECRET')

    credentials = (consumer_key, consumer_secret)
    token = eumdac.AccessToken(credentials)
    datastore = eumdac.DataStore(token)
    selected_collection = datastore.get_collection('EO:EUM:DAT:0662')

    print(selected_collection)

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

    df = pd.DataFrame({"product": list_product_str, "product_class": list_product})
    
    # selecting the second date to get a proper value 
    df["date"] = df["product"].str[(len(prefix) + 14 + 11):(len(prefix) + 14 + 11 + 14)]
    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d%H%M%S")

    time_index = pd.date_range(begin_date, end_date, freq='30min')
    time_df = pd.DataFrame(time_index, columns=['date'])

    df = df.sort_values('date')
    time_df = time_df.sort_values('date')

    matched_df = pd.merge_asof(time_df, df, on='date', direction='nearest')
    print(matched_df)

    unique_products_df = matched_df.dropna(subset=['product_class']).drop_duplicates(subset=['product'])

    channels_section1 = ['vis_04', 'vis_09', "nir_13", "nir_16", ]
    channels_section2 = ['ir_38', 'ir_87', 'wv_63']

    for index, row in unique_products_df.iterrows():
        product = row['product_class']
        try:
            fsrc_name = download_with_retry(product)
            if not fsrc_name:
                continue

            unzip_dir = 'tmp_unzip/'
            if fsrc_name.endswith('.zip'):
                if os.path.exists(unzip_dir):
                    shutil.rmtree(unzip_dir)
                os.makedirs(unzip_dir)

                with zipfile.ZipFile(fsrc_name, 'r') as zip_ref:
                    zip_ref.extractall(unzip_dir)
                print(f"Unzipped {fsrc_name} to {unzip_dir}")

                nc_files_to_process = []
                for suffix in ['34', '35', '36', '37', '38']:
                    suffix_nc = suffix + '.nc'
                    pattern = os.path.join(unzip_dir, '**', f'*{suffix_nc}')
                    nc_files_to_process.extend(glob.glob(pattern, recursive=True))

                tif_files_to_merge = []
                for nc_file in nc_files_to_process:
                    nc_file_idx = nc_file.split("_")[-1].split(".")[0]
                    tif_name = row['date'].strftime("%Y%m%d%H%M%S") + "_channelssection1_" + nc_file_idx + ".tif"
                    tif_path = "results_geotif/" + tif_name
                    
                    print(f"Processing file: {nc_file}")
                    nc_to_geotiff(nc_file, channels_section1, tif_path)
                    tif_files_to_merge.append(tif_path)
                    print(f"Processed file: {nc_file}")

                if len(tif_files_to_merge) > 1:
                    src_files_to_mosaic = []
                    for tif_file in tif_files_to_merge:
                        src = rasterio.open(tif_file)
                        src_files_to_mosaic.append(src)

                    mosaic, out_trans = merge(src_files_to_mosaic)
                    merged_tif_name = row['date'].strftime("%Y%m%d%H%M%S") + "_channelssection1_merged.tif"
                    out_path = "results_geotif/" + merged_tif_name

                    out_meta = src_files_to_mosaic[0].meta.copy()
                    out_meta.update({"driver": "GTiff",
                                     "height": mosaic.shape[1],
                                     "width": mosaic.shape[2],
                                     "transform": out_trans,
                                     })

                    with rasterio.open(out_path, "w", **out_meta) as dest:
                        dest.write(mosaic)

                    for src in src_files_to_mosaic:
                        src.close()

                    print(f"Merged {len(tif_files_to_merge)} files into {out_path}")

                    # Preprocess and upload the merged tif
                    output_dir = "preprocessed_eumetsat"
                    preprocess_eumetsat_file(out_path, output_dir)
                    
                    base_name = os.path.splitext(os.path.basename(out_path))[0]
                    npz_file = os.path.join(output_dir, f"{base_name}_epsg32630_france.npz")
                    if os.path.exists(npz_file):
                        destination_blob_name = f"eumetsat_data/{os.path.basename(npz_file)}"
                        upload_to_gcp(gcp_bucket_name, npz_file, destination_blob_name)
                    else:
                        print(f"Could not find {npz_file} to upload.")

                shutil.rmtree(unzip_dir)
                os.remove(fsrc_name)
                print(f"Cleaned up {fsrc_name} and {unzip_dir}")
                
                # remove all the npz files in preprocessed_eumetsat/ and results_geotif/
                for file in glob.glob("preprocessed_eumetsat/*.npz"):
                    os.remove(file)
                for file in glob.glob("results_geotif/*.tif"):
                    os.remove(file)

            else:
                print(f"Downloaded file {fsrc_name} is not a zip file, skipping preprocessing.")
                os.remove(fsrc_name)

        except Exception as e:
            print(f"An error occurred while processing {product}: {e}")
