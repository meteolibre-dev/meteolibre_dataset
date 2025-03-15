import os
from tqdm import tqdm

import pandas as pd

from google.cloud import storage
import google.auth


def download_file(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    try:
        credentials, project = google.auth.default()
        storage_client = storage.Client(credentials=credentials)

        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_name)

        print(
            "Downloaded storage object {} from bucket {} to filename {}.".format(
                source_blob_name,
                bucket_name,
                destination_file_name,
            )
        )

    except Exception as e:
        print(
            f"Failed to download gs://{bucket_name}/{source_blob_name} to {destination_file_name}: {e}"
        )


def main():
    csv_file = "list_files.parquet"
    output_dir = "../data/h5"  # Directory to save the downloaded files

    # Create the output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    try:
        df = pd.read_parquet(csv_file)

        # filter the csv to keep only the h5 files
        df = df[df["name"].str.endswith(".h5")]

        # Assuming the CSV has columns named 'bucket_name' and 'name'
        for index, row in tqdm(
            df.iterrows(), total=len(df), desc="Downloading files", unit="file"
        ):
            bucket_name = "meteofrancedata"
            name = row["name"]
            if name.endswith(".h5"):
                filename = os.path.basename(name)
                output_path = os.path.join(output_dir, filename)
                download_file(bucket_name, name, output_path)

    except FileNotFoundError:
        print(f"Error: CSV file not found at {csv_file}")
    except pd.errors.EmptyDataError:
        print(f"Error: CSV file is empty at {csv_file}")
    except KeyError as e:
        print(
            f"Error: Missing column in CSV file: {e}. Please check the CSV file's structure."
        )
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    main()
