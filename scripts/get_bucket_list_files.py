import os
from google.cloud import storage
import google.auth

import pandas as pd

bucket_name = "meteofrancedata"

## llist the files in the bucket
# Check if running on Google Cloud Environment
if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") is None:
    # Load credentials from a service account key file if not on GCE
    try:
        credentials, project = google.auth.default()
        storage_client = storage.Client(credentials=credentials)
    except google.auth.exceptions.DefaultCredentialsError:
        print("Could not automatically determine credentials.  Please set GOOGLE_APPLICATION_CREDENTIALS or ensure you are running in a Google Cloud environment.")
        storage_client = None # Handle the case where credentials are not available
else:
    storage_client = storage.Client()

if storage_client:
    bucket = storage_client.bucket(bucket_name)

    blob_list = bucket.list_blobs()

    list_name = []

    # save all the name
    for blob in blob_list:
        list_name.append(blob.name)

else:
    print("Storage client not initialized. Check credentials.")
    list_name = []

# count the number of element in the bucket
print(len(list_name))

# create a dataframe
df = pd.DataFrame(list_name, columns=['name'])

# the file name look like T_IMFR27_C_LFPW_20250103224500.bufr.gz
# we want to extract the date from the filename
df["date"] = df["name"].str.extract(r"(\d{12})")

# convert the date to datetime
df["date"] = pd.to_datetime(df["date"], format="%Y%m%d%H%M")
# sort the dataframe by date
df = df.sort_values(by="date")

# keep only the value that have a frequency of 1 hour or 30 minutes
df = df[df["date"].dt.minute.isin([0, 30])]

print("number of files : ", len(df))

# save the dataframe
df.to_parquet('list_files.parquet', index=False)