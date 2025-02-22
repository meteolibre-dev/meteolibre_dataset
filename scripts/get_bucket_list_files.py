import os
from google.cloud import storage
import google.auth


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

# put the list into a csv file
with open('list_files.csv', 'w') as f:
    for name in list_name:
        f.write(name + '\n')