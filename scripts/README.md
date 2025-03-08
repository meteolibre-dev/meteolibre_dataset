Scripts :


## To retrieve the radar info from the gcp bucket

- get_bucket_list_files.py : script to get the list of files (h5 / radar info) in the gcp bucket. Get a csv file with the list of files
- download_h5_files.py : script to download the h5 files from the gcp bucket (with the list of files)

## Ground stations data

- preprocess_download_groundstations.ipynb : script to download the ground stations data from the data gouv fr website
- preprocess_groundstations.py : script to preprocess the ground stations data (filter on datetime, compute the coordinates of the stations, save it somewhere)

## Other scripts

- preprocess_geodiff_data.py : script to preprocess the geodiff data to get the elevation data


## Final

- index_creation.py : script to create the index of the meteolibre data (h5 files with dates)
