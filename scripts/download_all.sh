# !/bin/bash
# download all the files from the bucket
# and save them in the current directory

# get the list of files (radar info and h5 files)
python3 get_bucket_list_files.py

# download all the files (only h5 files)
python3 download_h5_files.py

# index creation (for h5 files) (only radar/info files)
python3 index_creation.py

# download ground station data
python3 download_groundstation.py

# preprocess ground station data
python3 preprocess_groundstattions.py