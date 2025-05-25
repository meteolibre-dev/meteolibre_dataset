# !/bin/bash
# generate the hf files from the h5 files

# create the proper dataset
python3 hf_dataset_resize.py

# we push it to the hub
python3 hf_dataset_creation.py