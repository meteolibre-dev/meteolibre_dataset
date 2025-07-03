# !/bin/bash
# generate the hf files from the h5 files

# create the proper dataset
python3 hf_dataset_resize.py

# we push it to the hub
cd ../data
zip -r hf_dataset.zip hf_dataset/
gsutil cp hf_dataset.zip gs://meteofrance-preprocess/