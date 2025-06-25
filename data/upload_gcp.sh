# zip and upload file to gcp
#zip -r groundstation_npz.zip groundstation_npz/
#zip -r h5.zip h5/
zip -r hf_dataset.zip hf_dataset/

#gsutil cp groundstation_npz.zip gs://meteofrance-preprocess/
#gsutil cp h5.zip gs://meteofrance-preprocess/
gsutil cp hf_dataset.zip gs://meteofrance-preprocess/hf_dataset_v3.zip

# also push index
#gsutil cp index.parquet gs://meteofrance-preprocess/
