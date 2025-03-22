# zip and upload file to gcp
zip -r groundstation_npz.zip groundstation_npz/
zip -r h5.zip h5/

gsutil cp groundstation_npz.zip gs://meteofrance-preprocess/
gsutil cp h5.zip gs://meteofrance-preprocess/

# also push index
gsutil cp index.parquet gs://meteofrance-preprocess/
