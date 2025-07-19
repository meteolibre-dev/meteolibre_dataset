

# Create a virtual environment and activate it
python3 -m venv venv
source venv/bin/activate

pip3 install uv
uv pip install -p .

# update data
gsutil cp gs://meteofrance-preprocess/2ad89e9d0b014ad0fc3b605dc69b9d41.parquet data/datagouv/
gsutil cp gs://meteofrance-preprocess/f0742d32016f83444e7da3b4b629f2e1.parquet data/datagouv/

