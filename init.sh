sudo apt update
sudo apt install python3-pip
sudo apt install python3.11-venv

# Create a virtual environment and activate it
python3 -m venv venv
source venv/bin/activate

pip3 install -r requirements.txt

# update data
gsutil cp gs://meteofrance-preprocess/2ad89e9d0b014ad0fc3b605dc69b9d41.parquet data/datagouv/
gsutil cp gs://meteofrance-preprocess/f0742d32016f83444e7da3b4b629f2e1.parquet data/datagouv/

sudo apt-get install unzip
sudo apt-get install zip

# install gh
(type -p wget >/dev/null || (sudo apt update && sudo apt-get install wget -y)) \
	&& sudo mkdir -p -m 755 /etc/apt/keyrings \
        && out=$(mktemp) && wget -nv -O$out https://cli.github.com/packages/githubcli-archive-keyring.gpg \
        && cat $out | sudo tee /etc/apt/keyrings/githubcli-archive-keyring.gpg > /dev/null \
	&& sudo chmod go+r /etc/apt/keyrings/githubcli-archive-keyring.gpg \
	&& echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | sudo tee /etc/apt/sources.list.d/github-cli.list > /dev/null \
	&& sudo apt update \
	&& sudo apt install gh -y

sudo apt update
sudo apt install gh

git config --global user.email "adrienbufort@gmail.com"
git config --global user.name "Adrien B"
