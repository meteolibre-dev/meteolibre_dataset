# meteolibre_dataset

This repository is dedicated to the preprocessing of Météo-France (MF) data and ground station data to create a unified dataset suitable for training machine learning models.

The primary goal is to take raw data from Météo-France and ground stations, apply necessary preprocessing steps, and fuse them into a single, ready-to-use (huggingface) dataset.

## Features

- Download scripts for raw Météo-France and ground station data.
- Preprocessing pipelines for both data sources.
- Data fusion mechanism to combine different data types.
- Integration with Hugging Face Datasets for easy access and use.

## Setup

To set up the project, you can either use the provided Dockerfile or install dependencies manually.

### Using Docker

Build the Docker image:
```bash
docker build -t meteolibre_dataset .
```

Run the container:
```bash
docker run -it meteolibre_dataset /bin/bash
```

Inside the container, you can run the initialization script:
```bash
./init.sh
cd meteolibre_dataset/scripts/
bash download_all.sh
```

### Manual Installation

1. Clone the repository:
```bash
git clone https://github.com/your_username/meteolibre_dataset.git
cd meteolibre_dataset
```

2. Install the required dependencies using pip:
```bash
pip install -r requirements.txt
```

3. Run the initialization script:
```bash
./init.sh
cd meteolibre_dataset/scripts/
bash download_all.sh
```

## Data

The dataset is built from two main sources:

1.  **Météo-France (MF) Data:** Raw meteorological data provided by Météo-France.
2.  **Ground Station Data:** Data collected from various ground weather stations.

The preprocessing steps involve cleaning, transforming, and aligning data from both sources before fusion.

## Usage

### Downloading Data

To download all necessary raw data, run the main download script:
```bash
./scripts/download_all.sh
```
*(Note: This script may require specific credentials or access to Météo-France data sources not included in this repository.)*

### Preprocessing and Dataset Creation

After downloading the raw data, you can run the preprocessing and dataset creation scripts. The main steps involve:

1.  Preprocessing ground station data: `scripts/preprocess_groundstations.py`
2.  Writing ground station data to NPZ format: `scripts/groundstation_npz_writing.py`
3.  Creating the Hugging Face dataset from processed data: `scripts/hf_dataset_creation.py`
4.  Resizing the Hugging Face dataset (if needed): `scripts/hf_dataset_resize.py`

You can typically run these scripts in sequence after the data download is complete.

## Project Structure

```
.
├── Dockerfile
├── init.sh
├── LICENSE
├── README.md
├── requirements.txt
├── reprojected_gebco_32630_500m_padded.png
├── data/                 # Directory for raw and processed data
├── meteolibre_dataset/   # Main package directory (if applicable)
├── scripts/
│   ├── download_all.sh
│   ├── download_groundstation.py
│   ├── groundstation_npz_writing.py
│   ├── hf_dataset_creation.py
│   ├── hf_dataset_resize.py
│   └── preprocess_groundstations.py
└── tables/               # Directory for lookup tables or metadata
```

## License

This project is licensed under the terms of the [LICENSE](LICENSE) file. (Apache 2.0)
