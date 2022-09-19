#!/bin/bash

# Download and extract team logs
wget http://cloudone.isti.cnr.it/VBS2022_KIS_logs/team_logs.zip
unzip team_logs.zip -d data/2022/
rm team_logs.zip

# Create a virtual environment and install requirements
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Convert CVHunter
python scripts/cvhunter_to_pandas.py --input_file data/2022/team_logs/CVHunter/CVHunter_filtered_data.csv --output_path cache/team_logs/2022