to install on local machine
python -m venv .pyclif_dev
source .pyclif_dev/bin/activate.bat
pip install --quiet -r requirements.txt
pip install --quiet jupyter ipykernel papermill
cd to the pyclif folder

pip install -e .