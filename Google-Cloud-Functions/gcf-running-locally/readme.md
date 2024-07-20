Step 1. Project Directory Structure:

cloud-function-run-locally/
├── main.py
├── requirements.txt
├── simulate_event.py
├── .vscode/
│   └── launch.json


Step 2: Steps to Run and Debug

2.1 Install Dependencies:

Ensure you have all dependencies installed. You can create a virtual environment and install the dependencies using pip:
# pls run the following commands one by one
python -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`
pip install -r requirements.txt

2.2 Run Functions Framework Locally:

Start the Functions Framework to serve your Cloud Function locally:
# Navigate through to the folder in which you have gcf files
python -m functions_framework --target bucket_creator

2.3 Simulate Event:

In a separate terminal, run the simulation script to send an event to the local function:
# run the following command
python simulate_event.py
