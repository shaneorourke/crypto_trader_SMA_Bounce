#!bin/bash

chmod +x run_trader_cron.sh &&
chmod +x run_trader.sh &&
chmod +x db_queries.sh &&
mkdir /home/pi/Documents/cronlogs &&
python3 -m venv trader &&
source trader/bin/activate &&
pip install -r requirements.txt &&
python setup.py &&
deactivate