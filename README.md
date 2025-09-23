<<<<<<< HEAD
# Malm-Bike-Analytics
=======
# Bike Ingest (Lite)

## Setup
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux:
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # then fill URLs/token

## Run
python -m src.main

## Schedule (cron, every 5 min)
*/5 * * * * /abs/path/.venv/bin/python /abs/path/src/main.py >> /abs/path/ingest.log 2>&1

## Switch to Postgres later
pip install psycopg2-binary
# change DATABASE_URL in .env to:
# postgresql+psycopg2://user:pass@host:5432/bike_db
>>>>>>> 9645448 (test)
