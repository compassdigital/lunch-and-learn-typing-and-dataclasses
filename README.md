# Lunch and Learn: Typing and Dataclasses

## Running Locally

1. Clone the repo to your local machine
2. Navigate to the repo directory
3. Start a virtual environment `python -m venv env`
4. Activate the virtual environment `source env/bin/activate`
5. Install requirements `pip install -r requirements.txt`
6. Create your `.env` file
```
REDSHIFT_DB_NAME=...
REDSHIFT_HOST=...
REDSHIFT_USERNAME=...
REDSHIFT_PASSWORD=...
REDSHIFT_PORT=...
```
7. Run the script of your choosing (both have the same result):
  - `python typed/scripts/dump_dummy_data_to_redshift.py`
  - `python untyped/scripts/dump_dummy_data_to_redshift.py`
