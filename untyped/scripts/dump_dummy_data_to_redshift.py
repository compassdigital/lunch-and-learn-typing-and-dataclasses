from dotenv import load_dotenv
from pathlib import Path
from untyped.utils.redshift import dump_csv_to_redshift_table
from untyped.utils.redshift import make_redshift_sqlalchemy_engine_from_creds_object
from untyped.utils.redshift import RedshiftCredentials

ROOT_DIR = Path(__file__).resolve().parent.parent.parent

CSV_NAME = "dummy_data.csv"

SCHEMA_NAME = "dbt_dev_david_beallor"
TABLE_NAME = "dummy_data"

CSV_PATH = ROOT_DIR / CSV_NAME

def main():
    load_dotenv()

    creds = RedshiftCredentials.from_env_vars(prefix="REDSHIFT")
    engine = make_redshift_sqlalchemy_engine_from_creds_object(creds)

    dump_csv_to_redshift_table(
        engine=engine,
        csv_path=CSV_PATH,
        table_name=TABLE_NAME,
        schema_name=SCHEMA_NAME,
    )

if __name__ == "__main__":
    main()
