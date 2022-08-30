import os
from dataclasses import dataclass
from pathlib import Path

from pandas import DataFrame
from pandas import read_csv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class RedshiftCredentials:
    """A dataclass to hold redshift authentication credentials."""

    username: str
    password: str
    host: str
    port: int
    db_name: str

    @property
    def url(self):
        """Constructs the database url for the credentials instance."""

        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.db_name}"

    @staticmethod
    def from_env_vars(prefix: str) -> "RedshiftCredentials":
        """Constructs a RedshiftCredentials instance from env vars.
        
        To use credentials from your environment variables, ensure your credentials all have
        the same prefix, and pass that prefix along through the prefix arg. 

        For example, you can pass prefix="MY_PREFIX" and the following env vars will be used:
            - MY_PREFIX_USERNAME
            - MY_PREFIX_PASSWORD
            - MY_PREFIX_HOST
            - MY_PREFIX_PORT
            - MY_PREFIX_DB_NAME
        """

        return RedshiftCredentials(
            username=os.environ[f"{prefix}_USERNAME"],
            password=os.environ[f"{prefix}_PASSWORD"],
            host=os.environ[f"{prefix}_HOST"],
            port=int(os.environ[f"{prefix}_PORT"]),
            db_name=os.environ[f"{prefix}_DB_NAME"],
        )


def make_redshift_sqlalchemy_engine(creds: RedshiftCredentials) -> Engine:
    """Creates a sqlalchemy engine with the supplied credentials."""

    return create_engine(url=creds.url)



def dump_dataframe_to_redshift_table(
    engine: Engine,
    df: DataFrame,
    table_name: str,
    schema_name: str,
) -> None:
    """Dumps a pandas dataframe to a redshift table."""

    print(f"Dumping data frame to redshift table {schema_name}.{table_name}...")

    df.to_sql(
        con=engine,
        name=table_name,
        schema=schema_name,
        index=False,
        if_exists="replace",
        method="multi",
    )

    print(f"Done dumping data frame to redshift table {schema_name}.{table_name}.")


def dump_csv_to_redshift_table(
    engine: Engine,
    csv_path: Path,
    table_name: str,
    schema_name: str,
) -> None:
    """Dumps a csv to a redshift table."""

    print(f"Loading csv from path {csv_path}...")

    if csv_path.is_file():
        df = read_csv(str(csv_path))
    else:
        raise ValueError(f"No file at path {csv_path}!")

    dump_dataframe_to_redshift_table(engine, df, table_name, schema_name)
