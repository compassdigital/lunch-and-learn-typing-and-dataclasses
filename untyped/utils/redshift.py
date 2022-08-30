import os

from sqlalchemy import create_engine
from pandas import read_csv


def make_redshift_database_url(username, password, host, port, db_name):
    """Creates a database url from the supplied credentials.
    
    Keyword arguments:
    username -- the db user to connect with (str)
    password -- the password to connect with (str)
    host -- the redshift host to connect to (str)
    port -- the port to connect to (int)
    db_name -- the name of the db to connect to (str)
    """

    return f"postgresql://{username}:{password}@{host}:{port}/{db_name}"


def make_redshift_sqlalchemy_engine(username, password, host, port, db_name):
    """Creates a sqlalchemy engine with the supplied credentials.
    
    Keyword arguments:
    username -- the db user to connect with (str)
    password -- the password to connect with (str)
    host -- the redshift host to connect to (str)
    port -- the port to connect to (int)
    db_name -- the name of the db to connect to (str)
    """

    return create_engine(
        url=make_redshift_database_url(username, password, host, port, db_name),
    )


def make_redshift_sqlalchemy_engine_from_env_vars(prefix):
    """Creates a sqlalchemy engine with the credentials stored in environment
    variables at the supplied prefix.
    
    For example, you can pass prefix="MY_PREFIX" and the following env vars will be used:
        - MY_PREFIX_USERNAME
        - MY_PREFIX_PASSWORD
        - MY_PREFIX_HOST
        - MY_PREFIX_PORT
        - MY_PREFIX_DB_NAME
    
    Keyword arguments:
    prefix -- the prefix under which the credentials are stored in env vars (str)
    """

    return make_redshift_sqlalchemy_engine(
        username=os.environ[f"{prefix}_USERNAME"],
        password=os.environ[f"{prefix}_PASSWORD"],
        host=os.environ[f"{prefix}_HOST"],
        port=os.environ[f"{prefix}_PORT"],
        db_name=os.environ[f"{prefix}_DB_NAME"],
    )


def dump_dataframe_to_redshift_table(engine, df, table_name, schema_name):
    """Dumps a pandas dataframe to a redshift table.
    
    Keyword arguments:
    engine -- a connected sqlalchemy engine to write the table with (sqlalchemy.Engine)
    df -- the pandas dataframe to write to redshift (pandas.DataFrame)
    table_name -- the table name to write to (str)
    schema_name -- the schema to write to (str)
    """

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


def dump_csv_to_redshift_table(engine, csv_path, table_name, schema_name):
    """Dumps a csv to a redshift table.
    
    Keyword arguments:
    engine -- a connected sqlalchemy engine to write the table with (sqlalchemy.Engine)
    csv_path -- the path to the csv that's to be written to redshift (pathlib.Path)
    table_name -- the table name to write to (str)
    schema_name -- the schema to write to (str)
    """

    print(f"Loading csv from path {csv_path}...")

    if csv_path.is_file():
        df = read_csv(str(csv_path))
    else:
        raise ValueError(f"No file at path {csv_path}!")

    dump_dataframe_to_redshift_table(engine, df, table_name, schema_name)


def make_redshift_database_url_from_creds_dict(creds_dict):
    """Creates a database url from the supplied credentials dictionary.

    The credentials dictionary should take the form:
    {
        "username": ...
        "password": ...
        "host": ...
        "port": ...
        "db_name": ...
    }
    
    Keyword arguments:
    creds_dict -- a dictionary storing the credentials with which the url will be constructed (dict)
    """

    return f"postgresql://{creds_dict['username']}:{creds_dict['password']}@{creds_dict['host']}:{creds_dict['port']}/{creds_dict['db_name']}"


def make_redshift_sqlalchemy_engine_from_creds_dict(creds_dict):
    """Creates a sqlalchemy engine with the supplied credentials dictionary.

    The credentials dictionary should take the form:
    {
        "username": ...
        "password": ...
        "host": ...
        "port": ...
        "db_name": ...
    }
    
    Keyword arguments:
    creds_dict -- a dictionary storing the credentials with which to connect to redshift (dict)
    """

    return create_engine(url=make_redshift_database_url_from_creds_dict(creds_dict))


class RedshiftCredentials:
    """A class to store redshift credentials."""

    def __init__(
        self, 
        username,
        password,
        host,
        port,
        db_name,
    ):
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.db_name = db_name

    @property
    def url(self):
        """Creates a database url for the credentials instance."""

        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.db_name}"

    @staticmethod
    def from_env_vars(prefix):
        """Constructs a RedshiftCredentials instance from env vars.
        
        To use credentials from your environment variables, ensure your credentials all have
        the same prefix, and pass that prefix along through the prefix arg. 

        For example, you can pass prefix="MY_PREFIX" and the following env vars will be used:
            - MY_PREFIX_USERNAME
            - MY_PREFIX_PASSWORD
            - MY_PREFIX_HOST
            - MY_PREFIX_PORT
            - MY_PREFIX_DB_NAME

        Keyword Arguments:
        prefix -- the prefix at which the credentials are stored in env vars (str)
        """

        return RedshiftCredentials(
            username=os.environ[f"{prefix}_USERNAME"],
            password=os.environ[f"{prefix}_PASSWORD"],
            host=os.environ[f"{prefix}_HOST"],
            port=os.environ[f"{prefix}_PORT"],
            db_name=os.environ[f"{prefix}_DB_NAME"],
        )


def make_redshift_sqlalchemy_engine_from_creds_object(creds_object):
    """Creates a sqlalchemy engine with the supplied credentials object.
    
    Keyword arguments:
    creds_object -- the credentials object to use to connect to redshift (RedshiftCredentials)
    """

    return create_engine(url=creds_object.url)