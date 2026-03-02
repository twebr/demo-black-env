"""
Interface for fetching data from Snowflake and writing the data to MLFlow.
"""

import os

import pandas as pd
import snowflake.connector

VERBOSE = True

def fetch_df(
    sql: str,
    params: dict | list = None,
) -> pd.DataFrame:
    """
    Fetches the result of an SQL query as a dataframe.

    :param sql: A string containing the SQL statement to execute.
    :param params: If you used parameters for binding data in the SQL statement, set
    this to the list or dictionary of variables that should be bound to those parameters.
    :return: the result of the SQL statement as a dataframe
    """

    if VERBOSE:
        print(f"Retrieving data from Snowflake...")
        print("Running query:")
        print(sql)

    ## Set up a database connection to Snowflake
    # See API: https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-api#fetch_pandas_all
    # And docs: https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-pandas
    with snowflake.connector.connect(
        user=os.environ.get("SNOWFLAKE_USER"),
        authenticator=os.environ.get("SNOWFLAKE_AUTHENTICATOR"),
        account=os.environ.get("SNOWFLAKE_ACCOUNT"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
        database=os.environ.get("SNOWFLAKE_DATABASE"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA"),
    ) as ctx:

        # Create a cursor object.
        with ctx.cursor() as cur:

            # Execute a statement that will generate a result set.
            cur.execute(sql, params)
            # df = pd.DataFrame.from_records(
            #     iter(cur), columns=[x[0] for x in cur.description]
            # )
            df = cur.fetch_pandas_all()

            # Reduce memory by converting any floats with int values to ints.
            # df = downcast_dtypes(df)

            # Fetch the result set from the cursor and deliver it as the pandas DataFrame.
            return df
