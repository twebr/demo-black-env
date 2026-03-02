# This is a sample Python script.


# %% Imports

import os

import pandas as pd
from dotenv import load_dotenv

from helpers import snowflake_interface

# %% Initial setup

# Note that for a large project, you might want to move this part to a separate file
load_dotenv()

# %% Try to get an environment variable

# Let's try to fetch some environment variable
CACHE_DIRECTORY = os.environ.get("CACHE_DIRECTORY")


# %% Check out the environment variable we fetched earlier

print(CACHE_DIRECTORY)

# %% Let's try Snowflake integration

df: pd.DataFrame = snowflake_interface.fetch_df("""
    SELECT distinct Gebied_type, GEBIED_CODE, STATION_INSTALLATIE_NAAM, CBS_VERSION
    FROM PRD_INZICHT_IN_PROGNOSE.PUB.VW_ASSET_AREAS_YEARS
    WHERE CBS_VERSION = '2023'
    AND GEBIED_TYPE = 'BUURT'
""")

print(df)

