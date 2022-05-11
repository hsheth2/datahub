import pathlib
import subprocess
import time

import pandas as pd
import sqlalchemy

# This script loads testing data into a locally running instance of DataHub.
# We make the assumption that you're already running `datahub docker quickstart`
# on your local machine, within docker, and with the default configuration
# and parameters.
#
# The data is loaded from two files:
#  - data/graph-test-data.json
#  - data/graph-real-world-data.csv
#
# WARNING: Running this script will delete all data you previously had stored
# in your datahub instance.
#
# NOTE: This script uses pandas.DataFrame.to_sql, which interally requires
# SQLAlchemy v1.4+. However, the `datahub` package has a hard dependency
# on SQLAlchemy v1.3.x. To get around this, we'll need to use two separate
# Python virtualenvs. Specifically, the graph directory should have its own
# virtualenv, and the datahub directory should already have its own virtualenv
# from the gradle build process.


def check_datahub_docker():
    subprocess.check_call(["datahub", "docker", "check"])


print("Loading data")
table = pd.read_csv(
    pathlib.Path(__file__).parent / "data" / "graph-real-world-data.csv"
)
print(table)
print()

print("Writing data to the DB")
check_datahub_docker()

engine = sqlalchemy.create_engine("mysql+pymysql://datahub:datahub@localhost/datahub")
table.to_sql(
    "metadata_aspect_v2",
    engine,
    if_exists="replace",
    index=False,
    dtype={
        # The pandas library makes some assumptions about column types,
        # but these assumptions are not true for the datahub MySQL
        # database instance. Instead, we need to explicitly specify the
        # column types to match the expected schema.
        #
        # DataHub MySQL schema: https://github.com/datahub-project/datahub/blob/204d7e633aef1a540372028d3a8050d5454b43ad/docker/mysql/init.sql#L2-L12
        "urn": sqlalchemy.VARCHAR(500),
        "aspect": sqlalchemy.VARCHAR(200),
        "version": sqlalchemy.dialects.mysql.BIGINT(20),
        "metadata": sqlalchemy.dialects.mysql.LONGTEXT,
        "systemmetadata": sqlalchemy.dialects.mysql.LONGTEXT,
        "createdon": sqlalchemy.dialects.mysql.DATETIME,
        "createdby": sqlalchemy.VARCHAR(255),
        "createdfor": sqlalchemy.VARCHAR(255),
    },
)

print("Updating indexes")
# TODO: This often fails when not running in Neo4j mode; need to figure out a way to
# automate this process properly.
#
# In order to get around some issues, we usually need to run the upgrade script three times.
# Once with -a clean to remove the old elasticsearch indexes,
# once without -a clean to generate the some elasticsearch indexes,
# and a third time to restore the things that had dependencies on previous
# data and hence were rejected in the second run.
#
# During this process, we need to monitor `datahub docker check` in order to
# make sure that datahub-gms does not crash - this happens frequently because
# I'm running on a memory constrained machine.

# Step 1 - clean the indexes.
check_datahub_docker()
subprocess.run(
    [
        "./docker/datahub-upgrade/datahub-upgrade.sh",
        "-u",
        "RestoreIndices",
        "-a",
        "clean",
    ],
    cwd="../..",
    check=False,
)
time.sleep(10)

# Step 2 - ingest hand-made test data.
subprocess.check_call(
    [
        "datahub",
        "docker",
        "ingest-sample-data",
        "--path",
        str(pathlib.Path(__file__).parent / "data" / "graph-test-data.json"),
    ]
)

# Step 3 - generate the indexes where dependencies are met.
check_datahub_docker()
subprocess.run(
    [
        "./docker/datahub-upgrade/datahub-upgrade.sh",
        "-u",
        "RestoreIndices",
    ],
    cwd="../..",
    check=False,
)
time.sleep(30)

# Step 4 - regenerate the indexes again to ensure that everything is up to date.
# This is the step where we can set check=True.
check_datahub_docker()
subprocess.run(
    [
        "./docker/datahub-upgrade/datahub-upgrade.sh",
        "-u",
        "RestoreIndices",
    ],
    cwd="../..",
    check=True,
)
time.sleep(30)

print("Done")
