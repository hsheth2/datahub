import pathlib
import subprocess
import time

import pandas as pd
import sqlalchemy


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
