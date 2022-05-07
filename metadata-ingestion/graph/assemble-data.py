import pathlib
import subprocess

import pandas as pd
import pyarrow.parquet as pq
import sqlalchemy

BACKUPS_DIR = pathlib.Path("/Users/hsheth/Downloads/oss-backup")

print("Reading data from oss-backup")
table = None
for dir in sorted(
    [dir for dir in BACKUPS_DIR.iterdir() if not dir.name.startswith(".")],
    key=lambda x: int(x.name),
):
    for parquet_file in dir.glob("*.gz.parquet"):
        print(f"Reading {parquet_file}")
        parquet_table = pq.read_pandas(parquet_file).to_pandas()
        # print(parquet_table)

        # parquet_table.reset_index(inplace=True)
        # parquet_table.fillna("", inplace=True)

        if table is None:
            table = parquet_table
        else:
            table = pd.concat([table, parquet_table])

assert table is not None
# table.reset_index(inplace=True, drop=True)
print(table)
print()

print("Cleaning data")

# Filter out most of the datahub execution requests, but keep some.
# example urn: urn:li:dataHubExecutionRequest:00ae17e6-093c-46f0-8807-df688faa1316
table = table[
    ~table["urn"].str.startswith("urn:li:dataHubExecutionRequest:")
    | table["urn"].str.startswith("urn:li:dataHubExecutionRequest:000")
]

# Remove temporary snowflake tables and other test tables.
# example urn: urn:li:dataset:(urn:li:dataPlatform:snowflake,demo_pipeline.public.ge_tmp_abb7ace7,DEV)
# example urn: urn:li:dataset:(urn:li:dataPlatform:snowflake,aseem_test_db.information_schema.applicable_roles,PROD)
table = table[
    ~table["urn"].str.contains("demo_pipeline.public.ge_tmp_")
    & ~table["urn"].str.contains("demo_pipeline.public.ge_temp_")
    & ~table["urn"].str.contains(".information_schema.")
]

# Don't ingest any of the policies.
# example urn: urn:li:dataHubPolicy:017d02d0-38d4-4b98-94d8-cf4f1097ced3
table = table[~table["urn"].str.startswith("urn:li:dataHubPolicy:")]

# Don't ingest managed ingestion configs.
# example urn: urn:li:dataHubIngestionSource:7b34072a-944b-4215-8fa5-b2bf3eea16ad
table = table[~table["urn"].str.startswith("urn:li:dataHubIngestionSource")]

# Only keep the latest versions of aspects.
table = table[table["version"] == 0]

# TODO reorder rows

print(table)
print()

# breakpoint()
# exit(0)


print("Writing data to the DB")
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
subprocess.check_call(
    [
        "./docker/datahub-upgrade/datahub-upgrade.sh",
        "-u",
        "RestoreIndices",
        "-a",
        "clean",
    ],
    cwd="../..",
)

print("Done")
