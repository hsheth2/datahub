import pathlib

import requests


def _get_dataset_schema_field(dataset, field_name):
    for field in dataset["schemaMetadata"]["fields"]:
        if field["fieldPath"] == field_name:
            return field
    raise KeyError(f'Field "{field_name}" not found in dataset {dataset["urn"]}')


def _merge_fields(a, b):
    """Merge two fields, with b taking precedence"""

    if not b:
        return a

    if isinstance(b, str):
        return b
    elif isinstance(b, list):
        return (a or []) + b
    elif isinstance(b, dict):
        if a is None:
            return b
        assert isinstance(a, dict)
        assert a.keys() == b.keys()
        return {key: _merge_fields(a[key], b[key]) for key in a.keys()}
    else:
        raise TypeError(f'Cannot merge field "{a}" with "{b}"')


def _fetch_dataset(dataset_urn: str) -> dict:
    cookies = {
        "PLAY_SESSION": "5001875762c5b72e22949cfeaac0bf9f8c21fda9-actor=urn%3Ali%3Acorpuser%3Adatahub&token=eyJhbGciOiJIUzI1NiJ9.eyJhY3RvclR5cGUiOiJVU0VSIiwiYWN0b3JJZCI6ImRhdGFodWIiLCJ0eXBlIjoiU0VTU0lPTiIsInZlcnNpb24iOiIxIiwiZXhwIjoxNjUyMDM4MTI5LCJqdGkiOiIzYTE4YTZkMi05NjgwLTQzM2YtYTU4ZS0xNWYzODRiYjZmOGQiLCJzdWIiOiJkYXRhaHViIiwiaXNzIjoiZGF0YWh1Yi1tZXRhZGF0YS1zZXJ2aWNlIn0.J7EpO2pJy4ZmmN_yOXdLp4bLoHwO6O4ggRBrmOyA558",
        "actor": "urn:li:corpuser:datahub",
    }

    json_data = {
        "query": (pathlib.Path(__file__).parent / "get-dataset.gql").read_text(),
        "variables": {
            "urn": dataset_urn,
        },
    }

    response = requests.post(
        "http://localhost:9002/api/graphql",
        cookies=cookies,
        json=json_data,
    )

    return response.json()["data"]["dataset"]


def get_dataset(dataset_urn):
    dataset = _fetch_dataset(dataset_urn)

    # Merge editable schema fields into the original dataset.
    editableSchemaMetadata = dataset.pop("editableSchemaMetadata", None)
    if editableSchemaMetadata is not None:
        for editedField in editableSchemaMetadata["editableSchemaFieldInfo"]:
            field = _get_dataset_schema_field(dataset, editedField["fieldPath"])

            field["description"] = _merge_fields(
                field["description"], editedField["description"]
            )
            field["glossaryTerms"] = _merge_fields(
                field["glossaryTerms"], editedField["glossaryTerms"]
            )
            field["tags"] = _merge_fields(field["tags"], editedField["tags"])

    # TODO: Simplify the response.

    return dataset


# types of match results:
# - copy
#   + field details
# - transformation
#   + which fields were copied
# - similar schema
#   + which fields are similar (lowest confidence)


def match_datasets(dataset_a, dataset_b):
    # TODO check lineage
    # TODO fetch infinite depth of lineage info

    # TODO compare field names

    # TODO compare field names w/ minor modifications
    pass


if __name__ == "__main__":
    from pprint import pprint

    upstream = get_dataset(
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_public_forecasts.county_14d,PROD)"
    )
    pprint(upstream)

    downstream = get_dataset(
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_public_forecasts.county_14d_historical,PROD)"
    )
    pprint(downstream)
