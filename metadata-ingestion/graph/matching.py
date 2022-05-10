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
    """
    Fetch a dataset from DataHub using the GraphQL API.

    Below is an example response:

    {'editableSchemaMetadata': {'editableSchemaFieldInfo': [{'description': None,
                                                             'fieldPath': 'recovered',
                                                             'glossaryTerms': None,
                                                             'tags': {'tags': [{'tag': {'urn': 'urn:li:tag:Legacy'}}]}}]},
     'glossaryTerms': {'terms': [{'term': {'urn': 'urn:li:glossaryTerm:SavingAccount'}}]},
     'lineage': {'relationships': [{'entity': {'urn': 'urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_public_forecasts.county_14d_historical,PROD)'},
                                    'type': 'DownstreamOf'},
                                   {'entity': {'urn': 'urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_public_forecasts.county_28d,PROD)'},
                                    'type': 'DownstreamOf'},
                                   {'entity': {'urn': 'urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_public_forecasts.state_14d,PROD)'},
                                    'type': 'DownstreamOf'}]},
     'ownership': {'owners': [{'owner': {'__typename': 'CorpUser',
                                         'urn': 'urn:li:corpuser:HarvardGlobalHealthInstitute'},
                               'type': 'DATAOWNER'},
                              {'owner': {'__typename': 'CorpUser',
                                         'urn': 'urn:li:corpuser:Google'},
                               'type': 'DATAOWNER'}]},
     'properties': {'description': 'This predicts the value for key metrics for '
                                   'COVID-19 impacts over a 14-day horizon at the '
                                   'US state level.',
                    'name': 'bigquery-public-data.covid19_public_forecasts.county_14d'},
     'schemaMetadata': {'fields': [{'description': '5-digit unique identifer of '
                                                   'the county.',
                                    'fieldPath': 'county_fips_code',
                                    'glossaryTerms': None,
                                    'tags': None},
                                   {'description': 'Full text name of the county',
                                    'fieldPath': 'county_name',
                                    'glossaryTerms': None,
                                    'tags': None},
                                   {'description': 'Full text name of the state in '
                                                   'which a given county lies',
                                    'fieldPath': 'state_name',
                                    'glossaryTerms': None,
                                    'tags': None},
                                   {'description': 'Date of the forecast',
                                    'fieldPath': 'forecast_date',
                                    'glossaryTerms': None,
                                    'tags': None},
                                   {'description': 'Predicted date of the given '
                                                   'metrics',
                                    'fieldPath': 'prediction_date',
                                    'glossaryTerms': None,
                                    'tags': None},
                                   {'description': 'Predicted number of new '
                                                   'confirmed cases on the '
                                                   'prediction_date. This is not '
                                                   'cumulative over time',
                                    'fieldPath': 'new_confirmed',
                                    'glossaryTerms': None,
                                    'tags': None},
                                   {'description': 'Predicted number of cumulative '
                                                   'deaths on the prediction_date. '
                                                   'This is cumulative over time',
                                    'fieldPath': 'cumulative_confirmed',
                                    'glossaryTerms': None,
                                    'tags': None},
                                   {'description': 'The seven day rolling average '
                                                   'of new confirmed cases.',
                                    'fieldPath': 'new_confirmed_7day_rolling',
                                    'glossaryTerms': None,
                                    'tags': None},
                                   {'description': 'Predicted number of new deaths '
                                                   'on the prediction_date. This '
                                                   'is cumulative over time',
                                    'fieldPath': 'new_deaths',
                                    'glossaryTerms': None,
                                    'tags': None},
                                   {'description': 'Predicted number of cumulative '
                                                   'confirmed cases on the '
                                                   'prediction_date. This is not '
                                                   'cumulative over time',
                                    'fieldPath': 'cumulative_deaths',
                                    'glossaryTerms': None,
                                    'tags': None},
                                   {'description': 'The seven day rolling average '
                                                   'of new confirmed cases.',
                                    'fieldPath': 'new_deaths_7day_rolling',
                                    'glossaryTerms': None,
                                    'tags': None},
                                   {'description': 'Predicted number of people '
                                                   'hospitalized on the '
                                                   'prediction_date. This is not '
                                                   'cumulative over time',
                                    'fieldPath': 'hospitalized_patients',
                                    'glossaryTerms': None,
                                    'tags': None},
                                   {'description': 'Predicted number of people '
                                                   'documented as recovered on the '
                                                   'prediction_date. This is not '
                                                   'cumulative over time',
                                    'fieldPath': 'recovered',
                                    'glossaryTerms': None,
                                    'tags': None},
                                   {'description': 'Actual number of new confirmed '
                                                   'cases according to the ground '
                                                   'truth data. This is not '
                                                   'cumulative over time',
                                    'fieldPath': 'new_confirmed_ground_truth',
                                    'glossaryTerms': None,
                                    'tags': None},
                                   {'description': 'Actual number of cumulative '
                                                   'confirmed cases according to '
                                                   'the ground truth data. This is '
                                                   'cumulative over time',
                                    'fieldPath': 'cumulative_confirmed_ground_truth',
                                    'glossaryTerms': None,
                                    'tags': None},
                                   {'description': 'Actual number of new deaths '
                                                   'according to the ground truth '
                                                   'data. This is not cumulative '
                                                   'over time',
                                    'fieldPath': 'new_deaths_ground_truth',
                                    'glossaryTerms': None,
                                    'tags': None},
                                   {'description': 'Actual number of cumulative '
                                                   'deaths according to the ground '
                                                   'truth data. This is cumulative '
                                                   'over time',
                                    'fieldPath': 'cumulative_deaths_ground_truth',
                                    'glossaryTerms': None,
                                    'tags': None},
                                   {'description': 'Actual number of people '
                                                   'hospitalized according to the '
                                                   'ground truth data. This is not '
                                                   'cumulative over time',
                                    'fieldPath': 'hospitalized_patients_ground_truth',
                                    'glossaryTerms': None,
                                    'tags': None},
                                   {'description': 'Actual number of people '
                                                   'hospitalized according to the '
                                                   'ground truth data',
                                    'fieldPath': 'recovered_documented_ground_truth',
                                    'glossaryTerms': None,
                                    'tags': None},
                                   {'description': 'Total population of the county',
                                    'fieldPath': 'county_population',
                                    'glossaryTerms': None,
                                    'tags': None}]},
     'tags': {'tags': []},
     'type': 'DATASET',
     'urn': 'urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_public_forecasts.county_14d,PROD)'}
    """

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


def _simplify_tags(tags: dict) -> list:
    if not tags:
        return []

    tags_inner = tags["tags"]

    urns = [tag["tag"]["urn"] for tag in tags_inner]

    return urns


def _simplify_glossary_terms(terms: dict) -> list:
    if not terms:
        return []

    terms_inner = terms["terms"]

    urns = [term["term"]["urn"] for term in terms_inner]

    return urns


def get_dataset(dataset_urn: str) -> dict:
    """
    Get a dataset by its URN.

    Below is an example return value:

    {'urn': 'urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_public_forecasts.county_14d,PROD)',
     'type': 'DATASET',
     'properties': {'name': 'bigquery-public-data.covid19_public_forecasts.county_14d',
                    'description': 'This predicts the value for key metrics for '
                                   'COVID-19 impacts over a 14-day horizon at the '
                                   'US state level.'},
     'ownership': ['urn:li:corpuser:HarvardGlobalHealthInstitute',
                   'urn:li:corpuser:Google'],
     'tags': [],
     'glossaryTerms': ['urn:li:glossaryTerm:SavingAccount'],
     'schemaMetadata': [{'fieldPath': 'county_fips_code',
                         'description': '5-digit unique identifer of the county.',
                         'tags': [],
                         'glossaryTerms': []},
                        {'fieldPath': 'county_name',
                         'description': 'Full text name of the county',
                         'tags': [],
                         'glossaryTerms': []},
                        {'fieldPath': 'state_name',
                         'description': 'Full text name of the state in which a '
                                        'given county lies',
                         'tags': [],
                         'glossaryTerms': []},
                        {'fieldPath': 'forecast_date',
                         'description': 'Date of the forecast',
                         'tags': [],
                         'glossaryTerms': []},
                        {'fieldPath': 'prediction_date',
                         'description': 'Predicted date of the given metrics',
                         'tags': [],
                         'glossaryTerms': []},
                        {'fieldPath': 'new_confirmed',
                         'description': 'Predicted number of new confirmed cases '
                                        'on the prediction_date. This is not '
                                        'cumulative over time',
                         'tags': [],
                         'glossaryTerms': []},
                        {'fieldPath': 'cumulative_confirmed',
                         'description': 'Predicted number of cumulative deaths on '
                                        'the prediction_date. This is cumulative '
                                        'over time',
                         'tags': [],
                         'glossaryTerms': []},
                        {'fieldPath': 'new_confirmed_7day_rolling',
                         'description': 'The seven day rolling average of new '
                                        'confirmed cases.',
                         'tags': [],
                         'glossaryTerms': []},
                        {'fieldPath': 'new_deaths',
                         'description': 'Predicted number of new deaths on the '
                                        'prediction_date. This is cumulative over '
                                        'time',
                         'tags': [],
                         'glossaryTerms': []},
                        {'fieldPath': 'cumulative_deaths',
                         'description': 'Predicted number of cumulative confirmed '
                                        'cases on the prediction_date. This is not '
                                        'cumulative over time',
                         'tags': [],
                         'glossaryTerms': []},
                        {'fieldPath': 'new_deaths_7day_rolling',
                         'description': 'The seven day rolling average of new '
                                        'confirmed cases.',
                         'tags': [],
                         'glossaryTerms': []},
                        {'fieldPath': 'hospitalized_patients',
                         'description': 'Predicted number of people hospitalized '
                                        'on the prediction_date. This is not '
                                        'cumulative over time',
                         'tags': [],
                         'glossaryTerms': []},
                        {'fieldPath': 'recovered',
                         'description': 'Predicted number of people documented as '
                                        'recovered on the prediction_date. This is '
                                        'not cumulative over time',
                         'tags': ['urn:li:tag:Legacy'],
                         'glossaryTerms': []},
                        {'fieldPath': 'new_confirmed_ground_truth',
                         'description': 'Actual number of new confirmed cases '
                                        'according to the ground truth data. This '
                                        'is not cumulative over time',
                         'tags': [],
                         'glossaryTerms': []},
                        {'fieldPath': 'cumulative_confirmed_ground_truth',
                         'description': 'Actual number of cumulative confirmed '
                                        'cases according to the ground truth data. '
                                        'This is cumulative over time',
                         'tags': [],
                         'glossaryTerms': []},
                        {'fieldPath': 'new_deaths_ground_truth',
                         'description': 'Actual number of new deaths according to '
                                        'the ground truth data. This is not '
                                        'cumulative over time',
                         'tags': [],
                         'glossaryTerms': []},
                        {'fieldPath': 'cumulative_deaths_ground_truth',
                         'description': 'Actual number of cumulative deaths '
                                        'according to the ground truth data. This '
                                        'is cumulative over time',
                         'tags': [],
                         'glossaryTerms': []},
                        {'fieldPath': 'hospitalized_patients_ground_truth',
                         'description': 'Actual number of people hospitalized '
                                        'according to the ground truth data. This '
                                        'is not cumulative over time',
                         'tags': [],
                         'glossaryTerms': []},
                        {'fieldPath': 'recovered_documented_ground_truth',
                         'description': 'Actual number of people hospitalized '
                                        'according to the ground truth data',
                         'tags': [],
                         'glossaryTerms': []},
                        {'fieldPath': 'county_population',
                         'description': 'Total population of the county',
                         'tags': [],
                         'glossaryTerms': []}],
     'downstreams': ['urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_public_forecasts.county_14d_historical,PROD)',
                     'urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_public_forecasts.county_28d,PROD)',
                     'urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_public_forecasts.state_14d,PROD)']}
    """

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

    # Simplify the tags/terms.
    dataset["tags"] = _simplify_tags(dataset["tags"])
    dataset["glossaryTerms"] = _simplify_glossary_terms(dataset["glossaryTerms"])
    for field in dataset["schemaMetadata"]["fields"]:
        field["tags"] = _simplify_tags(field["tags"])
        field["glossaryTerms"] = _simplify_glossary_terms(field["glossaryTerms"])

    # Simplify the ownership info.
    dataset["ownership"] = [
        owner["owner"]["urn"] for owner in dataset["ownership"]["owners"]
    ]

    # Simplify the lineage info.
    dataset["downstreams"] = [
        item["entity"]["urn"] for item in dataset["lineage"]["relationships"]
    ]
    dataset.pop("lineage")

    # Move the schema to the top level.
    dataset["schemaMetadata"] = dataset["schemaMetadata"]["fields"]

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
    pprint(upstream, sort_dicts=False)

    # downstream = get_dataset(
    #     "urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_public_forecasts.county_14d_historical,PROD)"
    # )
    # pprint(downstream)
