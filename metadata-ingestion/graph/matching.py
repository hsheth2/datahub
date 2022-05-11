import enum

import humps
from dataset import Dataset, get_dataset
from thefuzz import fuzz


@enum.unique
class MatchType(enum.Enum):
    # For copies, we'll including a mapping of the fields.
    COPY = enum.auto()

    # A likely copy is the same as copy, but will be generated if
    # there is no pre-existing lineage relationship.
    LIKELY_COPY = enum.auto()

    # When there's a lineage edge, we'll attempt to match the fields
    # between the two entities.
    TRANSFORMATION = enum.auto()

    # When there's no lineage edge but the schemas are similar, we'll
    # attempt to match the fields between the two entities.
    SIMILAR_SCHEMAS = enum.auto()

    # If none of the above apply, we'll declare that there's no match.
    NO_MATCH = enum.auto()


def _simplify_name(column_name: str) -> str:
    """
    Return a simplified version of the column name. This is used to
    "canonicalize" the column name, so that we can compare it to column
    names from other tables. It converts common naming schemes, including
    PascalCase, camelCase, and kebab-case into a standard snake_case.
    """

    if humps.is_pascalcase(column_name):
        return humps.depascalize(column_name)
    elif humps.is_camelcase(column_name):
        return humps.decamelize(column_name)
    elif "-" in column_name:
        # Check if the column is kebab-case by converting to snake_case.
        snake_case_column_name = column_name.replace("-", "_")

        if humps.is_snakecase(snake_case_column_name):
            return snake_case_column_name
    elif humps.is_snakecase(column_name):
        return column_name

    print(f"warn: unknown column name format: {column_name}")
    return column_name


def names_match(column_a: str, column_b: str, threshold: int = 100) -> bool:
    """
    Determines if two names match. Does some string canonicalization
    and potentially fuzzy matching to allow for minor modifications.
    By default, we set the threshold to 100 for exact matches, but
    this threshold can be reduced on a case-by-case basis.
    """

    ratio = fuzz.ratio(column_a, column_b)

    if ratio >= threshold:
        return True

    column_a = _simplify_name(column_a)
    column_b = _simplify_name(column_b)

    ratio = fuzz.ratio(column_a, column_b)
    if ratio >= threshold:
        return True

    return False


def _is_schema_copy(dataset_a: Dataset, dataset_b: Dataset) -> bool:
    """
    A schema copy is one where the all fields match in the same order.
    """

    if len(dataset_a["schemaMetadata"]) != len(dataset_b["schemaMetadata"]):
        return False

    for field_a, field_b in zip(
        dataset_a["schemaMetadata"], dataset_b["schemaMetadata"]
    ):
        if not names_match(field_a["fieldPath"], field_b["fieldPath"]):
            return False

    return True


def _has_lineage_downstream_relationship(
    dataset_a: Dataset, dataset_b: Dataset
) -> bool:
    """
    Determine if the two datasets have a lineage relationship.
    Specifically, if dataset_b is a downstream of dataset_a.
    """

    dataset_b_urn = dataset_b["urn"]

    if dataset_b_urn in dataset_a["downstreams"]:
        return True

    return False


def _match_columns(dataset_a: Dataset, dataset_b: Dataset) -> list:
    """
    Determine which columns match in a pair of datasets, and
    generate a mapping between the columns.

    Returns a list of mappings:

    [
        {"a": "fieldNameInA", "b": "fieldNameInB"},
        ...
    ]
    """

    columns_a = [field["fieldPath"] for field in dataset_a["schemaMetadata"]]
    columns_b = [field["fieldPath"] for field in dataset_b["schemaMetadata"]]

    used_b_cols = set()
    mappings = []

    for column_a in columns_a:
        matching_column = None
        for column_b in columns_b:
            if names_match(column_a, column_b):
                if matching_column is not None:
                    raise ValueError(
                        f"ambiguous column match: dataset A's {column_a} matches B's {matching_column} and {column_b}"
                    )

                matching_column = column_b

        if matching_column is not None:
            if matching_column in used_b_cols:
                raise ValueError(
                    f"ambiguous column match: {matching_column} matches multiple columns in dataset A"
                )

            mappings.append({"a": column_a, "b": matching_column})
            used_b_cols.add(matching_column)

    return mappings


def match_datasets(dataset_a: Dataset, dataset_b: Dataset) -> dict:
    """
    Determine the match type between two datasets.

    Returns a dictionary that looks like this:

    {
        "matchType": MatchType.COPY,
        "fields": [
            {"a": "fieldNameInA", "b": "fieldNameInB"},
            ...
        ]
    }
    """

    has_lineage = _has_lineage_downstream_relationship(dataset_a, dataset_b)
    is_schema_copy = _is_schema_copy(dataset_a, dataset_b)
    dataset_names_match = names_match(
        dataset_a["properties"]["name"], dataset_b["properties"]["name"], threshold=96
    )
    # TODO We should also consider the description.

    # If the schema fields are the same, it's either a COPY or LIKELY_COPY.
    # However, there's some cases where we have tables with names like
    # "bigquery-public-data.covid19_public_forecasts.county_14d" and
    # "bigquery-public-data.covid19_public_forecasts.county_14d_historical".
    # These shouldn't match as a COPY or LIKELY_COPY.

    if is_schema_copy and dataset_names_match:
        field_mapping = [
            {
                "a": field_a["fieldPath"],
                "b": field_b["fieldPath"],
            }
            for field_a, field_b in zip(
                dataset_a["schemaMetadata"],
                dataset_b["schemaMetadata"],
            )
        ]

        if has_lineage:
            return {
                "matchType": MatchType.COPY,
                "fields": field_mapping,
            }
        else:
            return {
                "matchType": MatchType.LIKELY_COPY,
                "fields": field_mapping,
            }

    # If we get here, we know it's not a COPY or LIKELY_COPY.
    field_mapping = _match_columns(dataset_a, dataset_b)

    # If there's a lineage edge, we'll match the fields.
    if has_lineage:
        return {
            "matchType": MatchType.TRANSFORMATION,
            "fields": field_mapping,
        }

    # If at least two columns match, we'll consider it a similar dataset.
    if len(field_mapping) >= 2:
        return {
            "matchType": MatchType.SIMILAR_SCHEMAS,
            "fields": field_mapping,
        }

    return {
        "matchType": MatchType.NO_MATCH,
    }


if __name__ == "__main__":
    from pprint import pprint

    # Tests for the column matching logic.
    assert names_match("county_name", "county_name")
    assert names_match("user_id", "userId")
    assert names_match("county-name", "county_name")
    assert names_match("county-name", "countyName")
    assert names_match("APIKey", "api_key")
    # FIXME: assert column_match("API_key", "api_key")
    assert not names_match("county_name", "county_name_historical")
    assert not names_match("new_confirmed_ground_truth", "confirmed_ground_truth")
    assert not names_match("forecast_date", "forecast_day")
    assert not names_match(
        "bigquery-public-data.covid19_public_forecasts.county_14d",
        "bigquery-public-data.covid19_public_forecasts.county_14d_historical",
        threshold=96,
    )

    # Test dataset matching logic.
    upstream = get_dataset(
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_public_forecasts.county_14d,PROD)"
    )
    downstream = get_dataset(
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_public_forecasts.county_14d_historical,PROD)"
    )
    pprint(match_datasets(upstream, downstream))
