import humps
from dataset import get_dataset
from thefuzz import fuzz

# types of match results:
# - copy
#   + field details
# - transformation
#   + which fields were copied
# - similar schema
#   + which fields are similar (lowest confidence)


def _simplify_column_name(column_name: str) -> str:
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


def columns_match(column_a: str, column_b: str) -> bool:
    ratio = fuzz.ratio(column_a, column_b)

    if ratio > 96:
        return True

    column_a = _simplify_column_name(column_a)
    column_b = _simplify_column_name(column_b)

    ratio = fuzz.ratio(column_a, column_b)
    if ratio > 95:
        return True

    return False


def match_datasets(dataset_a, dataset_b):
    # TODO check lineage
    # TODO fetch infinite depth of lineage info

    # TODO compare field names

    # TODO compare field names w/ minor modifications
    pass


if __name__ == "__main__":
    # from pprint import pprint

    # Tests for the column matching logic.
    assert columns_match("county_name", "county_name")
    assert columns_match("user_id", "userId")
    assert columns_match("county-name", "county_name")
    assert columns_match("county-name", "countyName")
    # assert columns_match("API_KEY", "api_key")
    assert not columns_match("county_name", "county_name_historical")
    assert not columns_match("new_confirmed_ground_truth", "confirmed_ground_truth")
    assert not columns_match("forecast_date", "forecast_day")

    upstream = get_dataset(
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_public_forecasts.county_14d,PROD)"
    )
    downstream = get_dataset(
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_public_forecasts.county_14d_historical,PROD)"
    )
    # pprint(upstream, sort_dicts=False)
