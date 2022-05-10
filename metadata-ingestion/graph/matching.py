from dataset import get_dataset

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

    downstream = get_dataset(
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_public_forecasts.county_14d_historical,PROD)"
    )
