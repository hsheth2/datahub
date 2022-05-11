import dataclasses
from typing import Any, Callable, List

from dataset import Dataset, get_dataset
from matching import Match, MatchType, match_datasets

FieldInfo = dict


@dataclasses.dataclass
class PolicyRule:
    # The name of the policy.
    name: str

    # The match types for which this policy should apply.
    match_types: List[MatchType]

    # If set to true, this policy rule will be applied to each column
    # in the match rather than the entire dataset.
    is_per_field: bool

    # The function to apply to the match.
    # If is_per_field is set to true, this function will be called
    # with each matching fields. Otherwise, it will be called with
    # the entire datasets
    apply_policy: Callable[..., None]


def _select_field_from_dataset(dataset: Dataset, field_name: str) -> FieldInfo:
    for field in dataset["schemaMetadata"]:
        if field["fieldPath"] == field_name:
            return field
    raise NameError(f"field {field_name} not found in dataset")


def apply_policy(
    policy: PolicyRule, dataset_a: Dataset, dataset_b: Dataset, match: Match
) -> None:
    # Verify that we're allowed to run this policy on this match type.
    if match.matchType not in policy.match_types:
        return

    if policy.is_per_field:
        for field in match.fields:
            col_a = field["a"]
            col_b = field["b"]

            field_a = _select_field_from_dataset(dataset_a, col_a)
            field_b = _select_field_from_dataset(dataset_b, col_b)

            policy.apply_policy(dataset_a, dataset_b, field_a, field_b, match)
    else:
        policy.apply_policy(dataset_a, dataset_b, match)

    # TODO transfer description
    # TODO transfer tags
    # TODO transfer terms
    # TODO transfer tags per field
    # TODO transfer terms per field


if __name__ == "__main__":
    from pprint import pprint

    upstream = get_dataset(
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_public_forecasts.county_14d,PROD)"
    )
    downstream = get_dataset(
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_public_forecasts.county_14d_historical,PROD)"
    )
    match = match_datasets(upstream, downstream)

    policy = PolicyRule(
        name="test",
        match_types=[MatchType.COPY, MatchType.LIKELY_COPY, MatchType.TRANSFORMATION],
        is_per_field=False,
        apply_policy=lambda a, b, m: pprint(m),
    )
    pprint(apply_policy(policy, upstream, downstream, match), sort_dicts=False)
