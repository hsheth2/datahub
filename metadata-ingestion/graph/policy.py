import dataclasses
from typing import Callable, List, Optional

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
    # with each matching dataset + field combination. Otherwise, it
    # will be called with the entire datasets.
    apply_policy: Callable[..., None]


def _select_field_from_dataset(dataset: Dataset, field_name: str) -> FieldInfo:
    for field in dataset["schemaMetadata"]:
        if field["fieldPath"] == field_name:
            return field
    raise NameError(f"field {field_name} not found in dataset")


def _apply_policy(
    policy: PolicyRule, dataset_a: Dataset, dataset_b: Dataset, match: Match
) -> None:
    # Verify that we're allowed to run this policy on this match type.
    if match.matchType not in policy.match_types:
        return

    if policy.is_per_field:
        # If the policy is per field, we iterate over the fields in the match.
        for field in match.fields:
            col_a = field["a"]
            col_b = field["b"]

            field_a = _select_field_from_dataset(dataset_a, col_a)
            field_b = _select_field_from_dataset(dataset_b, col_b)

            policy.apply_policy(dataset_a, dataset_b, field_a, field_b, match)
    else:
        # Otherwise, we apply the policy to the entire dataset.
        policy.apply_policy(dataset_a, dataset_b, match)


def apply_policies(
    policies: List[PolicyRule], dataset_a: Dataset, dataset_b: Dataset
) -> None:
    matches = match_datasets(dataset_a, dataset_b)

    for policy in policies:
        _apply_policy(policy, dataset_a, dataset_b, matches)


def _propagate_description(
    desc_a: Optional[str],
    desc_b: Optional[str],
    name_a: str,
    name_b: str,
) -> None:
    if desc_a and not desc_b:
        print(f'suggest for {name_b}: set description to "{desc_a}"')
    if desc_b and not desc_a:
        print(f'suggest for {name_a}: set description to "{desc_b}"')


def _propagate_tags(
    tags_a: list,
    tags_b: list,
    name_a: str,
    name_b: str,
) -> None:
    for tag_a in tags_a:
        if tag_a not in tags_b:
            print(f'suggest for {name_b}: add tag "{tag_a}"')

    for tag_b in tags_b:
        if tag_b not in tags_a:
            print(f'suggest for {name_a}: add tag "{tag_b}"')


def _propagate_terms(
    terms_a: List[str],
    terms_b: List[str],
    name_a: str,
    name_b: str,
) -> None:
    for term_a in terms_a:
        if term_a not in terms_b:
            print(f'suggest for {name_b}: add term "{term_a}"')

    for term_b in terms_b:
        if term_b not in terms_a:
            print(f'suggest for {name_a}: add term "{term_b}"')


DEFAULT_POLICIES: List[PolicyRule] = [
    # Description policies.
    PolicyRule(
        # If we have a (probable) copy and one doesn't have a description,
        # we should add it.
        name="propagate_description",
        match_types=[MatchType.COPY, MatchType.LIKELY_COPY],
        is_per_field=False,
        apply_policy=lambda a, b, m: _propagate_description(
            a["properties"]["description"],
            b["properties"]["description"],
            a["urn"],
            b["urn"],
        ),
    ),
    PolicyRule(
        # If we have similar columns but one doesn't have a description,
        # we should propagate it from the other.
        #
        # FIXME: We should have exceptions for certain matches that happen
        # from similar schemas. For instance, two tables could have "id" and "country"
        # fields, but one id is a user ID and the other is a country ID. As such,
        # we shouldn't be suggesting the ID description of one to the other, despite
        # the similar schema.
        name="propagate_field_descriptions",
        match_types=[
            MatchType.COPY,
            MatchType.LIKELY_COPY,
            MatchType.TRANSFORMATION,
            MatchType.SIMILAR_SCHEMAS,
        ],
        is_per_field=True,
        apply_policy=lambda a, b, fa, fb, m: _propagate_description(
            fa["description"],
            fb["description"],
            f"{a['urn']}.{fa['fieldPath']}",
            f"{b['urn']}.{fb['fieldPath']}",
        ),
    ),
    # Likely copies should have a lineage edge.
    PolicyRule(
        name="lineage_edge_for_likely_copies",
        match_types=[MatchType.LIKELY_COPY],
        is_per_field=False,
        apply_policy=lambda a, b, m: print(
            f"suggest for {a['urn']}: add lineage edge to {b['urn']}"
        ),
    ),
    # Tags policies.
    #
    # Generally, tags that are applied to a dataset or field should be
    # propagated to similar instances.
    #
    # In the future, this policy would likely be made much more advanced.
    # For instance, it could look for "Legacy" tags on upstream datasets
    # and add alerts or warning tags to the downstreams to notify the
    # users that it relies on a legacy or deprecated dataset.
    PolicyRule(
        name="propagate_tags_for_dataset",
        match_types=[
            MatchType.COPY,
            MatchType.LIKELY_COPY,
            MatchType.TRANSFORMATION,
            MatchType.SIMILAR_SCHEMAS,
        ],
        is_per_field=False,
        apply_policy=lambda a, b, m: _propagate_tags(
            a["tags"], b["tags"], a["urn"], b["urn"]
        ),
    ),
    PolicyRule(
        name="propagate_tags_for_dataset_field",
        match_types=[
            MatchType.COPY,
            MatchType.LIKELY_COPY,
            MatchType.TRANSFORMATION,
            MatchType.SIMILAR_SCHEMAS,
        ],
        is_per_field=True,
        apply_policy=lambda a, b, fa, fb, m: _propagate_tags(
            fa["tags"],
            fb["tags"],
            f"{a['urn']}.{fa['fieldPath']}",
            f"{b['urn']}.{fb['fieldPath']}",
        ),
    ),
    # Terms policies.
    #
    # The glossary terms policy is similar to that of tags, except that datasets
    # with similar schemas should not necessarily propagate terms between them
    # without a lineage edge.
    PolicyRule(
        name="propagate_terms_for_dataset",
        match_types=[MatchType.COPY, MatchType.LIKELY_COPY, MatchType.TRANSFORMATION],
        is_per_field=False,
        apply_policy=lambda a, b, m: _propagate_terms(
            a["glossaryTerms"], b["glossaryTerms"], a["urn"], b["urn"]
        ),
    ),
    PolicyRule(
        name="propagate_terms_for_dataset_field",
        match_types=[
            MatchType.COPY,
            MatchType.LIKELY_COPY,
            MatchType.TRANSFORMATION,
            MatchType.SIMILAR_SCHEMAS,
        ],
        is_per_field=True,
        apply_policy=lambda a, b, fa, fb, m: _propagate_terms(
            fa["glossaryTerms"],
            fb["glossaryTerms"],
            f"{a['urn']}.{fa['fieldPath']}",
            f"{b['urn']}.{fb['fieldPath']}",
        ),
    ),
    # TODO: ownership propagation for copies.
]


if __name__ == "__main__":
    upstream = get_dataset(
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_public_forecasts.county_14d,PROD)"
    )
    downstream = get_dataset(
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_public_forecasts.county_14d_historical,PROD)"
    )

    apply_policies(DEFAULT_POLICIES, upstream, downstream)
