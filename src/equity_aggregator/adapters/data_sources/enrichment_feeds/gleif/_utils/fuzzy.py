# _utils/fuzzy.py

from rapidfuzz import fuzz, utils


def rank_candidates(
    equity_name: str,
    candidates: list[tuple[str, str]],
) -> list[tuple[str, str]]:
    """
    Rank GLEIF entity candidates by fuzzy similarity to the equity name.

    Scores each candidate's legal name against the equity name using WRatio,
    filtering out candidates below the score cutoff and sorting by descending
    score.

    Returns:
        list[tuple[str, str]]: Candidates as (legal_name, lei) sorted by
            score descending, filtered to those above cutoff.
    """
    if not candidates:
        return []

    scored = [
        (
            fuzz.WRatio(
                equity_name,
                legal_name,
                processor=utils.default_process,
                score_cutoff=70,
            ),
            legal_name,
            lei,
        )
        for legal_name, lei in candidates
    ]

    return [
        (legal_name, lei)
        for score, legal_name, lei in sorted(scored, key=lambda t: t[0], reverse=True)
        if score > 0
    ]


def select_best_parent(
    equity_name: str,
    parents: list[tuple[str, str]],
) -> str | None:
    """
    Select the best parent entity from a list of parent candidates.

    Always prefers a parent entity over the original candidate, since
    parent entities are the root issuers whose LEIs are required for
    XBRL lookups. When multiple parents exist, selects the one whose
    name best matches the equity name.

    Returns:
        str | None: The best parent's LEI, or None if no parents exist.
    """
    if not parents:
        return None

    best_name, best_lei = parents[0]
    best_score = fuzz.WRatio(
        equity_name,
        best_name,
        processor=utils.default_process,
    )

    for parent_name, parent_lei in parents[1:]:
        score = fuzz.WRatio(
            equity_name,
            parent_name,
            processor=utils.default_process,
        )
        if score > best_score:
            best_score = score
            best_lei = parent_lei

    return best_lei
