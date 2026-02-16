# _utils/normalise.py


_SINGLE_WORD_SUFFIXES = frozenset(
    {
        "AB",
        "AG",
        "ASA",
        "BV",
        "CO",
        "CORP",
        "CORPORATION",
        "GMBH",
        "INC",
        "INCORPORATED",
        "KG",
        "KGAA",
        "LIMITED",
        "LLC",
        "LP",
        "LTD",
        "NV",
        "OYJ",
        "PLC",
        "SA",
        "SARL",
        "SAS",
        "SE",
        "SPA",
    },
)

_MULTI_WORD_SUFFIXES = (("PUBLIC", "LIMITED", "COMPANY"),)


def strip_corporate_suffix(name: str) -> str:
    """
    Strip common corporate suffixes from an equity name.

    Removes legal form indicators (PLC, INC, AG, etc.) from the end of
    the name to produce a cleaner search query for the GLEIF API. Handles
    both single-word and multi-word suffixes, including dotted forms like
    P.L.C. and Inc.

    Returns:
        str: The name with the corporate suffix removed, or the original
            name if no suffix is found or stripping would leave it empty.
    """
    words = name.split()

    stripped = _try_strip_multi_word(words)
    if stripped is not None:
        return stripped

    stripped = _try_strip_single_word(words)
    if stripped is not None:
        return stripped

    return name


def _try_strip_multi_word(
    words: list[str],
) -> str | None:
    """
    Attempt to strip a multi-word corporate suffix.

    Returns:
        str | None: The stripped name, or None if no multi-word suffix
            matched.
    """
    for suffix_words in _MULTI_WORD_SUFFIXES:
        n = len(suffix_words)
        if len(words) <= n:
            continue

        tail = tuple(w.upper().strip(".,") for w in words[-n:])
        if tail == suffix_words:
            return " ".join(words[:-n]).rstrip(" ,.")

    return None


def _try_strip_single_word(
    words: list[str],
) -> str | None:
    """
    Attempt to strip a single-word corporate suffix.

    Normalises the last word by removing dots and trailing punctuation
    before checking against known suffixes. This handles dotted forms
    such as P.L.C., Inc., and S.A.

    Returns:
        str | None: The stripped name, or None if no suffix matched or
            the name has only one word.
    """
    if len(words) <= 1:
        return None

    normalised = words[-1].upper().replace(".", "").rstrip(",")
    if normalised not in _SINGLE_WORD_SUFFIXES:
        return None

    return " ".join(words[:-1]).rstrip(" ,.")
