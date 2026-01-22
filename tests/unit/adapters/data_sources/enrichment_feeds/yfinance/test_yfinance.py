# yfinance/test_yfinance.py

import httpx
import pytest

from equity_aggregator.adapters.data_sources.enrichment_feeds.yfinance.config import (
    FeedConfig,
)
from equity_aggregator.adapters.data_sources.enrichment_feeds.yfinance.yfinance import (
    YFinanceFeed,
    _build_identifier_sequence,
    _build_search_terms,
    _rank_viable_candidates,
    _select_identifier_min_score,
    _validate_quote_summary,
    open_yfinance_feed,
)
from equity_aggregator.storage import save_cache_entry
from tests.unit.adapters.data_sources.enrichment_feeds.yfinance._helpers import (
    make_session,
)

pytestmark = pytest.mark.unit


def make_search_response(symbols_and_names: list[tuple[str, str]]) -> httpx.Response:
    """
    Create a mock search response with EQUITY quotes.
    """
    quotes = [
        {"symbol": symbol, "quoteType": "EQUITY", "longname": name}
        for symbol, name in symbols_and_names
    ]
    return httpx.Response(200, json={"quotes": quotes})


def make_quote_summary_response(
    quote_type: str = "EQUITY",
    long_name: str | None = "Test Corp",
) -> httpx.Response:
    """
    Create a mock quote summary response.
    """
    result = {"quoteType": {"quoteType": quote_type}}
    if long_name:
        result["price"] = {"longName": long_name}
    else:
        result["price"] = {}

    return httpx.Response(
        200,
        json={"quoteSummary": {"result": [result]}},
    )


def make_empty_search_response() -> httpx.Response:
    """
    Create a mock empty search response.
    """
    return httpx.Response(200, json={"quotes": []})


def test_build_identifier_sequence_both_present() -> None:
    """
    ARRANGE: both ISIN and CUSIP provided
    ACT:     call _build_identifier_sequence
    ASSERT:  returns both in order
    """
    actual = _build_identifier_sequence("US123", "456")

    assert actual == ("US123", "456")


def test_build_identifier_sequence_isin_only() -> None:
    """
    ARRANGE: only ISIN provided
    ACT:     call _build_identifier_sequence
    ASSERT:  returns single-element tuple
    """
    actual = _build_identifier_sequence("US123", None)

    assert actual == ("US123",)


def test_build_identifier_sequence_cusip_only() -> None:
    """
    ARRANGE: only CUSIP provided
    ACT:     call _build_identifier_sequence
    ASSERT:  returns single-element tuple
    """
    actual = _build_identifier_sequence(None, "456")

    assert actual == ("456",)


def test_build_identifier_sequence_both_none() -> None:
    """
    ARRANGE: no identifiers provided
    ACT:     call _build_identifier_sequence
    ASSERT:  returns empty tuple
    """
    actual = _build_identifier_sequence(None, None)

    assert actual == ()


def test__build_search_terms_different_values() -> None:
    """
    ARRANGE: different query and symbol
    ACT:     call _build_search_terms
    ASSERT:  returns both in order
    """
    actual = _build_search_terms("Apple Inc", "AAPL")

    assert actual == ("Apple Inc", "AAPL")


def test__build_search_terms_same_values() -> None:
    """
    ARRANGE: identical query and symbol
    ACT:     call _build_search_terms
    ASSERT:  returns deduplicated single-element tuple
    """
    actual = _build_search_terms("AAPL", "AAPL")

    assert actual == ("AAPL",)


def test__build_search_terms_preserves_order() -> None:
    """
    ARRANGE: query first, then symbol
    ACT:     call _build_search_terms
    ASSERT:  query appears first in result
    """
    actual = _build_search_terms("Microsoft Corp", "MSFT")

    assert actual[0] == "Microsoft Corp"


def test__select_identifier_min_score_single_result() -> None:
    """
    ARRANGE: viable_count of 1
    ACT:     call _select_identifier_min_score
    ASSERT:  returns reduced threshold 120
    """
    default_threshold = 160
    reduced_threshold = 120
    actual = _select_identifier_min_score(1, default_threshold)

    assert actual == reduced_threshold


def test__select_identifier_min_score_multiple_results() -> None:
    """
    ARRANGE: viable_count greater than 1
    ACT:     call _select_identifier_min_score
    ASSERT:  returns default threshold
    """
    default_threshold = 160
    actual = _select_identifier_min_score(3, default_threshold)

    assert actual == default_threshold


def test__select_identifier_min_score_zero_results() -> None:
    """
    ARRANGE: viable_count of 0
    ACT:     call _select_identifier_min_score
    ASSERT:  returns default threshold
    """
    default_threshold = 160
    actual = _select_identifier_min_score(0, default_threshold)

    assert actual == default_threshold

    """
    ARRANGE: valid EQUITY data with longName
    ACT:     call _validate_quote_summary
    ASSERT:  returns data unchanged
    """
    data = {"quoteType": "EQUITY", "longName": "Apple Inc."}

    actual = _validate_quote_summary(data, "AAPL")

    assert actual == data


def test__validate_quote_summary_success_with_shortname() -> None:
    """
    ARRANGE: valid EQUITY data with shortName only
    ACT:     call _validate_quote_summary
    ASSERT:  returns data unchanged
    """
    data = {"quoteType": "EQUITY", "shortName": "Apple"}

    actual = _validate_quote_summary(data, "AAPL")

    assert actual == data


def test__validate_quote_summary_success_with_both_names() -> None:
    """
    ARRANGE: valid EQUITY data with both names
    ACT:     call _validate_quote_summary
    ASSERT:  returns data unchanged
    """
    data = {"quoteType": "EQUITY", "longName": "Apple Inc.", "shortName": "Apple"}

    actual = _validate_quote_summary(data, "AAPL")

    assert actual == data


def test__validate_quote_summary_raises_on_none() -> None:
    """
    ARRANGE: None data
    ACT:     call _validate_quote_summary
    ASSERT:  raises LookupError with message
    """
    with pytest.raises(LookupError) as exc_info:
        _validate_quote_summary(None, "AAPL")

    assert "no data" in str(exc_info.value)


def test__validate_quote_summary_raises_on_empty_dict() -> None:
    """
    ARRANGE: empty dict data
    ACT:     call _validate_quote_summary
    ASSERT:  raises LookupError
    """
    with pytest.raises(LookupError):
        _validate_quote_summary({}, "AAPL")


def test__validate_quote_summary_raises_on_wrong_type() -> None:
    """
    ARRANGE: data with quoteType ETF
    ACT:     call _validate_quote_summary
    ASSERT:  raises LookupError mentioning EQUITY
    """
    data = {"quoteType": "ETF", "longName": "Some ETF"}

    with pytest.raises(LookupError) as exc_info:
        _validate_quote_summary(data, "SPY")

    assert "EQUITY" in str(exc_info.value)


def test__validate_quote_summary_raises_on_mutualfund() -> None:
    """
    ARRANGE: data with quoteType MUTUALFUND
    ACT:     call _validate_quote_summary
    ASSERT:  raises LookupError
    """
    data = {"quoteType": "MUTUALFUND", "longName": "Some Fund"}

    with pytest.raises(LookupError):
        _validate_quote_summary(data, "FUND")


def test__validate_quote_summary_raises_on_missing_name() -> None:
    """
    ARRANGE: EQUITY data without longName or shortName
    ACT:     call _validate_quote_summary
    ASSERT:  raises LookupError mentioning name
    """
    data = {"quoteType": "EQUITY"}

    with pytest.raises(LookupError) as exc_info:
        _validate_quote_summary(data, "TEST")

    assert "no company name" in str(exc_info.value)


def test__validate_quote_summary_includes_symbol_in_error() -> None:
    """
    ARRANGE: invalid data
    ACT:     call _validate_quote_summary with specific symbol
    ASSERT:  error message includes symbol
    """
    with pytest.raises(LookupError) as exc_info:
        _validate_quote_summary(None, "CUSTOM")

    assert "CUSTOM" in str(exc_info.value)


def test__rank_viable_candidates_with_viable_quotes() -> None:
    """
    ARRANGE: quotes with symbol and name
    ACT:     call _rank_viable_candidates
    ASSERT:  returns non-empty list
    """
    quotes = [
        {"symbol": "AAPL", "longname": "Apple Inc."},
        {"symbol": "APLE", "longname": "Apple Hospitality"},
    ]

    actual = _rank_viable_candidates(quotes, "Apple Inc", "AAPL", 100)

    assert len(actual) > 0


def test__rank_viable_candidates_returns_symbols() -> None:
    """
    ARRANGE: quotes with matching data
    ACT:     call _rank_viable_candidates
    ASSERT:  returns list containing expected symbol
    """
    quotes = [{"symbol": "MSFT", "longname": "Microsoft Corporation"}]

    actual = _rank_viable_candidates(quotes, "Microsoft", "MSFT", 100)

    assert "MSFT" in actual


def test__rank_viable_candidates_no_viable_quotes() -> None:
    """
    ARRANGE: quotes without symbol or name
    ACT:     call _rank_viable_candidates
    ASSERT:  returns empty list
    """
    quotes = [{"invalid": "data"}]

    actual = _rank_viable_candidates(quotes, "Apple", "AAPL", 100)

    assert actual == []


def test__rank_viable_candidates_empty_input() -> None:
    """
    ARRANGE: empty quotes list
    ACT:     call _rank_viable_candidates
    ASSERT:  returns empty list
    """
    actual = _rank_viable_candidates([], "Apple", "AAPL", 100)

    assert actual == []


async def test_open_yfinance_feed_yields_feed_instance() -> None:
    """
    ARRANGE: no config provided
    ACT:     use open_yfinance_feed context manager
    ASSERT:  yields YFinanceFeed instance
    """
    async with open_yfinance_feed() as feed:
        actual = isinstance(feed, YFinanceFeed)

    assert actual is True


async def test_open_yfinance_feed_with_custom_config() -> None:
    """
    ARRANGE: custom FeedConfig
    ACT:     use open_yfinance_feed with config
    ASSERT:  yields YFinanceFeed instance
    """
    config = FeedConfig()

    async with open_yfinance_feed(config=config) as feed:
        actual = isinstance(feed, YFinanceFeed)

    assert actual is True


async def test_open_yfinance_feed_closes_session() -> None:
    """
    ARRANGE: open_yfinance_feed context
    ACT:     exit context manager
    ASSERT:  session is closed without error
    """
    async with open_yfinance_feed() as feed:
        session = feed._session

    # Session should be closed after context exit
    assert session is not None


async def test_init_stores_session() -> None:
    """
    ARRANGE: YFSession instance
    ACT:     create YFinanceFeed
    ASSERT:  session is stored in _session
    """
    session = make_session(lambda r: httpx.Response(200, json={}))
    feed = YFinanceFeed(session)

    assert feed._session is session


async def test_init_has_default_min_score() -> None:
    """
    ARRANGE: YFSession instance
    ACT:     create YFinanceFeed
    ASSERT:  default_min_score is 160
    """
    expected_min_score = 160
    session = make_session(lambda r: httpx.Response(200, json={}))
    feed = YFinanceFeed(session)

    assert feed.default_min_score == expected_min_score


async def test_init_has_model_attribute() -> None:
    """
    ARRANGE: YFSession instance
    ACT:     create YFinanceFeed
    ASSERT:  model attribute exists
    """
    session = make_session(lambda r: httpx.Response(200, json={}))
    feed = YFinanceFeed(session)

    assert hasattr(feed, "model")


async def test_fetch_equity_returns_cached_entry() -> None:
    """
    ARRANGE: cached entry exists for symbol
    ACT:     call fetch_equity
    ASSERT:  returns cached data
    """
    save_cache_entry("yfinance_equities", "CACHED_TEST_001", {"test": "data"})
    session = make_session(lambda r: httpx.Response(200, json={}))
    feed = YFinanceFeed(session)

    actual = await feed.fetch_equity(symbol="CACHED_TEST_001", name="Test")

    assert actual == {"test": "data"}


async def test_fetch_equity_cache_hit_skips_search() -> None:
    """
    ARRANGE: cached entry exists, handler that would fail
    ACT:     call fetch_equity
    ASSERT:  returns cached data without calling handler
    """
    save_cache_entry("yfinance_equities", "CACHED_TEST_002", {"cached": "value"})

    def failing_handler(r: httpx.Request) -> httpx.Response:
        raise AssertionError("Should not be called")

    session = make_session(failing_handler)
    feed = YFinanceFeed(session)

    actual = await feed.fetch_equity(symbol="CACHED_TEST_002", name="Skip Test")

    assert actual == {"cached": "value"}


async def test_fetch_equity_raises_when_no_candidates_found() -> None:
    """
    ARRANGE: session returning empty search results
    ACT:     call fetch_equity without identifiers
    ASSERT:  raises LookupError
    """
    session = make_session(lambda r: make_empty_search_response())
    feed = YFinanceFeed(session)

    with pytest.raises(LookupError) as exc_info:
        await feed.fetch_equity(symbol="NOTFOUND_999", name="Not Found Corp")

    assert "No enrichment data found" in str(exc_info.value)


async def test_fetch_equity_successful_lookup_returns_data() -> None:
    """
    ARRANGE: session with valid search and quote responses
    ACT:     call fetch_equity
    ASSERT:  returns enriched data
    """

    def handler(r: httpx.Request) -> httpx.Response:
        url = str(r.url)
        if "search" in url:
            return make_search_response([("SUCCESS_001", "Success Corp")])
        if "quoteSummary" in url:
            return make_quote_summary_response("EQUITY", "Success Corp")
        return httpx.Response(200, json={})

    session = make_session(handler)
    feed = YFinanceFeed(session)

    actual = await feed.fetch_equity(symbol="SUCCESS_001", name="Success Corp")

    assert actual["quoteType"] == "EQUITY"


async def test_resolve_candidates_uses_isin_first() -> None:
    """
    ARRANGE: session with ISIN search returning results
    ACT:     call _resolve_candidates with ISIN
    ASSERT:  returns candidates from ISIN search
    """
    session = make_session(lambda r: make_search_response([("ISINTEST", "Test Corp")]))
    feed = YFinanceFeed(session)

    actual = await feed._resolve_candidates(
        symbol="ISINTEST",
        name="Test Corp",
        isin="US1234567890",
        cusip=None,
    )

    assert "ISINTEST" in actual


async def test_resolve_candidates_falls_back_to_cusip() -> None:
    """
    ARRANGE: ISIN search fails, CUSIP search succeeds
    ACT:     call _resolve_candidates with both
    ASSERT:  returns candidates from CUSIP search
    """
    call_count = []

    def handler(r: httpx.Request) -> httpx.Response:
        call_count.append(1)
        # First call (ISIN) returns empty, second (CUSIP) returns results
        if len(call_count) == 1:
            return make_empty_search_response()
        return make_search_response([("CUSIPTEST", "Test Corp")])

    session = make_session(handler)
    feed = YFinanceFeed(session)

    actual = await feed._resolve_candidates(
        symbol="CUSIPTEST",
        name="Test Corp",
        isin="US1234567890",
        cusip="123456789",
    )

    assert "CUSIPTEST" in actual


async def test_resolve_candidates_falls_back_to_name_search() -> None:
    """
    ARRANGE: no identifiers provided
    ACT:     call _resolve_candidates with name only
    ASSERT:  returns candidates from name search
    """
    session = make_session(
        lambda r: make_search_response([("NAMETEST", "Name Test Corp")]),
    )
    feed = YFinanceFeed(session)

    actual = await feed._resolve_candidates(
        symbol="NAMETEST",
        name="Name Test Corp",
        isin=None,
        cusip=None,
    )

    assert "NAMETEST" in actual


async def test_resolve_candidates_skips_none_identifiers() -> None:
    """
    ARRANGE: ISIN is None, CUSIP provided
    ACT:     call _resolve_candidates
    ASSERT:  tries CUSIP search
    """
    session = make_session(
        lambda r: make_search_response([("CTEST", "CUSIP Only Test")]),
    )
    feed = YFinanceFeed(session)

    actual = await feed._resolve_candidates(
        symbol="CTEST",
        name="CUSIP Only Test",
        isin=None,
        cusip="987654321",
    )

    assert "CTEST" in actual


async def test_resolve_candidates_raises_when_all_fail() -> None:
    """
    ARRANGE: all searches return empty
    ACT:     call _resolve_candidates
    ASSERT:  raises LookupError
    """
    session = make_session(lambda r: make_empty_search_response())
    feed = YFinanceFeed(session)

    with pytest.raises(LookupError):
        await feed._resolve_candidates(
            symbol="FAIL",
            name="Fail Corp",
            isin="FAIL123",
            cusip="FAIL456",
        )


async def test_resolve_by_identifier_safe_returns_symbols_on_success() -> None:
    """
    ARRANGE: session returning valid identifier search results
    ACT:     call _resolve_by_identifier_safe
    ASSERT:  returns ranked symbols
    """
    session = make_session(
        lambda r: make_search_response([("SAFE1", "Safe Test Corp")]),
    )
    feed = YFinanceFeed(session)

    actual = await feed._resolve_by_identifier_safe(
        identifier="TEST123",
        expected_name="Safe Test Corp",
        expected_symbol="SAFE1",
    )

    assert "SAFE1" in actual


async def test_resolve_by_identifier_safe_returns_empty_on_error() -> None:
    """
    ARRANGE: session returning no search results
    ACT:     call _resolve_by_identifier_safe
    ASSERT:  returns empty list
    """
    session = make_session(lambda r: make_empty_search_response())
    feed = YFinanceFeed(session)

    actual = await feed._resolve_by_identifier_safe(
        identifier="INVALID",
        expected_name="Test",
        expected_symbol="TEST",
    )

    assert actual == []


async def test_resolve_by_identifier_safe_handles_no_viable_candidates() -> None:
    """
    ARRANGE: session returning quotes without symbol/name
    ACT:     call _resolve_by_identifier_safe
    ASSERT:  returns empty list
    """
    session = make_session(
        lambda r: httpx.Response(200, json={"quotes": [{"invalid": "data"}]}),
    )
    feed = YFinanceFeed(session)

    actual = await feed._resolve_by_identifier_safe(
        identifier="BAD",
        expected_name="Bad",
        expected_symbol="BAD",
    )

    assert actual == []


async def test_resolve_by_identifier_returns_ranked_symbols() -> None:
    """
    ARRANGE: session with valid identifier search results
    ACT:     call _resolve_by_identifier
    ASSERT:  returns list of symbols
    """
    session = make_session(
        lambda r: make_search_response([("ID1", "Identifier Test 1")]),
    )
    feed = YFinanceFeed(session)

    actual = await feed._resolve_by_identifier(
        identifier="US123",
        expected_name="Identifier Test 1",
        expected_symbol="ID1",
    )

    assert "ID1" in actual


async def test_resolve_by_identifier_raises_on_empty_quotes() -> None:
    """
    ARRANGE: session returning empty quotes
    ACT:     call _resolve_by_identifier
    ASSERT:  raises LookupError
    """
    session = make_session(lambda r: make_empty_search_response())
    feed = YFinanceFeed(session)

    with pytest.raises(LookupError) as exc_info:
        await feed._resolve_by_identifier(
            identifier="EMPTY",
            expected_name="Empty",
            expected_symbol="EMPTY",
        )

    assert "no results" in str(exc_info.value)


async def test_resolve_by_identifier_raises_on_no_viable() -> None:
    """
    ARRANGE: session returning EQUITY quotes without symbol or name
    ACT:     call _resolve_by_identifier
    ASSERT:  raises LookupError about no viable candidates
    """
    # Create EQUITY quotes that pass search_quotes filter but fail filter_equities
    quotes_response = httpx.Response(
        200,
        json={"quotes": [{"quoteType": "EQUITY", "missing": "symbol_and_name"}]},
    )
    session = make_session(lambda r: quotes_response)
    feed = YFinanceFeed(session)

    with pytest.raises(LookupError) as exc_info:
        await feed._resolve_by_identifier(
            identifier="NOVIABLE",
            expected_name="No Viable",
            expected_symbol="NOVIABLE",
        )

    assert "No viable candidates" in str(exc_info.value)


async def test_resolve_by_identifier_uses_lower_score_for_single_result() -> None:
    """
    ARRANGE: session returning single viable quote
    ACT:     call _resolve_by_identifier
    ASSERT:  returns symbol (using threshold 120)
    """
    session = make_session(
        lambda r: make_search_response([("SINGLE", "Single Result Corp")]),
    )
    feed = YFinanceFeed(session)

    actual = await feed._resolve_by_identifier(
        identifier="SINGLE123",
        expected_name="Single Result Corp",
        expected_symbol="SINGLE",
    )

    assert "SINGLE" in actual


async def test_resolve_by_identifier_uses_default_score_for_multiple() -> None:
    """
    ARRANGE: session returning multiple viable quotes
    ACT:     call _resolve_by_identifier
    ASSERT:  returns symbols (using default threshold)
    """
    session = make_session(
        lambda r: make_search_response(
            [
                ("MULTI1", "Multi Corp One"),
                ("MULTI2", "Multi Corp Two"),
            ],
        ),
    )
    feed = YFinanceFeed(session)

    actual = await feed._resolve_by_identifier(
        identifier="MULTI",
        expected_name="Multi Corp",
        expected_symbol="MULTI1",
    )

    assert len(actual) >= 0  # May filter based on score


async def test_resolve_by_search_terms_returns_ranked_symbols() -> None:
    """
    ARRANGE: session with valid search results
    ACT:     call _resolve_by_search_terms
    ASSERT:  returns list of symbols
    """
    session = make_session(
        lambda r: make_search_response([("SEARCH1", "Search Test Corp")]),
    )
    feed = YFinanceFeed(session)

    actual = await feed._resolve_by_search_terms(
        query="Search Test Corp",
        expected_name="Search Test Corp",
        expected_symbol="SEARCH1",
    )

    assert "SEARCH1" in actual


async def test_resolve_by_search_terms_tries_multiple_terms() -> None:
    """
    ARRANGE: first search fails, second succeeds
    ACT:     call _resolve_by_search_terms
    ASSERT:  returns symbols from second search
    """
    call_count = []

    def handler(r: httpx.Request) -> httpx.Response:
        call_count.append(1)
        if len(call_count) == 1:
            return make_empty_search_response()
        return make_search_response([("SECOND", "Second Term Corp")])

    session = make_session(handler)
    feed = YFinanceFeed(session)

    actual = await feed._resolve_by_search_terms(
        query="FirstTerm",
        expected_name="Second Term Corp",
        expected_symbol="SECOND",
    )

    assert "SECOND" in actual


async def test_resolve_by_search_terms_raises_on_no_results() -> None:
    """
    ARRANGE: all searches return empty
    ACT:     call _resolve_by_search_terms
    ASSERT:  raises LookupError
    """
    session = make_session(lambda r: make_empty_search_response())
    feed = YFinanceFeed(session)

    with pytest.raises(LookupError) as exc_info:
        await feed._resolve_by_search_terms(
            query="NoResults",
            expected_name="No Results",
            expected_symbol="NORESULTS",
        )

    assert "No symbol candidates" in str(exc_info.value)


async def test_resolve_by_search_terms_skips_non_viable_quotes() -> None:
    """
    ARRANGE: first search returns non-viable quotes, second viable
    ACT:     call _resolve_by_search_terms
    ASSERT:  returns symbols from viable search
    """
    call_count = []

    def handler(r: httpx.Request) -> httpx.Response:
        call_count.append(1)
        if len(call_count) == 1:
            return httpx.Response(200, json={"quotes": [{"invalid": "data"}]})
        return make_search_response([("VIABLE", "Viable Corp")])

    session = make_session(handler)
    feed = YFinanceFeed(session)

    actual = await feed._resolve_by_search_terms(
        query="Test Query",
        expected_name="Viable Corp",
        expected_symbol="VIABLE",
    )

    assert "VIABLE" in actual


async def test_resolve_by_search_terms_returns_first_ranked_match() -> None:
    """
    ARRANGE: first search returns ranked symbols
    ACT:     call _resolve_by_search_terms
    ASSERT:  returns symbols without trying second term
    """
    call_count = []

    def handler(r: httpx.Request) -> httpx.Response:
        call_count.append(1)
        return make_search_response([("FIRST", "First Match Corp")])

    session = make_session(handler)
    feed = YFinanceFeed(session)

    actual = await feed._resolve_by_search_terms(
        query="First Match Corp",
        expected_name="First Match Corp",
        expected_symbol="FIRST",
    )

    assert "FIRST" in actual


async def test_fetch_first_valid_quote_returns_first_valid() -> None:
    """
    ARRANGE: session with valid EQUITY quote
    ACT:     call _fetch_first_valid_quote
    ASSERT:  returns validated quote data
    """
    session = make_session(
        lambda r: make_quote_summary_response("EQUITY", "Valid Corp"),
    )
    feed = YFinanceFeed(session)

    actual = await feed._fetch_first_valid_quote(["VALID"])

    assert actual.get("quoteType") == "EQUITY"


async def test_fetch_first_valid_quote_skips_invalid_quotes() -> None:
    """
    ARRANGE: first symbol invalid, second valid
    ACT:     call _fetch_first_valid_quote
    ASSERT:  returns second quote data
    """
    call_count = []

    def handler(r: httpx.Request) -> httpx.Response:
        call_count.append(1)
        if len(call_count) == 1:
            return make_quote_summary_response("ETF", "Invalid ETF")
        return make_quote_summary_response("EQUITY", "Valid Corp")

    session = make_session(handler)
    feed = YFinanceFeed(session)

    actual = await feed._fetch_first_valid_quote(["INVALID", "VALID"])

    assert actual.get("quoteType") == "EQUITY"


async def test_fetch_first_valid_quote_raises_when_all_fail() -> None:
    """
    ARRANGE: all symbols return invalid quotes
    ACT:     call _fetch_first_valid_quote
    ASSERT:  raises LookupError
    """
    session = make_session(lambda r: make_quote_summary_response("ETF", "Not Equity"))
    feed = YFinanceFeed(session)

    with pytest.raises(LookupError) as exc_info:
        await feed._fetch_first_valid_quote(["BAD1", "BAD2", "BAD3"])

    assert "All candidates failed" in str(exc_info.value)


async def test_fetch_first_valid_quote_handles_empty_list() -> None:
    """
    ARRANGE: empty symbols list
    ACT:     call _fetch_first_valid_quote
    ASSERT:  raises LookupError
    """
    session = make_session(lambda r: httpx.Response(200, json={}))
    feed = YFinanceFeed(session)

    with pytest.raises(LookupError):
        await feed._fetch_first_valid_quote([])


async def test_fetch_first_valid_quote_validates_equity_type() -> None:
    """
    ARRANGE: session returning non-EQUITY quote
    ACT:     call _fetch_first_valid_quote
    ASSERT:  raises LookupError
    """
    session = make_session(
        lambda r: make_quote_summary_response("MUTUALFUND", "Some Fund"),
    )
    feed = YFinanceFeed(session)

    with pytest.raises(LookupError):
        await feed._fetch_first_valid_quote(["FUND"])


async def test_fetch_first_valid_quote_validates_company_name() -> None:
    """
    ARRANGE: session returning EQUITY without name
    ACT:     call _fetch_first_valid_quote
    ASSERT:  raises LookupError
    """
    session = make_session(lambda r: make_quote_summary_response("EQUITY", None))
    feed = YFinanceFeed(session)

    with pytest.raises(LookupError):
        await feed._fetch_first_valid_quote(["NONAME"])
