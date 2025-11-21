# _utils/json.py

import httpx


def safe_json_parse(
    response: httpx.Response,
    context: str,
) -> dict[str, object]:
    """
    Parse JSON response, raising LookupError on any failure.

    Args:
        response (httpx.Response): The HTTP response to parse.
        context (str): Context information for error messages (e.g., ticker symbol).

    Returns:
        dict[str, object]: Parsed JSON data.

    Raises:
        LookupError: If JSON parsing fails or content-type is invalid.
    """
    # Validate content-type
    content_type = response.headers.get("content-type", "")
    if "application/json" not in content_type:
        raise LookupError(
            f"Non-JSON response (content-type: {content_type}) for {context}",
        )

    # Parse JSON
    try:
        return response.json()
    except Exception as exc:
        raise LookupError(
            f"Invalid JSON response from endpoint for {context}",
        ) from exc
