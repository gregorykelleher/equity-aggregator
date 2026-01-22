# storage/cache.py

import pickle
import sqlite3
import time

from ._utils import CACHE_TABLE, connect, ttl_seconds


def load_cache(cache_name: str) -> object | None:
    """
    Retrieve a cached object from the cache store using its cache name.

    Opens a database connection, purges expired entries, and fetches the cached
    value associated with the given cache name. Returns None if no entry is
    found or if cache_name is None.

    Args:
        cache_name: Unique identifier for the cached object.

    Returns:
        object | None: The cached object if present, otherwise None.
    """
    if cache_name is None:
        return None

    with connect() as conn:
        return _cache_get(conn, cache_name, "_")


def _cache_get(conn: sqlite3.Connection, cache_name: str, key: str) -> object | None:
    """
    Retrieve a cached object from database for the specified cache and key.

    This function initialises the cache table if it does not exist, purges any
    expired entries for the given cache name and key, and then attempts to fetch
    the cached payload. If a cached value is found, it is deserialised and
    returned; otherwise, None is returned.

    Args:
        conn: The SQLite database connection.
        cache_name: The name of the cache to query.
        key: The key identifying the cached object.

    Returns:
        object | None: The deserialised cached object if found, otherwise None.
    """
    _init_cache_table(conn)

    _purge_expired(conn, cache_name, key)

    row = conn.execute(
        f"SELECT payload FROM {CACHE_TABLE} WHERE cache_name = ? AND key = ?",
        (cache_name, key),
    ).fetchone()

    return pickle.loads(row[0]) if row else None


def _init_cache_table(conn: sqlite3.Connection) -> None:
    """
    Initialise the cache table in the provided SQLite database connection.

    Creates a table named as specified by the module-level variable `CACHE_TABLE`
    if it does not already exist. The table includes columns for cache name, key,
    creation timestamp, and payload. The combination of cache name and key serves
    as the primary key.

    Args:
        conn: The SQLite database connection to use for table creation.
    """
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {CACHE_TABLE} (
            cache_name   TEXT NOT NULL,
            key          TEXT NOT NULL,
            created_at   INTEGER NOT NULL,
            payload      BLOB NOT NULL,
            PRIMARY KEY (cache_name, key)
        );
        """,
    )


def _purge_expired(conn: sqlite3.Connection, cache_name: str, key: str | None) -> None:
    """
    Remove expired cache entries from database for a given cache and key.

    Entries are considered expired if the time elapsed since their creation
    exceeds the configured time-to-live (TTL) value. If TTL is set to 0,
    expiry is disabled and no entries are removed.

    Args:
        conn: The SQLite database connection to use for deletion.
        cache_name: The name of the cache to purge expired entries from.
        key: The specific cache key to purge. If None, purges entries
            with a NULL key.
    """
    ttl = ttl_seconds()

    if ttl == 0:
        return  # expiry disabled

    now = int(time.time())
    where_key = "key = ?" if key is not None else "key IS ?"

    conn.execute(
        f"DELETE FROM {CACHE_TABLE} "
        f"WHERE cache_name = ? AND {where_key} AND ? - created_at > ?",
        (cache_name, key, now, ttl),
    )


def load_cache_entry(cache_name: str, key: str) -> object | None:
    """
    Retrieve a cached entry from the specified cache using the provided key.

    Args:
        cache_name: The name of the cache to retrieve the entry from.
        key: The key identifying the cached entry.

    Returns:
        object | None: The cached object if found, otherwise None.
    """
    with connect() as conn:
        return _cache_get(conn, cache_name, key)


def save_cache(cache_name: str, value: object) -> None:
    """
    Save a value to the cache with the specified cache name.

    Args:
        cache_name: The unique identifier for the cache entry.
        value: The value to be stored in the cache.
    """
    with connect() as conn:
        _cache_put(conn, cache_name, "_", value)


def _cache_put(
    conn: sqlite3.Connection,
    cache_name: str,
    key: str,
    value: object,
) -> None:
    """
    Store a value in the SQLite cache table with the specified name and key.

    If an entry with the same cache name and key already exists, it will be
    replaced. The value is serialised using pickle before storage, and the
    current timestamp is recorded as the creation time.

    Args:
        conn: The SQLite database connection object.
        cache_name: The name of the cache to store the value under.
        key: The key identifying the cached value.
        value: The Python object to cache; must be pickle-serialisable.
    """
    _init_cache_table(conn)

    conn.execute(
        f"INSERT OR REPLACE INTO {CACHE_TABLE} "
        "(cache_name, key, created_at, payload) "
        "VALUES (?, ?, ?, ?)",
        (cache_name, key, int(time.time()), pickle.dumps(value, protocol=4)),
    )


def save_cache_entry(
    cache_name: str,
    key: str,
    value: object,
) -> None:
    """
    Save a value in the cache under the given cache name and key.

    Opens a connection to the cache store and persists the value using the
    specified cache name and key. If cache_name is None, the function does
    nothing.

    Args:
        cache_name: Name of the cache to store the entry in.
        key: Key under which the value will be stored.
        value: The value to cache; must be pickle-serialisable.
    """
    if cache_name is None:
        return

    with connect() as conn:
        _cache_put(conn, cache_name, key, value)
