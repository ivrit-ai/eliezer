"""The shared Postgres connection pool for the hub's queue, outbox and webhooks."""

import os
import threading

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

DATABASE_URL = os.environ.get("DATABASE_URL", "")

_pool = None
_pool_lock = threading.Lock()


def pool():
    global _pool
    if _pool is None:
        # The senders, the sweeper and the first requests all get here at startup; an
        # unguarded check would build one pool each and leak all but the last.
        with _pool_lock:
            if _pool is None:
                _pool = ConnectionPool(
                    DATABASE_URL,
                    min_size=1,
                    # Long-polls hold no connection while they wait, but the edges,
                    # the app's server and the senders all query at once.
                    max_size=int(os.environ.get("DB_POOL_MAX", "20")),
                    kwargs={"row_factory": dict_row},
                    open=True,
                )
    return _pool
