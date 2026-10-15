import logging
import os
import signal
from threading import Event

from pipeline.analysis import DataError, analyze
from pipeline.store import Store

logger = logging.getLogger(__name__)


def run_once(store, analyzer=analyze):
    job = store.claim()
    if job is None:
        return False
    try:
        result = analyzer(job["source"])
    except DataError as error:
        store.fail(job, str(error), retryable=False)
    except Exception:
        # Do not persist stack traces or raw input in client-visible errors.
        logger.error("Job %s failed; retry scheduled if attempts remain", job["id"])
        store.fail(job, "Temporary processing failure", retryable=True)
    else:
        if not store.finish(job, result):
            logger.info("Discarded stale or cancelled result for %s", job["id"])
    return True


def main():
    logging.basicConfig(level=logging.INFO)
    store = Store(os.getenv("DATABASE_PATH", "data/jobs.db"))
    stop = Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    while not stop.is_set():
        if not run_once(store):
            stop.wait(0.25)


if __name__ == "__main__":
    main()
