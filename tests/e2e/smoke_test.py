"""Stage-3 post-deploy smoke test (SS6.2): tiny known dataset through the
real path -- upload -> audit -> assert. Runs after every CD deploy.
Exit code IS the gate: non-zero fails the deploy."""

import sys
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

from fraud_lakehouse.common.config import load_config
from fraud_lakehouse.common.logger import get_logger
from fraud_lakehouse.ingestion.batch_ingest import BronzeIngestor
from fraud_lakehouse.utils.s3_utils import list_keys

logger = get_logger("e2e.smoke")


def main() -> int:
    cfg = load_config()
    smoke_date = date(1999, 1, 1)  # sentinel date -- never collides with real data
    with TemporaryDirectory() as td:
        f = Path(td) / "smoke.parquet"
        f.write_bytes(b"smoke-bytes")
        ing = BronzeIngestor(cfg)
        ing.run(f, smoke_date)
        ing.run(f, smoke_date)  # idempotency check in prod too
        keys = list_keys(cfg.s3.bucket, ing.partition_prefix(smoke_date))
        if len(keys) != 1:
            logger.error("SMOKE FAIL: expected 1 object, found %d", len(keys))
            return 1
    logger.info("SMOKE PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
