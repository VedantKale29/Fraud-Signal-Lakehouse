"""
Stage 1 -- adapt the Elliptic dataset to our data contract.

WHAT ELLIPTIC GIVES US (and honestly, what it doesn't):
  elliptic_txs_features.csv : tx_id + time_step (1..49) + 165 anon features
  elliptic_txs_classes.csv  : tx_id -> '1' illicit / '2' licit / 'unknown'
  (no real amounts, no wallet ids, no timestamps -- it's anonymized)

MAPPING RULES (all deterministic -> re-runs are byte-identical, which the
idempotency gate requires; all documented so you can defend them):
  event_ts  = BASE_DATE + (time_step - 1) days + jitter(hash(tx_id))
              -> preserves Elliptic's real temporal ordering
  wallet_id = 'w-' + (stable_hash(tx_id) % N_WALLETS)
              -> synthetic but STABLE: same tx always maps to same wallet,
                 so SCD2 and per-wallet windows behave consistently
  value     = scaled |first anon feature|  -> synthetic magnitude proxy
  labels    -> separate dim_fraud_label feed; THE genuinely real signal,
               used for Stage-4 model evaluation (time-split, never random)

If the CSVs aren't downloaded yet, generate_synthetic() produces
contract-shaped data so every downstream stage is buildable today.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from fraud_lakehouse.common.exceptions import IngestionError
from fraud_lakehouse.common.logger import get_logger

logger = get_logger(__name__)

BASE_DATE = datetime(2026, 1, 1)
N_WALLETS = 5_000
LABEL_MAP = {"1": "ILLICIT", "2": "LICIT", "unknown": "UNKNOWN"}


def load_elliptic(spark, features_csv: Path, classes_csv: Path):
    """Return (transactions_df, labels_df) shaped to the data contract."""
    try:
        from pyspark.sql import functions as F

        features_csv, classes_csv = Path(features_csv), Path(classes_csv)
        for p in (features_csv, classes_csv):
            if not p.exists():
                raise IngestionError(f"Elliptic file missing: {p}")

        feats = spark.read.csv(str(features_csv), header=False, inferSchema=True)
        cols = feats.columns
        feats = (
            feats.withColumnRenamed(cols[0], "tx_id")
            .withColumnRenamed(cols[1], "time_step")
            .withColumnRenamed(cols[2], "f0")
        )

        # NATIVE hashing (no Python UDF): md5 -> first 15 hex chars ->
        # base-16 to base-10 -> mod N. Pure JVM, so (a) no Python worker
        # processes to spawn (kills the Windows "worker failed to connect
        # back" class of failure entirely) and (b) no Python<->JVM
        # serialization overhead. Deterministic across machines: md5 is md5.
        def md5_mod(col, m):
            return F.pmod(
                F.conv(F.substring(F.md5(col.cast("string")), 1, 15), 16, 10).cast("long"),
                F.lit(m),
            )

        transactions = feats.select(
            F.col("tx_id").cast("string").alias("tx_id"),
            F.concat(F.lit("w-"), md5_mod(F.col("tx_id"), N_WALLETS)).alias("wallet_id"),
            F.concat(
                F.lit("w-"),
                md5_mod(F.concat(F.col("tx_id").cast("string"), F.lit("cp")), N_WALLETS),
            ).alias("counterparty_id"),
            (
                F.lit(BASE_DATE)
                + F.make_interval(days=F.col("time_step") - 1)
                + F.make_interval(secs=md5_mod(F.col("tx_id"), 86_400))
            ).alias("event_ts"),
            F.round(F.abs(F.col("f0")) * 1000 + 1, 2).cast("string").alias("value"),
            F.lit("BTC").alias("asset"),
        )

        classes = spark.read.csv(str(classes_csv), header=True, inferSchema=False)
        c0, c1 = classes.columns
        labels = classes.select(
            F.col(c0).cast("string").alias("tx_id"),
            F.col(c1).alias("raw_class"),
        ).withColumn(
            "fraud_label",
            F.when(F.col("raw_class") == "1", "ILLICIT")
            .when(F.col("raw_class") == "2", "LICIT")
            .otherwise("UNKNOWN"),
        ).drop("raw_class")

        logger.info(
            "elliptic loaded | tx=%d labels=%d", transactions.count(), labels.count()
        )
        return transactions, labels
    except IngestionError:
        raise
    except Exception as e:
        logger.error("elliptic load failed", exc_info=True)
        raise IngestionError("Elliptic load failed", e) from e


def generate_synthetic(spark, n: int = 10_000):
    """Contract-shaped synthetic data -- keeps every stage buildable before
    the real download. Deterministic (seeded) for reproducible tests."""
    try:
        from pyspark.sql import functions as F

        base = spark.range(n).withColumnRenamed("id", "seq")

        def md5_mod(col, m):
            return F.pmod(
                F.conv(F.substring(F.md5(col.cast("string")), 1, 15), 16, 10).cast("long"),
                F.lit(m),
            )

        return base.select(
            F.concat(F.lit("tx-"), F.col("seq")).alias("tx_id"),
            F.concat(F.lit("w-"), md5_mod(F.col("seq"), 500)).alias("wallet_id"),
            F.concat(F.lit("w-"), md5_mod(F.col("seq") + 7, 500)).alias(
                "counterparty_id"
            ),
            (
                F.lit(BASE_DATE) + F.make_interval(secs=(F.col("seq") % 2_592_000))
            ).alias("event_ts"),
            F.round(F.pmod(F.col("seq") * 37.7, 5000.0) + 1, 2).cast("string").alias("value"),
            F.when(F.col("seq") % 3 == 0, "ETH").otherwise("BTC").alias("asset"),
        )
    except Exception as e:
        raise IngestionError("synthetic generation failed", e) from e
