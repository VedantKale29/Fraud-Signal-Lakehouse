"""Stage-1 gate: Elliptic adaptation is contract-shaped AND deterministic
(determinism feeds the pipeline idempotency gate)."""

from chispa.dataframe_comparer import assert_df_equality

from fraud_lakehouse.ingestion.elliptic_loader import generate_synthetic, load_elliptic

CONTRACT = {"tx_id", "wallet_id", "counterparty_id", "event_ts", "value", "asset"}


def _fake_elliptic(tmp_path):
    # 3 tx nodes across 2 time steps + a classes file (header, like the real one)
    f = tmp_path / "elliptic_txs_features.csv"
    f.write_text("101,1,0.5\n102,1,-2.0\n103,2,1.25\n")
    c = tmp_path / "elliptic_txs_classes.csv"
    c.write_text("txId,class\n101,1\n102,2\n103,unknown\n")
    return f, c


def test_output_matches_contract_and_labels_map(spark, tmp_path):
    f, c = _fake_elliptic(tmp_path)
    tx, labels = load_elliptic(spark, f, c)
    assert set(tx.columns) == CONTRACT
    assert tx.count() == 3
    got = {r.tx_id: r.fraud_label for r in labels.collect()}
    assert got == {"101": "ILLICIT", "102": "LICIT", "103": "UNKNOWN"}


def test_temporal_ordering_preserved(spark, tmp_path):
    """time_step 2 must land on a later date than time_step 1 -- Elliptic's
    real temporal signal survives the mapping (Stage-4 time-split depends on it)."""
    f, c = _fake_elliptic(tmp_path)
    tx, _ = load_elliptic(spark, f, c)
    rows = {r.tx_id: r.event_ts for r in tx.collect()}
    assert rows["103"].date() > rows["101"].date()


def test_loader_is_deterministic(spark, tmp_path):
    f, c = _fake_elliptic(tmp_path)
    tx1, _ = load_elliptic(spark, f, c)
    tx2, _ = load_elliptic(spark, f, c)
    assert_df_equality(tx1, tx2, ignore_row_order=True)


def test_synthetic_generator_contract_and_determinism(spark):
    a, b = generate_synthetic(spark, 50), generate_synthetic(spark, 50)
    assert set(a.columns) == CONTRACT
    assert_df_equality(a, b, ignore_row_order=True)
