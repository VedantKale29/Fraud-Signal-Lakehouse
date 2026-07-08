# End-to-end / chaos tests (gates from Part 10)

- `test_idempotency.py` (Stage 1 gate): run the DAG twice for one logical
  date; row counts + checksums identical.
- `test_kill_restart.py` (Stage 2 gate): SIGKILL the stream mid-batch,
  restart from checkpoint, diff against batch reference.
- `test_rebuild_env.py` (Stage 3 gate): terraform destroy -> apply -> one
  green pipeline run in < 60 min.
