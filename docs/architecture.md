# Architecture

See Part 10 SS2.1 (playbook). End state: Kafka + batch S3 -> Spark ->
Iceberg bronze/silver/gold (star schema, SCD2 dims) -> Athena/dashboard,
with anomaly + agentic RAG layer on top. Diagram image lands here in Stage 1.
