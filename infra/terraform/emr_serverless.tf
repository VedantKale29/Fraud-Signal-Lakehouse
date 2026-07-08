# EMR Serverless: the batch compute. Serverless because at solo scale you
# pay per vCPU-second of actual runs -- no idle cluster billing (the
# "managed beats self-managed at my scale" interview line, in code).

resource "aws_emrserverless_application" "spark" {
  name          = "fsl-spark-${var.env}"
  release_label = "emr-7.2.0" # Spark 3.5 line -- matches the pinned stack
  type          = "spark"

  maximum_capacity {
    cpu    = "16 vCPU"
    memory = "64 GB"
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 5 # cost guardrail: dies fast when idle
  }
}
