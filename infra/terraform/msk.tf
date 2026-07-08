# MSK Serverless: provisioned ONLY for the recorded demo (cost note SS8.2).
# Dev streaming runs on docker-compose Kafka for free. count = var toggle
# so `terraform apply -var msk_enabled=false` skips it entirely.

resource "aws_msk_serverless_cluster" "kafka" {
  count        = var.msk_enabled ? 1 : 0
  cluster_name = "fsl-kafka-${var.env}"

  vpc_config {
    subnet_ids         = var.subnet_ids
    security_group_ids = [aws_security_group.msk[0].id]
  }

  client_authentication {
    sasl {
      iam { enabled = true }
    }
  }
}

resource "aws_security_group" "msk" {
  count  = var.msk_enabled ? 1 : 0
  name   = "fsl-msk-${var.env}"
  vpc_id = var.vpc_id

  ingress {
    from_port   = 9098
    to_port     = 9098
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr] # broker access from inside the VPC only
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
