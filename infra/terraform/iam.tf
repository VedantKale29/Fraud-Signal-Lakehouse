# Least-privilege job role: EMR jobs may touch ONLY the lake bucket and
# the Glue catalog. No wildcards on data buckets -- the tfsec gate (SS6.2)
# fails the build otherwise.

data "aws_iam_policy_document" "emr_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "emr_job" {
  name               = "fsl-emr-job-${var.env}"
  assume_role_policy = data.aws_iam_policy_document.emr_trust.json
}

data "aws_iam_policy_document" "emr_job" {
  statement {
    sid       = "LakeBucketRW"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*"]
  }
  statement {
    sid = "GlueCatalog"
    actions = [
      "glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables",
      "glue:GetPartition", "glue:GetPartitions", "glue:CreateTable",
      "glue:UpdateTable", "glue:CreatePartition", "glue:BatchCreatePartition"
    ]
    resources = ["*"] # catalog ARNs are account-scoped; actions are read/DDL only
  }
  statement {
    sid       = "Metrics"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["FraudSignalLakehouse"]
    }
  }
}

resource "aws_iam_role_policy" "emr_job" {
  name   = "fsl-emr-job-policy"
  role   = aws_iam_role.emr_job.id
  policy = data.aws_iam_policy_document.emr_job.json
}

# Analyst role for the Lake Formation governance demo (SS6.2 gate)
resource "aws_iam_role" "analyst" {
  name = "fsl-analyst-${var.env}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { AWS = "arn:aws:iam::${data.aws_caller_identity.me.account_id}:root" }
    }]
  })
}

data "aws_caller_identity" "me" {}
