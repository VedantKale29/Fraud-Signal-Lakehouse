# Stage 0/3: the lake bucket + Glue databases, hardened to pass security
# scanning (checkov). Real controls get real fixes; checks that don't fit
# solo scale get explicit, justified skips -- never silent ones.

resource "aws_s3_bucket" "lake" {
  #checkov:skip=CKV_AWS_18:Access logging needs a second log bucket; deferred to multi-user scale. Data-plane audit comes from CloudTrail at demo time.
  #checkov:skip=CKV_AWS_144:Cross-region replication is cost-prohibitive at solo scale and not required for a portfolio demo.
  #checkov:skip=CKV2_AWS_62:No event-driven consumers; Airflow pulls on schedule.
  bucket        = var.lake_bucket_name
  force_destroy = true # cost discipline: destroy must leave nothing billing
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = "alias/aws/s3" # AWS-managed key: encryption without key-management overhead
    }
    bucket_key_enabled = true # caps KMS request costs on high-volume Spark writes
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    id     = "housekeeping"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 3
    }
    noncurrent_version_expiration {
      noncurrent_days = 14 # versioning is for oops-recovery, not archival
    }
  }
}

resource "aws_glue_catalog_database" "silver" {
  name = "silver"
}

resource "aws_glue_catalog_database" "gold" {
  name = "gold"
}
