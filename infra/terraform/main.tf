# Stage 0: just the lake bucket + Glue database. Stage 3 adds EMR Serverless,
# MSK, IAM roles, Lake Formation grants, CloudWatch alarms -- as modules.

resource "aws_s3_bucket" "lake" {
  bucket        = var.lake_bucket_name
  force_destroy = true # solo-scale cost discipline: destroy must leave nothing billing
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_glue_catalog_database" "silver" { name = "silver" }
resource "aws_glue_catalog_database" "gold"   { name = "gold" }
