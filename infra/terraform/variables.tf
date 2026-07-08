variable "aws_region" {
  type    = string
  default = "ap-south-1"
}

variable "env" {
  type    = string
  default = "dev"
}

variable "lake_bucket_name" {
  description = "S3 bucket for bronze/silver/gold"
  type        = string
}
