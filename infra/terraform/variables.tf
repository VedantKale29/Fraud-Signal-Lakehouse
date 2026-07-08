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

variable "alert_email" {
  description = "Where CloudWatch + budget alerts land"
  type        = string
}

variable "monthly_budget_usd" {
  type    = string
  default = "25"
}

variable "msk_enabled" {
  description = "Provision MSK Serverless (demo only -- dev uses docker Kafka)"
  type        = bool
  default     = false
}

variable "vpc_id" {
  type    = string
  default = ""
}

variable "vpc_cidr" {
  type    = string
  default = "10.0.0.0/16"
}

variable "subnet_ids" {
  type    = list(string)
  default = []
}
