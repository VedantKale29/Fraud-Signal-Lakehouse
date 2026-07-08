terraform {
  required_version = ">= 1.7"
  backend "s3" {
    bucket         = "CHANGE_ME-tfstate"      # create once, out-of-band
    key            = "fraud-signal-lakehouse/terraform.tfstate"
    region         = "ap-south-1"
    dynamodb_table = "CHANGE_ME-tf-locks"
    encrypt        = true
  }
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = { project = "fraud-signal-lakehouse", env = var.env, owner = "vedant" }
  }
}
