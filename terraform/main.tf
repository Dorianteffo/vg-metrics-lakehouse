terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }

  }

  backend "s3" {
        bucket         	   = "iac-dorian-tf-state"
        key                = "state-vg-sales-tf/terraform.tfstate"
        region         	   = "eu-west-3"
        encrypt        	   = true
    }
}

# Configure the AWS Provider
provider "aws" {
  region = "eu-west-3"
}


resource "aws_s3_bucket" "vg-sales-raw-bucket" {
  bucket = var.raw_bucket_name
}

resource "aws_s3_bucket" "vg-sales-lakehouse-bucket" {
  bucket = var.lakehouse_bucket_name
}

resource "aws_ecr_repository" "ecr-repo" {
  name = var.lambda_ecr_repo
}

resource "aws_cloudwatch_event_rule" "event-rule" {
  name        = var.eventbridge_rule
  schedule_expression = "rate(6 hours)"

}

resource "aws_iam_role" "lambda_iam_role" {
  name = var.lambda_iam_role_name
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      },
    ]
  })

  managed_policy_arns = ["arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryFullAccess", "arn:aws:iam::aws:policy/AmazonS3FullAccess","arn:aws:iam::aws:policy/CloudWatchEventsFullAccess","arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"]

}