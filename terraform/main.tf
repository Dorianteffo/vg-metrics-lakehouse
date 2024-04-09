terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }

  }

  backend "s3" {
        bucket         	   = "iac-dorian-tf-state"
        key                = "state-vg-tf/terraform.tfstate"
        region         	   = "eu-west-3"
        encrypt        	   = true
    }
}

# Configure the AWS Provider
provider "aws" {
  region = "eu-west-3"
}


# S3 bukcet raw data
resource "aws_s3_bucket" "vg-raw-bucket" {
  bucket = var.raw_bucket_name
}


# S3 bucket lakehouse 
resource "aws_s3_bucket" "vg-lakehouse-bucket" {
  bucket = var.lakehouse_bucket_name
}

resource "aws_s3_object" "bronze_folder" {
  bucket = aws_s3_bucket.vg-lakehouse-bucket.id
  key    = "lakehouse/bronze/"
}

resource "aws_s3_object" "silver_folder" {
  bucket = aws_s3_bucket.vg-lakehouse-bucket.id
  key    = "lakehouse/silver/"
}

resource "aws_s3_object" "gold_folder" {
  bucket = aws_s3_bucket.vg-lakehouse-bucket.id
  key    = "lakehouse/gold/"
}

resource "aws_s3_object" "delta_jar_core" {
  bucket = aws_s3_bucket.vg-lakehouse-bucket.id
  key    = "delta_jar/delta-core_2.12-2.1.0.jar"
  source = "../delta_jar/delta-core_2.12-2.1.0.jar"
}

resource "aws_s3_object" "delta_jar_storage" {
  bucket = aws_s3_bucket.vg-lakehouse-bucket.id
  key    = "delta_jar/delta-storage-2.1.0.jar"
  source = "../delta_jar/delta-storage-2.1.0.jar"
}


# ECR repo 
resource "aws_ecr_repository" "ecr-repo" {
  name = var.lambda_ecr_repo
}


# Eventbridge rule to trigger lambda
resource "aws_cloudwatch_event_rule" "event-rule" {
  name        = var.eventbridge_rule
  schedule_expression = "rate(2 hours)"

}

#Lambda IAM role
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


# Glue databases for the lakehouse
resource "aws_glue_catalog_database" "bronze_database" {
  name = var.bronze_glue_database
  location_uri = var.s3_location_bronze_glue_database
}

resource "aws_glue_catalog_database" "silver_database" {
  name = var.silver_glue_database
  location_uri = var.s3_location_silver_glue_database
}

resource "aws_glue_catalog_database" "gold_database" {
  name = var.gold_glue_database
  location_uri = var.s3_location_gold_glue_database
}


# Glue IAM role 
resource "aws_iam_role" "glue_iam_role" {
  name = var.glue_iam_role_name
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      },
    ]
  })

  managed_policy_arns = ["arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"]
}
