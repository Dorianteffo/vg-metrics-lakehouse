variable "raw_bucket_name" {
  type        = string
  default     = "vg-sales-raw-data"
}


variable "lakehouse_bucket_name" {
  type        = string
  default     = "vg-sales-lakehouse"
}

variable "lambda_ecr_repo" {
  type        = string
  default     = "ecr-repo-lambda-vg-sales"
}

variable "eventbridge_rule" {
  type        = string
  default     = "rule-vg-sales"
}

variable "lambda_iam_role_name" {
  type        = string
  default     = "s3-cloudwatch-ecr-lambdarole"
}