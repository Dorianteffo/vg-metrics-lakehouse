variable "raw_bucket_name" {
  type        = string
  default     = "vg-raw-data"
}


variable "lakehouse_bucket_name" {
  type        = string
  default     = "vg-lakehouse"
}

variable "lambda_ecr_repo" {
  type        = string
  default     = "ecr-repo-lambda-vg"
}

variable "eventbridge_rule" {
  type        = string
  default     = "rule-vg"
}

variable "lambda_iam_role_name" {
  type        = string
  default     = "s3-cloudwatch-ecr-lambdarole"
}


variable "glue_iam_role_name" {
  type        = string
  default     = "vg-glue-role"
}


variable "bronze_glue_database" {
  type        = string
  default     = "bronze"
}

variable "silver_glue_database" {
  type        = string
  default     = "silver"
}

variable "gold_glue_database" {
  type        = string
  default     = "gold"
}


variable "s3_location_bronze_glue_database" {
  type        = string
  default     = "s3://vg-lakehouse/lakehouse/bronze/"
}

variable "s3_location_silver_glue_database" {
  type        = string
  default     = "s3://vg-lakehouse/lakehouse/silver/"
}

variable "s3_location_gold_glue_database" {
  type        = string
  default     = "s3://vg-lakehouse/lakehouse/gold/"
}

variable "glue_script_bucket" {
  type        = string
  default     = "vg-lakehouse-glue"

}

variable "key_name" {
  type        = string
  default     = "app-key"
  description = "EC2 key name"
}


variable "airflow_instance_type" {
  type        = string
  default     = "t2.xlarge"
  description = "Airflow instance typ ec2"
}