variable "aws_region" {
  description = "AWS region for deployment."
  type        = string
  default     = "ap-southeast-2"
}

variable "aws_profile" {
  description = "AWS shared config profile name used for Terraform authentication."
  type        = string
  default     = ""
}

variable "data_bucket" {
  description = "Primary S3 bucket name used by the pipeline."
  type        = string
}

variable "dataset_prefix" {
  description = "Dataset root prefix inside the S3 bucket."
  type        = string
  default     = "jodi-oil"
}

variable "jobs_prefix" {
  description = "Prefix where job scripts/config are uploaded."
  type        = string
  default     = "jodi-oil/jobs"
}

variable "logs_prefix" {
  description = "Prefix for EMR Serverless logs."
  type        = string
  default     = "jodi-oil/logs"
}

variable "athena_output_s3_uri" {
  description = "Athena output S3 URI (for example s3://bucket/athena-results/)."
  type        = string

  validation {
    condition     = startswith(var.athena_output_s3_uri, "s3://")
    error_message = "athena_output_s3_uri must start with s3://"
  }
}

variable "glue_database" {
  description = "Glue database name used by Silver/Gold tables."
  type        = string
  default     = "jodi_oil_db"
}

variable "athena_workgroup" {
  description = "Athena workgroup used for all orchestration queries."
  type        = string
  default     = "jodi-oil-wg"
}

variable "emr_application_id" {
  description = "EMR Serverless Spark application ID."
  type        = string
}

variable "emr_runtime_role_arn" {
  description = "IAM role ARN for EMR Serverless job runtime."
  type        = string
}

variable "step_functions_role_arn" {
  description = "IAM role ARN used by Step Functions state machine."
  type        = string
}

variable "state_machine_name" {
  description = "Step Functions state machine name."
  type        = string
  default     = "jodi-oil-pipeline"
}

variable "step_functions_log_group_name" {
  description = "CloudWatch log group name for Step Functions execution logs."
  type        = string
  default     = "/aws/vendedlogs/states/jodi-oil-pipeline"
}

variable "log_retention_days" {
  description = "Retention period for Step Functions log group."
  type        = number
  default     = 30
}

variable "spark_adaptive_enabled" {
  description = "Value for spark.sql.adaptive.enabled."
  type        = bool
  default     = true
}

variable "spark_shuffle_partitions" {
  description = "Value for spark.sql.shuffle.partitions."
  type        = number
  default     = 200
}

variable "spark_target_files_per_partition" {
  description = "Target files per partition used by Silver/Gold jobs."
  type        = number
  default     = 1
}

variable "silver_script_s3_uri" {
  description = "Optional explicit S3 URI to Week3 silver script."
  type        = string
  default     = ""
}

variable "gold_script_s3_uri" {
  description = "Optional explicit S3 URI to Week4 gold script."
  type        = string
  default     = ""
}

variable "data_quality_script_s3_uri" {
  description = "Optional explicit S3 URI to Week5 data quality script."
  type        = string
  default     = ""
}

variable "config_s3_uri" {
  description = "Optional explicit S3 URI to shared config.yaml used by all jobs."
  type        = string
  default     = ""
}

variable "tags" {
  description = "Optional tags applied to created resources."
  type        = map(string)
  default     = {}
}
