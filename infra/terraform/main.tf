terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile != "" ? var.aws_profile : null
}

data "aws_caller_identity" "current" {}

locals {
  dataset_prefix_clean = trim(var.dataset_prefix, "/")
  jobs_prefix_clean    = trim(var.jobs_prefix, "/")
  logs_prefix_clean    = trim(var.logs_prefix, "/")
  step_functions_role_name = reverse(split("/", var.step_functions_role_arn))[0]
  athena_output_bucket      = split("/", trimprefix(local.athena_output_s3_uri, "s3://"))[0]

  raw_uri     = "s3a://${var.data_bucket}/${local.dataset_prefix_clean}/raw/"
  silver_uri  = "s3a://${var.data_bucket}/${local.dataset_prefix_clean}/silver/"
  gold_uri    = "s3a://${var.data_bucket}/${local.dataset_prefix_clean}/gold/"
  reports_uri = "s3a://${var.data_bucket}/${local.dataset_prefix_clean}/reports/"

  silver_script_s3_uri = trimspace(var.silver_script_s3_uri) != "" ? var.silver_script_s3_uri : "s3://${var.data_bucket}/${local.jobs_prefix_clean}/week3/spark_to_silver.py"
  gold_script_s3_uri = trimspace(var.gold_script_s3_uri) != "" ? var.gold_script_s3_uri : "s3://${var.data_bucket}/${local.jobs_prefix_clean}/week4/spark_to_gold.py"
  data_quality_script_s3_uri = trimspace(var.data_quality_script_s3_uri) != "" ? var.data_quality_script_s3_uri : "s3://${var.data_bucket}/${local.jobs_prefix_clean}/week5/data_quality_spark.py"
  config_s3_uri = trimspace(var.config_s3_uri) != "" ? var.config_s3_uri : "s3://${var.data_bucket}/${local.jobs_prefix_clean}/shared/config.yaml"

  emr_logs_s3_uri      = "s3://${var.data_bucket}/${local.logs_prefix_clean}/"
  athena_output_s3_uri = endswith(var.athena_output_s3_uri, "/") ? var.athena_output_s3_uri : "${var.athena_output_s3_uri}/"

  silver_table_s3_location = "s3://${var.data_bucket}/${local.dataset_prefix_clean}/silver/"
  gold_monthly_s3_location = "s3://${var.data_bucket}/${local.dataset_prefix_clean}/gold/gold_monthly_global_production/"
  gold_country_s3_location = "s3://${var.data_bucket}/${local.dataset_prefix_clean}/gold/gold_country_production_trend/"
  gold_top_s3_location     = "s3://${var.data_bucket}/${local.dataset_prefix_clean}/gold/gold_top_producers_by_month/"
  gold_trade_s3_location   = "s3://${var.data_bucket}/${local.dataset_prefix_clean}/gold/gold_trade_balance_by_country/"

  silver_spark_submit_parameters = join(" ", [
    "--files ${local.config_s3_uri}",
    "--conf spark.sql.adaptive.enabled=${var.spark_adaptive_enabled}",
    "--conf spark.sql.shuffle.partitions=${var.spark_shuffle_partitions}",
    "--conf spark.emr-serverless.driverEnv.RAW_URI=${local.raw_uri}",
    "--conf spark.emr-serverless.driverEnv.SILVER_URI=${local.silver_uri}",
    "--conf spark.emr-serverless.driverEnv.SPARK_ADAPTIVE_ENABLED=${var.spark_adaptive_enabled}",
    "--conf spark.emr-serverless.driverEnv.SPARK_SHUFFLE_PARTITIONS=${var.spark_shuffle_partitions}",
    "--conf spark.emr-serverless.driverEnv.SPARK_TARGET_FILES_PER_PARTITION=${var.spark_target_files_per_partition}"
  ])

  gold_spark_submit_parameters = join(" ", [
    "--files ${local.config_s3_uri}",
    "--conf spark.sql.adaptive.enabled=${var.spark_adaptive_enabled}",
    "--conf spark.sql.shuffle.partitions=${var.spark_shuffle_partitions}",
    "--conf spark.emr-serverless.driverEnv.SILVER_URI=${local.silver_uri}",
    "--conf spark.emr-serverless.driverEnv.GOLD_URI=${local.gold_uri}",
    "--conf spark.emr-serverless.driverEnv.SPARK_ADAPTIVE_ENABLED=${var.spark_adaptive_enabled}",
    "--conf spark.emr-serverless.driverEnv.SPARK_SHUFFLE_PARTITIONS=${var.spark_shuffle_partitions}",
    "--conf spark.emr-serverless.driverEnv.SPARK_TARGET_FILES_PER_PARTITION=${var.spark_target_files_per_partition}"
  ])

  dq_spark_submit_parameters = join(" ", [
    "--files ${local.config_s3_uri}",
    "--conf spark.sql.adaptive.enabled=${var.spark_adaptive_enabled}",
    "--conf spark.sql.shuffle.partitions=${var.spark_shuffle_partitions}",
    "--conf spark.emr-serverless.driverEnv.RAW_URI=${local.raw_uri}",
    "--conf spark.emr-serverless.driverEnv.SILVER_URI=${local.silver_uri}",
    "--conf spark.emr-serverless.driverEnv.GOLD_URI=${local.gold_uri}",
    "--conf spark.emr-serverless.driverEnv.REPORTS_URI=${local.reports_uri}",
    "--conf spark.emr-serverless.driverEnv.SPARK_ADAPTIVE_ENABLED=${var.spark_adaptive_enabled}",
    "--conf spark.emr-serverless.driverEnv.SPARK_SHUFFLE_PARTITIONS=${var.spark_shuffle_partitions}"
  ])
}

resource "aws_cloudwatch_log_group" "step_functions" {
  name              = var.step_functions_log_group_name
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

resource "aws_iam_role_policy" "step_functions_support" {
  name = "${var.state_machine_name}-support"
  role = local.step_functions_role_name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowStepFunctionsManagedRuleForSyncJobs"
        Effect = "Allow"
        Action = [
          "events:PutRule",
          "events:PutTargets",
          "events:DescribeRule",
          "events:DeleteRule",
          "events:RemoveTargets"
        ]
        Resource = "arn:aws:events:${var.aws_region}:${data.aws_caller_identity.current.account_id}:rule/StepFunctionsGetEventsFor*"
      },
      {
        Sid    = "AllowStepFunctionsLogDelivery"
        Effect = "Allow"
        Action = [
          "logs:CreateLogDelivery",
          "logs:GetLogDelivery",
          "logs:UpdateLogDelivery",
          "logs:DeleteLogDelivery",
          "logs:ListLogDeliveries",
          "logs:PutResourcePolicy",
          "logs:DescribeResourcePolicies",
          "logs:DescribeLogGroups"
        ]
        Resource = "*"
      },
      {
        Sid    = "AllowStepFunctionsXRay"
        Effect = "Allow"
        Action = [
          "xray:PutTraceSegments",
          "xray:PutTelemetryRecords",
          "xray:GetSamplingRules",
          "xray:GetSamplingTargets",
          "xray:GetSamplingStatisticSummaries"
        ]
        Resource = "*"
      },
      {
        Sid    = "AllowAthenaGlueCatalogWrites"
        Effect = "Allow"
        Action = [
          "glue:CreateDatabase",
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:BatchGetPartition",
          "glue:CreatePartition",
          "glue:BatchCreatePartition"
        ]
        Resource = "*"
      },
      {
        Sid    = "AllowAthenaOutputBucketAccess"
        Effect = "Allow"
        Action = [
          "s3:GetBucketLocation",
          "s3:ListBucket",
          "s3:ListBucketMultipartUploads"
        ]
        Resource = "arn:aws:s3:::${local.athena_output_bucket}"
      },
      {
        Sid    = "AllowAthenaOutputObjectWrites"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts"
        ]
        Resource = "arn:aws:s3:::${local.athena_output_bucket}/*"
      }
    ]
  })
}

resource "aws_sfn_state_machine" "jodi_oil_pipeline" {
  name     = var.state_machine_name
  role_arn = var.step_functions_role_arn
  depends_on = [
    aws_cloudwatch_log_group.step_functions,
    aws_iam_role_policy.step_functions_support
  ]

  definition = templatefile("${path.module}/../statemachine.asl.json", {
    emr_application_id            = var.emr_application_id
    emr_runtime_role_arn          = var.emr_runtime_role_arn
    silver_script_s3_uri          = local.silver_script_s3_uri
    gold_script_s3_uri            = local.gold_script_s3_uri
    data_quality_script_s3_uri    = local.data_quality_script_s3_uri
    silver_spark_submit_parameters = local.silver_spark_submit_parameters
    gold_spark_submit_parameters   = local.gold_spark_submit_parameters
    dq_spark_submit_parameters     = local.dq_spark_submit_parameters
    emr_logs_s3_uri               = local.emr_logs_s3_uri
    glue_database                 = var.glue_database
    athena_workgroup              = var.athena_workgroup
    athena_output_s3_uri          = local.athena_output_s3_uri
    silver_table_s3_location      = local.silver_table_s3_location
    gold_monthly_s3_location      = local.gold_monthly_s3_location
    gold_country_s3_location      = local.gold_country_s3_location
    gold_top_s3_location          = local.gold_top_s3_location
    gold_trade_s3_location        = local.gold_trade_s3_location
  })

  logging_configuration {
    include_execution_data = true
    level                  = "ALL"
    log_destination        = "${aws_cloudwatch_log_group.step_functions.arn}:*"
  }

  tracing_configuration {
    enabled = true
  }

  tags = var.tags
}
