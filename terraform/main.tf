terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "us-east-2"
}

# S3 bucket
module "data_bucket" {
  source      = "./modules/s3_bucket"
  bucket_name = "cloud-data-quest-926"
}

# SQS queue
module "report_queue" {
  source     = "./modules/sqs_queue"
  queue_name = "my-report-queue"
}

# Load data to S3
module "load_data_to_s3" {
  source      = "./modules/lambda"
  lambda_name = "load_data_to_s3"
  handler     = "lambda_handler.main"

  bucket_name = "lambda-source-pmcq"
  key    = "load_to_s3_lambda.zip"

  name_prefix = "load_data_to_s3"
}


# Lambda 2: Report
module "report_lambda" {
  source      = "./modules/lambda"
  lambda_name = "report-lambda"
  handler     = "lambda_handler.main"

  bucket_name = "lambda-source-pmcq"
  key    = "write_report.zip"

  name_prefix = "report"
}

# CloudWatch schedule for Lambda 1
module "daily_schedule" {
  source              = "./modules/cloudwatch_schedule"
  lambda_arn          = module.load_data_to_s3.lambda_arn
  name_prefix         = "load_data_to_s3"
  schedule_expression = "rate(1 day)"
}

# S3 Notification to SQS
resource "aws_s3_bucket_notification" "s3_to_sqs" {
  bucket = module.data_bucket.bucket_id

  queue {
    queue_arn     = module.report_queue.queue_arn
    events        = ["s3:ObjectCreated:*"]
    filter_suffix = ".json"
  }
}

# SQS policy to allow S3 -> SQS
resource "aws_sqs_queue_policy" "allow_s3" {
  queue_url = module.report_queue.queue_url

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "s3.amazonaws.com" }
        Action    = "sqs:SendMessage"
        Resource  = module.report_queue.queue_arn
        Condition = {
          ArnEquals = {
            "aws:SourceArn" = module.data_bucket.bucket_arn
          }
        }
      }
    ]
  })
}
