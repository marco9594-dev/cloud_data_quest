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
module "get_bls_and_data_usa_data" {
  source      = "./modules/lambda"
  lambda_name = "get_bls_and_data_usa_data"
  handler     = "get_bls_and_data_usa_data.get_bls_and_data_usa_data"

  bucket_name = "lambda-source-pmcq"
  key    = "get_bls_and_data_usa_data.zip"

  name_prefix = "get_bls_and_data_usa_data"

  timeout     = 180
}


# Lambda 2: Report on Data
module "analyze_bls_and_data_usa_data" {
  source      = "./modules/lambda"
  lambda_name = "analyze_bls_and_data_usa_data"
  handler     = "analyze_bls_and_data_usa_data.analyze_bls_and_data_usa_data"

  bucket_name = "lambda-source-pmcq"
  key    = "analyze_bls_and_data_usa_data.zip"

  name_prefix = "analyze_bls_and_data_usa_data"

  timeout     = 180

  layers = [
    "arn:aws:lambda:us-east-2:336392948345:layer:AWSSDKPandas-Python311:26"
  ]
}

# CloudWatch schedule for Lambda 1
module "daily_schedule" {
  source              = "./modules/cloudwatch_schedule"
  lambda_arn          = module.get_bls_and_data_usa_data.lambda_arn
  name_prefix         = "get_bls_and_data_usa_data"
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

# Trigger analyze_bls_and_data_usa_data Lambda from SQS
resource "aws_lambda_event_source_mapping" "analyze_bls_sqs" {
  event_source_arn  = module.report_queue.queue_arn
  function_name     = module.analyze_bls_and_data_usa_data.lambda_arn
  batch_size        = 1          # number of messages per Lambda invocation
  enabled           = true
}
