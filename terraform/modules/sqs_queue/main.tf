resource "aws_sqs_queue" "this" {
  name = var.queue_name
}

resource "aws_sqs_queue_policy" "allow_s3" {
  queue_url = module.report_queue.queue_url

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = {
          Service = "s3.amazonaws.com"
        }
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


output "queue_arn" {
  value = aws_sqs_queue.this.arn
}

output "queue_name" {
  value = aws_sqs_queue.this.name
}

output "queue_url" {
  value = aws_sqs_queue.this.id  # or .url depending on AWS provider version
}
