resource "aws_sqs_queue" "this" {
  name = var.queue_name
}

output "queue_arn" {
  value = aws_sqs_queue.this.arn
}

output "queue_url" {
  value = aws_sqs_queue.this.id
}

output "queue_name" {
  value = aws_sqs_queue.this.name
}
