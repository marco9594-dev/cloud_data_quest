variable "lambda_name" {}
variable "handler" {}
variable "name_prefix" {}
variable "bucket_name" {}
variable "key" {}
variable "timeout" {
  description = "Lambda timeout in seconds"
  type        = number
  default     = 60   # default 60 seconds if not overridden
}