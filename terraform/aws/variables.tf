variable "name" {
  type    = string
  default = "ratelimit"
}

variable "shard_count" {
  type    = number
  default = 3
  validation {
    condition     = var.shard_count >= 1 && var.shard_count <= 16
    error_message = "shard_count must be between 1 and 16."
  }
}

variable "node_type" {
  type    = string
  default = "cache.t4g.small"
}

variable "vpc_id" {
  type = string
}

variable "private_subnet_ids" {
  type = list(string)
}

variable "gateway_security_group_id" {
  description = "Security group of the gateway pods/nodes (EKS security groups for pods or node SG)."
  type        = string
}

variable "tags" {
  type    = map(string)
  default = { project = "ratelimit-gateway" }
}
