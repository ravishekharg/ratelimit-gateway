# Redis shards for the gateway on ElastiCache: one replication group per shard (primary + replica in another AZ,
# automatic failover), encrypted in transit and at rest, reachable only from the gateway's security group.
# The gateway shards keys itself with the consistent-hash ring, so each group runs with cluster mode disabled.

locals {
  shards = toset([for i in range(var.shard_count) : "shard-${i}"])
}

resource "aws_elasticache_subnet_group" "this" {
  name       = "${var.name}-redis"
  subnet_ids = var.private_subnet_ids
}

resource "aws_security_group" "redis" {
  name_prefix = "${var.name}-redis-"
  description = "Redis shards for ${var.name}"
  vpc_id      = var.vpc_id

  ingress {
    description     = "Redis from the gateway only"
    from_port       = 6379
    to_port         = 6379
    protocol        = "tcp"
    security_groups = [var.gateway_security_group_id]
  }

  tags = var.tags
}

resource "aws_elasticache_replication_group" "shard" {
  for_each = local.shards

  replication_group_id       = "${var.name}-${each.key}"
  description                = "${var.name} rate-limit ${each.key}"
  engine                     = "redis"
  engine_version             = "7.1"
  node_type                  = var.node_type
  num_cache_clusters         = 2
  automatic_failover_enabled = true
  multi_az_enabled           = true
  port                       = 6379
  subnet_group_name          = aws_elasticache_subnet_group.this.name
  security_group_ids         = [aws_security_group.redis.id]
  at_rest_encryption_enabled = true
  transit_encryption_enabled = true
  parameter_group_name       = "default.redis7"
  snapshot_retention_limit   = 0 # counters are ephemeral: no backups needed
  apply_immediately          = false
  tags                       = var.tags
}
