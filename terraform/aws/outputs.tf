output "redis_urls" {
  description = "Value for the gateway's REDIS_URLS (TLS, primary endpoints in shard order)."
  value       = join(",", [for k in sort(tolist(local.shards)) : "rediss://${aws_elasticache_replication_group.shard[k].primary_endpoint_address}:6379"])
}
