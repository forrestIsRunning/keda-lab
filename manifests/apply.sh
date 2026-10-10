#!/bin/sh
# Apply the Redis list demo onto the local kind context. Does not change the current kubectl context.
set -eu

context=kind-k8s-lab
docker_bin="${DOCKER_BIN:-/Applications/Docker.app/Contents/Resources/bin/docker}"
redis_name="${REDIS_CONTAINER:-keda-redis}"
dir=$(CDPATH= cd -- "$(dirname "$0")" && pwd)

redis_ip=$("$docker_bin" inspect -f '{{.NetworkSettings.Networks.kind.IPAddress}}' "$redis_name")
if [ -z "$redis_ip" ]; then
  echo "redis container $redis_name has no address on the kind network" >&2
  exit 1
fi

tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT
sed "s/__REDIS_IP__/${redis_ip}/g" "$dir/keda-redis-demo.yaml" >"$tmp"
echo "redis ${redis_name} on kind network: ${redis_ip}"
kubectl --context "$context" apply -f "$tmp"
