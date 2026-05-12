# Quick Start Guide

Get up and running with cassandra-easy-stress monitoring in 5 minutes!

## Prerequisites

- Kubernetes cluster access
- `kubectl` configured
- Cassandra username and password
- **Either**: Astra bundle zip file (secure-connect-*.zip) **OR** Cassandra hostname/IP

## Deploy Everything

**For Astra DB:**
```bash
cd kubernetes
./stress.sh \
  -u <username> \
  -p <password> \
  -b /path/to/secure-connect-bundle.zip
```

If you have an astra token json file, you can connect using it:
```bash
cd kubernetes
./stress.sh \
  --astra <database id> \
  --astra-token-file /path/to/astra-token.json \
```

**For Direct Cassandra:**
```bash
cd kubernetes
./stress.sh \
  -u <username> \
  -p <password> \
  -H cassandra.example.com
```

That's it! The script will:
1. Create namespace if needed
2. Create Astra bundle secret
3. Create all ConfigMaps
4. Deploy the Job
5. Show you the next steps

### Custom Configuration

```bash
./stress.sh \
  -n my-stress-test \
  -N my-namespace \
  -w KeyValue \
  -r 0.5 \
  -R 5000 \
  -t 20 \
  -d 1h \
  -u <username> \
  -p <password> \
  -b /path/to/bundle.zip
```

### Available Workloads

BasicTimeSeries (default), KeyValue, CountersWide, Maps, Sets, UdtTimeSeries, RandomPartitionAccess, MaterializedViews, LWT, SAI, AllowFiltering, RangeScan, CreateDrop, DSESearch, Locking, TxnCounter

## Access Grafana Dashboard

Once the pod is running:

```bash
# Get pod name
POD_NAME=$(kubectl get pods -n amc-benchmarks -l job-name=n1-stress-sni -o jsonpath='{.items[0].metadata.name}')

# Port forward
kubectl port-forward -n amc-benchmarks $POD_NAME 3000:3000
```

Open browser: **http://localhost:3000**

## Monitor Progress

```bash
# Watch pod status
kubectl get pod -n amc-benchmarks -l job-name=n1-stress-sni -w

# View stress test logs
kubectl logs -n amc-benchmarks $POD_NAME -c cassandra-easy-stress -f

# View all container logs
kubectl logs -n amc-benchmarks $POD_NAME --all-containers=true -f
```

## Cleanup

```bash
# Basic cleanup (keeps ConfigMaps for reuse)
./cleanup.sh -n my-stress-test -N my-namespace

# Full cleanup including ConfigMaps
./cleanup.sh -n my-stress-test -N my-namespace --delete-configmaps
```

## Troubleshooting

### Pod not starting?

```bash
kubectl describe pod $POD_NAME -n amc-benchmarks
```

### Grafana not loading?

```bash
# Check Grafana logs
kubectl logs -n amc-benchmarks $POD_NAME -c grafana

# Verify ConfigMaps
kubectl get configmaps -n amc-benchmarks | grep grafana
```

### No metrics in dashboard?

```bash
# Check if metrics endpoint is working
kubectl port-forward -n amc-benchmarks $POD_NAME 9500:9500
curl http://localhost:9500/metrics

# Check Prometheus targets
kubectl port-forward -n amc-benchmarks $POD_NAME 9090:9090
# Open http://localhost:9090/targets
```

## What's Running?

The pod contains 3 containers:

1. **cassandra-easy-stress** (port 9500)
   - Runs the stress test for 3 hours
   - Exposes metrics at `/metrics`
   - Stays alive after test completes

2. **prometheus** (port 9090)
   - Scrapes metrics every 5 seconds
   - Stores data for 7 days
   - Provides query interface

3. **grafana** (port 3000)
   - Pre-configured dashboard
   - Anonymous access enabled
   - Auto-connects to Prometheus

## Key Metrics

- **Latency**: p50, p95, p99, p999 for all operations
- **Throughput**: Operations per second
- **Errors**: Total error count and rate
- **Operations**: Total counts for selects, mutations, deletions

## Need More Help?

See the full [README.md](README.md) for detailed documentation.