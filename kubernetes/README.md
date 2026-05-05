# Cassandra Easy Stress with Prometheus & Grafana Monitoring

This directory contains Kubernetes manifests for running cassandra-easy-stress with integrated Prometheus and Grafana monitoring in a single pod.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Kubernetes Pod                          │
│                                                                 │
│  ┌──────────────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │ cassandra-easy-stress│  │  Prometheus  │  │   Grafana    │   │
│  │                      │  │              │  │              │   │
│  │  Port 9500           │◄─┤  Port 9090   │◄─┤  Port 3000   │   │
│  │  /metrics endpoint   │  │  Scrapes     │  │  Dashboard   │   │
│  │                      │  │  every 5s    │  │  UI          │   │
│  └──────────────────────┘  └──────────────┘  └──────────────┘   │
│           │                        │                  │         │
│           │                        │                  │         │
│  ┌────────▼────────┐       ┌───────▼──────┐  ┌────────▼──────┐  │
│  │ Astra Bundle    │       │ Prometheus   │  │ Grafana       │  │
│  │ (Secret)        │       │ Config       │  │ Configs       │  │
│  │ Results (CSV)   │       │ Data (7d)    │  │ (ConfigMaps)  │  │
│  └─────────────────┘       └──────────────┘  └───────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ kubectl port-forward
                              ▼
                        ┌──────────┐
                        │   User   │
                        │ Browser  │
                        └──────────┘
```

## Features

- **Real-time Metrics**: Prometheus scrapes cassandra-easy-stress metrics every 5 seconds
- **7-Day Retention**: Metrics are stored for 7 days for historical analysis
- **Pre-configured Dashboard**: Grafana dashboard automatically loads with comprehensive visualizations
- **Self-contained**: All monitoring runs within a single pod, no cluster-wide dependencies
- **Anonymous Access**: Grafana configured for easy access without authentication
- **Persistent Monitoring**: Containers continue running after stress test completes

## Dashboard Panels

The Grafana dashboard includes:

1. **Select Latency (Percentiles)**: p50, p75, p95, p98, p99, p999
2. **Mutation Latency (Percentiles)**: p50, p75, p95, p98, p99, p999
3. **Total Errors**: Gauge showing cumulative error count
4. **Total Selects**: Gauge showing total select operations
5. **Total Mutations**: Gauge showing total mutation operations
6. **Total Deletions**: Gauge showing total deletion operations
7. **Throughput**: Stacked area chart showing operations per second for all operation types
8. **Deletion Latency (Percentiles)**: p50, p75, p95, p98, p99, p999
9. **Populate Mutation Latency (Percentiles)**: p50, p75, p95, p98, p99, p999
10. **Error Rate**: Time series showing errors per second

## Prerequisites

- Kubernetes cluster access
- `kubectl` configured to access your cluster
- Cassandra username and password
- **Either**:
  - Astra bundle zip file (secure-connect-*.zip) for Astra DB, **OR**
  - Cassandra cluster hostname/IP for direct connection

## Deployment Steps

### Quick Deployment

Use the automated deployment script with either Astra DB or direct Cassandra connection:

**For Astra DB:**
```bash
cd kubernetes
./deploy.sh \
  -u <username> \
  -p <password> \
  -b /path/to/secure-connect-bundle.zip
```

**For Direct Cassandra Connection:**
```bash
cd kubernetes
./deploy.sh \
  -u <username> \
  -p <password> \
  -H cassandra.example.com
```

The script will:
1. Create namespace if it doesn't exist
2. Create Astra bundle secret from your local file (if using Astra)
3. Deploy all ConfigMaps
4. Generate and deploy the Job with your configuration
5. Wait for pod creation and show next steps

### Custom Configuration

All stress test parameters are configurable via command-line arguments:

```bash
./deploy.sh \
  --name my-stress-test \
  --namespace my-namespace \
  --read-rate 0.5 \
  --rate 5000 \
  --threads 20 \
  --duration 1h \
  --concurrency 15 \
  --keyspace myks \
  --cl LOCAL_ONE \
  --username myuser \
  --password mypass \
  --bundle /path/to/bundle.zip
```

#### Available Options

- `-n, --name`: Job name (default: cassandra-stress)
- `-N, --namespace`: Kubernetes namespace (default: amc-benchmarks)
- `-w, --workload`: Workload name (default: BasicTimeSeries)
- `-r, --read-rate`: Read ratio 0-1 (default: 0.25 = 25% reads)
- `-C, --cl`: Consistency level (default: LOCAL_QUORUM)
- `-R, --rate`: Operations per second (default: 10000)
- `-t, --threads`: Number of threads (default: 10)
- `-d, --duration`: Duration like 3h, 30m, 1h30m (default: 3h)
- `-c, --concurrency`: Connections per thread (default: 10)
- `-k, --keyspace`: Target keyspace (default: testks)
- `-u, --username`: Cassandra username (required)
- `-p, --password`: Cassandra password (required)
- `-b, --bundle`: Path to Astra bundle zip (required for Astra)
- `-H, --host`: Cassandra host (required for direct connection)

**Note:** Either `-b` (bundle) or `-H` (host) must be provided, but not both.

#### Available Workloads

BasicTimeSeries, KeyValue, CountersWide, Maps, Sets, UdtTimeSeries, RandomPartitionAccess, MaterializedViews, LWT, SAI, AllowFiltering, RangeScan, CreateDrop, DSESearch, Locking, TxnCounter

For more information about each workload, run:
```bash
cassandra-easy-stress info <WorkloadName>
```

### Manual Deployment

If you prefer manual deployment:

#### 1. Create Namespace

```bash
kubectl create namespace amc-benchmarks
```

#### 2. Create Astra Bundle Secret

```bash
kubectl create secret generic my-astra-bundle \
  --from-file=/path/to/secure-connect-bundle.zip \
  -n amc-benchmarks
```

#### 3. Deploy ConfigMaps

```bash
kubectl apply -f configmaps/ -n amc-benchmarks
```

#### 4. Deploy the Job

Edit `stress-job-with-monitoring.yaml` with your parameters, then:

```bash
kubectl apply -f stress-job-with-monitoring.yaml
```

### Monitor Pod Startup

```bash
# Get pod name (use your job name)
POD_NAME=$(kubectl get pods -n amc-benchmarks -l job-name=cassandra-stress -o jsonpath='{.items[0].metadata.name}')

# Watch pod creation
kubectl get pod $POD_NAME -n amc-benchmarks -w

# View logs from all containers
kubectl logs -n amc-benchmarks $POD_NAME --all-containers=true -f
```

### Access Grafana Dashboard

Once the pod is running (all containers ready), set up port forwarding:

```bash
# Forward Grafana port
kubectl port-forward -n amc-benchmarks $POD_NAME 3000:3000
```

Then open your browser to: **http://localhost:3000**

The dashboard will load automatically. No login required (anonymous access enabled).

### Access Prometheus (Optional)

To access Prometheus directly:

```bash
# Forward Prometheus port (in a new terminal)
kubectl port-forward -n amc-benchmarks $POD_NAME 9090:9090
```

Then open: **http://localhost:9090**

### Access Metrics Endpoint (Optional)

To view raw metrics from cassandra-easy-stress:

```bash
# Forward metrics port (in a new terminal)
kubectl port-forward -n amc-benchmarks $POD_NAME 9500:9500
```

Then access: **http://localhost:9500/metrics**

## Monitoring the Stress Test

### Real-time Monitoring

1. Open Grafana dashboard at http://localhost:3000
2. The dashboard auto-refreshes every 5 seconds
3. Use the time range selector (top right) to adjust the view window
4. Default view shows last 15 minutes

### Key Metrics to Watch

- **Latency Trends**: Monitor p99 and p999 latencies for performance degradation
- **Throughput**: Ensure operations/second matches expected rate (10,000 ops/sec configured)
- **Error Rate**: Should remain at 0 for healthy operations
- **Operation Counts**: Track progress of the 3-hour stress test

### Stress Test Timeline

- **Duration**: 3 hours (configured with `-d 3h`)
- **Rate**: 10,000 operations per second
- **Read/Write Ratio**: 25% reads, 75% writes (configured with `-r 0.25`)
- **Threads**: 10 concurrent threads
- **Connections**: 10 connections per thread

After the stress test completes:
- The cassandra-easy-stress container enters sleep mode
- Metrics endpoint remains available at port 9500
- Prometheus continues scraping and storing metrics
- Grafana dashboard remains accessible for analysis
- All data is retained for 7 days

## Viewing Results

### CSV Results

The stress test generates a CSV file with detailed results:

```bash
# Copy CSV results from the pod
kubectl cp -n amc-benchmarks $POD_NAME:/tmp/stress/stress_results.csv ./stress_results.csv -c cassandra-easy-stress
```

### Stress Logs

View detailed logs from the stress test:

```bash
# View cassandra-easy-stress logs
kubectl logs -n amc-benchmarks $POD_NAME -c cassandra-easy-stress

# Copy log files
kubectl cp -n amc-benchmarks $POD_NAME:/home/stress/.cassandra-easy-stress ./stress-logs -c cassandra-easy-stress
```

## Troubleshooting

### Pod Not Starting

Check pod events:
```bash
kubectl describe pod -n amc-benchmarks $POD_NAME
```

Common issues:
- Missing ConfigMaps: Ensure all 4 ConfigMaps are created
- Missing Secret: Verify `astra-bundle-n1` secret exists
- Resource constraints: Check cluster has sufficient CPU/memory

### Grafana Dashboard Not Loading

1. Check Grafana container logs:
```bash
kubectl logs -n amc-benchmarks $POD_NAME -c grafana
```

2. Verify ConfigMaps are mounted:
```bash
kubectl exec -n amc-benchmarks $POD_NAME -c grafana -- ls -la /etc/grafana/provisioning/datasources
kubectl exec -n amc-benchmarks $POD_NAME -c grafana -- ls -la /etc/grafana/provisioning/dashboards
```

3. Check Grafana health:
```bash
kubectl exec -n amc-benchmarks $POD_NAME -c grafana -- wget -O- http://localhost:3000/api/health
```

### Prometheus Not Scraping

1. Check Prometheus targets:
```bash
# Port-forward Prometheus
kubectl port-forward -n amc-benchmarks $POD_NAME 9090:9090

# Open http://localhost:9090/targets in browser
```

2. Verify metrics endpoint is accessible:
```bash
kubectl exec -n amc-benchmarks $POD_NAME -c prometheus -- wget -O- http://localhost:9500/metrics
```

3. Check Prometheus logs:
```bash
kubectl logs -n amc-benchmarks $POD_NAME -c prometheus
```

### No Metrics in Dashboard

1. Verify cassandra-easy-stress is running:
```bash
kubectl logs -n amc-benchmarks $POD_NAME -c cassandra-easy-stress --tail=50
```

2. Check if metrics endpoint is responding:
```bash
kubectl port-forward -n amc-benchmarks $POD_NAME 9500:9500
curl http://localhost:9500/metrics
```

3. Verify Prometheus is scraping:
```bash
# Check Prometheus logs for scrape errors
kubectl logs -n amc-benchmarks $POD_NAME -c prometheus | grep -i error
```

## Cleanup

### Using the Cleanup Script

```bash
# Basic cleanup (keeps ConfigMaps for reuse)
./cleanup.sh -n cassandra-stress -N amc-benchmarks

# Full cleanup including ConfigMaps
./cleanup.sh -n cassandra-stress -N amc-benchmarks --delete-configmaps
```

The script will:
- Delete the Job and wait for pod termination
- Delete the Astra bundle secret
- Optionally delete ConfigMaps

### Manual Cleanup

If you prefer manual cleanup:

```bash
# Delete the Job
kubectl delete job cassandra-stress -n amc-benchmarks

# Delete the secret
kubectl delete secret cassandra-stress-astra-bundle -n amc-benchmarks

# Delete ConfigMaps (optional)
kubectl delete configmap prometheus-config -n amc-benchmarks
kubectl delete configmap grafana-datasource -n amc-benchmarks
kubectl delete configmap grafana-dashboard-provider -n amc-benchmarks
kubectl delete configmap grafana-dashboard -n amc-benchmarks
```

## Customization

### Modify Stress Test Parameters

Use command-line arguments when deploying:

```bash
./deploy.sh \
  -n my-test \
  -w KeyValue \     # Use KeyValue workload
  -r 0.5 \          # 50% reads, 50% writes
  -R 20000 \        # 20k ops/sec
  -t 30 \           # 30 threads
  -c 15 \           # 15 connections per thread
  -d 2h \           # 2 hour duration
  -C LOCAL_ONE \    # Consistency level
  -k myks \         # Keyspace name
  -u user -p pass -b /path/to/bundle.zip
```

All available parameters are documented in the deploy.sh help:
```bash
./deploy.sh --help
```

### Modify Prometheus Retention

Edit `configmaps/prometheus-config.yaml` or update the Prometheus container args in the Job YAML:

```yaml
args:
  - '--storage.tsdb.retention.time=14d'  # Change from 7d to 14d
```

### Modify Scrape Interval

Edit `configmaps/prometheus-config.yaml`:

```yaml
global:
  scrape_interval: 10s  # Change from 5s to 10s
```

### Customize Dashboard

1. Access Grafana dashboard
2. Make changes to panels, queries, or layout
3. Click "Save dashboard" (top right)
4. Export dashboard JSON (Share > Export > Save to file)
5. Update `configmaps/grafana-dashboard.yaml` with new JSON
6. Reapply ConfigMap: `kubectl apply -f configmaps/grafana-dashboard.yaml`
7. Restart Grafana container or delete/recreate the job

## Resource Requirements

### Minimum Resources

- **cassandra-easy-stress**: 2 GB RAM, 1 CPU
- **Prometheus**: 512 MB RAM, 250m CPU
- **Grafana**: 256 MB RAM, 100m CPU
- **Total**: ~3 GB RAM, ~1.5 CPU

### Recommended Resources

For production workloads, increase limits in `stress-job-with-monitoring.yaml`:

```yaml
resources:
  requests:
    memory: "4Gi"
    cpu: "2000m"
  limits:
    memory: "8Gi"
    cpu: "4000m"
```

## Advanced Usage

### Multiple Concurrent Jobs

To run multiple stress tests simultaneously, change the job name:

```bash
# Copy the job YAML
cp stress-job-with-monitoring.yaml stress-job-2.yaml

# Edit the name in stress-job-2.yaml
# metadata.name: n1-stress-sni-2

# Deploy
kubectl apply -f stress-job-2.yaml
```

Each job will have its own isolated monitoring stack.

### Export Metrics for Long-term Storage

To preserve metrics beyond the 7-day retention:

```bash
# Port-forward Prometheus
kubectl port-forward -n amc-benchmarks $POD_NAME 9090:9090

# Use Prometheus API to export data
# Example: Export all metrics for the last hour
curl 'http://localhost:9090/api/v1/query_range?query=selects&start=2024-01-01T00:00:00Z&end=2024-01-01T01:00:00Z&step=5s' > selects_export.json
```

### Integration with External Monitoring

To integrate with cluster-wide Prometheus:

1. Add a ServiceMonitor or PodMonitor pointing to the stress test pod
2. Configure appropriate labels and selectors
3. External Prometheus will scrape the metrics endpoint at port 9500

## Support

For issues or questions:
- cassandra-easy-stress: https://github.com/apache/cassandra-easy-stress
- Prometheus: https://prometheus.io/docs/
- Grafana: https://grafana.com/docs/

## License

This configuration is part of the cassandra-easy-stress project and follows the same Apache 2.0 license.