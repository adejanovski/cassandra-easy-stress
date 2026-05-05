#!/bin/bash
# Deployment script for cassandra-easy-stress with Prometheus and Grafana monitoring
# This script deploys all necessary ConfigMaps and the Job to Kubernetes

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Default values
JOB_NAME="cassandra-easy-stress"
NAMESPACE="benchmarks"
WORKLOAD="BasicTimeSeries"
READ_RATE="0.25"
CONSISTENCY_LEVEL="LOCAL_QUORUM"
OPS_RATE="10000"
THREADS="10"
DURATION="3h"
CONCURRENCY="10"
KEYSPACE="testks"
USERNAME=""
PASSWORD=""
ASTRA_BUNDLE=""
HOST=""

# Function to display usage
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Deploy cassandra-easy-stress with Prometheus and Grafana monitoring to Kubernetes.

OPTIONS:
    -n, --name NAME              Job name (default: cassandra-easy-stress)
    -N, --namespace NAMESPACE    Kubernetes namespace (default: amc-benchmarks)
    -w, --workload WORKLOAD      Workload name (default: BasicTimeSeries)
    -r, --read-rate RATE         Read rate ratio 0-1 (default: 0.25)
    -C, --cl LEVEL               Consistency level (default: LOCAL_QUORUM)
    -R, --rate OPS               Operations per second (default: 10000)
    -t, --threads NUM            Number of threads (default: 10)
    -d, --duration TIME          Duration (e.g., 3h, 30m, 1h30m) (default: 3h)
    -c, --concurrency NUM        Connections per thread (default: 10)
    -k, --keyspace NAME          Target keyspace (default: testks)
    -u, --username USER          Cassandra username (required)
    -p, --password PASS          Cassandra password (required)
    -b, --bundle PATH            Path to Astra bundle zip file (optional, use with Astra)
    -H, --host HOST              Cassandra host (optional, use instead of bundle)
    -h, --help                   Display this help message

NOTE:
    Either --bundle or --host must be provided (not both).
    Use --bundle for Astra DB connections.
    Use --host for direct Cassandra cluster connections.

AVAILABLE WORKLOADS:
    BasicTimeSeries, KeyValue, CountersWide, Maps, Sets, UdtTimeSeries,
    RandomPartitionAccess, MaterializedViews, LWT, SAI, AllowFiltering,
    RangeScan, CreateDrop, DSESearch, Locking, TxnCounter

EXAMPLES:
    # Astra DB deployment
    $0 -u myuser -p mypass -b /path/to/bundle.zip

    # Direct Cassandra cluster connection
    $0 -u myuser -p mypass -H cassandra.example.com

    # Custom configuration with Astra
    $0 -n my-stress-test -N my-namespace \\
       -r 0.5 -R 5000 -t 20 -d 1h \\
       -u myuser -p mypass -b /path/to/bundle.zip

    # High throughput test with direct host
    $0 -R 50000 -t 50 -c 20 -d 30m \\
       -u myuser -p mypass -H 10.0.1.100

EOF
    exit 1
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -n|--name)
            JOB_NAME="$2"
            shift 2
            ;;
        -N|--namespace)
            NAMESPACE="$2"
            shift 2
            ;;
        -w|--workload)
            WORKLOAD="$2"
            shift 2
            ;;
        -r|--read-rate)
            READ_RATE="$2"
            shift 2
            ;;
        -C|--cl)
            CONSISTENCY_LEVEL="$2"
            shift 2
            ;;
        -R|--rate)
            OPS_RATE="$2"
            shift 2
            ;;
        -t|--threads)
            THREADS="$2"
            shift 2
            ;;
        -d|--duration)
            DURATION="$2"
            shift 2
            ;;
        -c|--concurrency)
            CONCURRENCY="$2"
            shift 2
            ;;
        -k|--keyspace)
            KEYSPACE="$2"
            shift 2
            ;;
        -u|--username)
            USERNAME="$2"
            shift 2
            ;;
        -p|--password)
            PASSWORD="$2"
            shift 2
            ;;
        -b|--bundle)
            ASTRA_BUNDLE="$2"
            shift 2
            ;;
        -H|--host)
            HOST="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "ERROR: Unknown option: $1"
            echo ""
            usage
            ;;
    esac
done

# Validate required parameters
if [ -z "$USERNAME" ]; then
    echo "ERROR: Username is required (-u/--username)"
    echo ""
    usage
fi

if [ -z "$PASSWORD" ]; then
    echo "ERROR: Password is required (-p/--password)"
    echo ""
    usage
fi

# Validate that either bundle or host is provided (but not both)
if [ -z "$ASTRA_BUNDLE" ] && [ -z "$HOST" ]; then
    echo "ERROR: Either Astra bundle (-b/--bundle) or Cassandra host (-H/--host) is required"
    echo ""
    usage
fi

if [ -n "$ASTRA_BUNDLE" ] && [ -n "$HOST" ]; then
    echo "ERROR: Cannot specify both Astra bundle (-b) and host (-H). Choose one."
    echo ""
    usage
fi

# Setup connection-specific variables
USE_ASTRA=false
BUNDLE_FILENAME=""
SECRET_NAME=""

if [ -n "$ASTRA_BUNDLE" ]; then
    USE_ASTRA=true
    # Validate Astra bundle file exists
    if [ ! -f "$ASTRA_BUNDLE" ]; then
        echo "ERROR: Astra bundle file not found: $ASTRA_BUNDLE"
        exit 1
    fi
    # Extract bundle filename for secret
    BUNDLE_FILENAME=$(basename "$ASTRA_BUNDLE")
    SECRET_NAME="${JOB_NAME}-astra-bundle"
fi

echo "=========================================="
echo "Cassandra Easy Stress Monitoring Deployment"
echo "=========================================="
echo ""
echo "Configuration:"
echo "  Job Name:          $JOB_NAME"
echo "  Namespace:         $NAMESPACE"
echo "  Workload:          $WORKLOAD"
echo "  Read Rate:         $READ_RATE"
echo "  Consistency Level: $CONSISTENCY_LEVEL"
echo "  Ops Rate:          $OPS_RATE ops/sec"
echo "  Threads:           $THREADS"
echo "  Duration:          $DURATION"
echo "  Concurrency:       $CONCURRENCY"
echo "  Keyspace:          $KEYSPACE"
echo "  Username:          $USERNAME"
if [ "$USE_ASTRA" = true ]; then
    echo "  Connection:        Astra DB"
    echo "  Astra Bundle:      $BUNDLE_FILENAME"
    echo "  Secret Name:       $SECRET_NAME"
else
    echo "  Connection:        Direct Host"
    echo "  Cassandra Host:    $HOST"
fi
echo ""

# Check if kubectl is available
if ! command -v kubectl &> /dev/null; then
    echo "ERROR: kubectl is not installed or not in PATH"
    exit 1
fi

# Check if namespace exists, create if not
if ! kubectl get namespace "$NAMESPACE" &> /dev/null; then
    echo "Namespace '$NAMESPACE' does not exist. Creating..."
    kubectl create namespace "$NAMESPACE"
    echo "✓ Namespace created"
else
    echo "✓ Namespace '$NAMESPACE' exists"
fi
echo ""

# Create or update Astra bundle secret (only if using Astra)
if [ "$USE_ASTRA" = true ]; then
    echo "Step 1: Creating Astra bundle secret..."
    echo "--------------------------------"
    if kubectl get secret "$SECRET_NAME" -n "$NAMESPACE" &> /dev/null; then
        echo "Secret '$SECRET_NAME' already exists. Deleting and recreating..."
        kubectl delete secret "$SECRET_NAME" -n "$NAMESPACE"
    fi

    kubectl create secret generic "$SECRET_NAME" \
        --from-file="$ASTRA_BUNDLE" \
        -n "$NAMESPACE"

    echo "✓ Secret '$SECRET_NAME' created with file: $BUNDLE_FILENAME"
    echo ""
fi

# Deploy ConfigMaps
STEP_NUM=2
if [ "$USE_ASTRA" = false ]; then
    STEP_NUM=1
fi
echo "Step $STEP_NUM: Deploying ConfigMaps..."
echo "--------------------------------"
kubectl apply -f "$SCRIPT_DIR/configmaps/prometheus-config.yaml" -n "$NAMESPACE"
kubectl apply -f "$SCRIPT_DIR/configmaps/grafana-datasource.yaml" -n "$NAMESPACE"
kubectl apply -f "$SCRIPT_DIR/configmaps/grafana-dashboard-provider.yaml" -n "$NAMESPACE"
kubectl apply -f "$SCRIPT_DIR/configmaps/grafana-dashboard.yaml" -n "$NAMESPACE"

echo ""
echo "✓ ConfigMaps deployed successfully"
echo ""

# Verify ConfigMaps
STEP_NUM=$((STEP_NUM + 1))
echo "Step $STEP_NUM: Verifying ConfigMaps..."
echo "--------------------------------"
kubectl get configmaps -n "$NAMESPACE" | grep -E "(prometheus-config|grafana-)" || true
echo ""

# Generate Job YAML from template
STEP_NUM=$((STEP_NUM + 1))
echo "Step $STEP_NUM: Generating Job manifest..."
echo "--------------------------------"

TEMP_JOB_FILE=$(mktemp)

cat > "$TEMP_JOB_FILE" << EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: $JOB_NAME
  namespace: $NAMESPACE
spec:
  template:
    spec:
      volumes:
EOF

# Add Astra bundle volume only if using Astra
if [ "$USE_ASTRA" = true ]; then
    cat >> "$TEMP_JOB_FILE" << EOF
        - name: astra-bundle
          secret:
            secretName: $SECRET_NAME
EOF
fi

cat >> "$TEMP_JOB_FILE" << EOF
        - name: results
          emptyDir: {}
        - name: stress-logs
          emptyDir: {}
        - name: prometheus-config
          configMap:
            name: prometheus-config
        - name: prometheus-data
          emptyDir: {}
        - name: grafana-datasource
          configMap:
            name: grafana-datasource
        - name: grafana-dashboard-provider
          configMap:
            name: grafana-dashboard-provider
        - name: grafana-dashboard
          configMap:
            name: grafana-dashboard
      
      containers:
        - name: cassandra-easy-stress
          image: adejanovski/cassandra-easy-stress:astra-jdk17-amd64
          command:
            - /bin/sh
            - -c
            - |
              java -jar /app/cassandra-easy-stress.jar run $WORKLOAD \\
                -r $READ_RATE \\
                --cl $CONSISTENCY_LEVEL \\
                --rate $OPS_RATE \\
                -t $THREADS \\
                -c $CONCURRENCY \\
                -d $DURATION \\
                --csv /tmp/stress/stress_results.csv \\
                --replication "{'class': 'NetworkTopologyStrategy', 'dc-1': 3}" \\
                --keyspace $KEYSPACE \\
                --skip-keyspace-creation \\
EOF

# Add connection-specific parameters
if [ "$USE_ASTRA" = true ]; then
    cat >> "$TEMP_JOB_FILE" << EOF
                --astra-bundle /astra/$BUNDLE_FILENAME \\
EOF
else
    cat >> "$TEMP_JOB_FILE" << EOF
                --host $HOST \\
EOF
fi

cat >> "$TEMP_JOB_FILE" << EOF
                -U '$USERNAME' \\
                -P '$PASSWORD'
              
              echo "Stress test completed. Keeping container alive for metrics access..."
              sleep infinity
          ports:
            - name: metrics
              containerPort: 9500
              protocol: TCP
          resources:
            requests:
              memory: "2Gi"
              cpu: "1000m"
            limits:
              memory: "4Gi"
              cpu: "2000m"
          volumeMounts:
EOF

# Add Astra bundle volume mount only if using Astra
if [ "$USE_ASTRA" = true ]; then
    cat >> "$TEMP_JOB_FILE" << EOF
            - name: astra-bundle
              mountPath: /astra
EOF
fi

cat >> "$TEMP_JOB_FILE" << EOF
            - name: results
              mountPath: /tmp/stress
            - name: stress-logs
              mountPath: /home/stress/.cassandra-easy-stress
          terminationMessagePath: /dev/termination-log
          terminationMessagePolicy: File
          imagePullPolicy: IfNotPresent
        
        - name: prometheus
          image: prom/prometheus:latest
          args:
            - '--config.file=/etc/prometheus/prometheus.yml'
            - '--storage.tsdb.path=/prometheus'
            - '--storage.tsdb.retention.time=7d'
            - '--web.console.libraries=/usr/share/prometheus/console_libraries'
            - '--web.console.templates=/usr/share/prometheus/consoles'
            - '--web.enable-lifecycle'
          ports:
            - name: prometheus
              containerPort: 9090
              protocol: TCP
          resources:
            requests:
              memory: "512Mi"
              cpu: "250m"
            limits:
              memory: "8Gi"
          volumeMounts:
            - name: prometheus-config
              mountPath: /etc/prometheus
            - name: prometheus-data
              mountPath: /prometheus
          livenessProbe:
            httpGet:
              path: /-/healthy
              port: 9090
            initialDelaySeconds: 30
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /-/ready
              port: 9090
            initialDelaySeconds: 5
            periodSeconds: 5
          terminationMessagePath: /dev/termination-log
          terminationMessagePolicy: File
          imagePullPolicy: IfNotPresent
        
        - name: grafana
          image: grafana/grafana:latest
          env:
            - name: GF_AUTH_ANONYMOUS_ENABLED
              value: "true"
            - name: GF_AUTH_ANONYMOUS_ORG_ROLE
              value: "Admin"
            - name: GF_AUTH_DISABLE_LOGIN_FORM
              value: "true"
            - name: GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH
              value: "/etc/grafana/provisioning/dashboards/cassandra-easy-stress-dashboard.json"
          ports:
            - name: grafana
              containerPort: 3000
              protocol: TCP
          resources:
            requests:
              memory: "256Mi"
              cpu: "100m"
            limits:
              memory: "2Gi"
          volumeMounts:
            - name: grafana-datasource
              mountPath: /etc/grafana/provisioning/datasources
            - name: grafana-dashboard-provider
              mountPath: /etc/grafana/provisioning/dashboards/provider
            - name: grafana-dashboard
              mountPath: /etc/grafana/provisioning/dashboards
          livenessProbe:
            httpGet:
              path: /api/health
              port: 3000
            initialDelaySeconds: 30
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /api/health
              port: 3000
            initialDelaySeconds: 5
            periodSeconds: 5
          terminationMessagePath: /dev/termination-log
          terminationMessagePolicy: File
          imagePullPolicy: IfNotPresent
      
      restartPolicy: Never
      terminationGracePeriodSeconds: 30
      dnsPolicy: ClusterFirst
      securityContext: {}
      schedulerName: default-scheduler
  
  completionMode: NonIndexed
  suspend: false
  podReplacementPolicy: TerminatingOrFailed
EOF

echo "✓ Job manifest generated"
echo ""

# Deploy the Job
echo "Step 5: Deploying the Job..."
echo "--------------------------------"
kubectl apply -f "$TEMP_JOB_FILE"
rm "$TEMP_JOB_FILE"
echo ""
echo "✓ Job deployed successfully"
echo ""

# Wait for pod to be created
echo "Step 6: Waiting for pod to be created..."
echo "--------------------------------"
sleep 3

POD_NAME=$(kubectl get pods -n "$NAMESPACE" -l job-name="$JOB_NAME" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")

if [ -z "$POD_NAME" ]; then
    echo "Pod not yet created. Waiting..."
    for i in {1..30}; do
        sleep 2
        POD_NAME=$(kubectl get pods -n "$NAMESPACE" -l job-name="$JOB_NAME" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
        if [ -n "$POD_NAME" ]; then
            break
        fi
    done
fi

if [ -z "$POD_NAME" ]; then
    echo "ERROR: Pod was not created after 60 seconds"
    echo "Check job status: kubectl describe job $JOB_NAME -n $NAMESPACE"
    exit 1
fi

echo "✓ Pod created: $POD_NAME"
echo ""

# Show pod status
echo "Step 7: Checking pod status..."
echo "--------------------------------"
kubectl get pod "$POD_NAME" -n "$NAMESPACE"
echo ""

echo "=========================================="
echo "Deployment Complete!"
echo "=========================================="
echo ""
echo "Job Name:  $JOB_NAME"
echo "Namespace: $NAMESPACE"
echo "Pod Name:  $POD_NAME"
echo ""
echo "Next Steps:"
echo "----------"
echo ""
echo "1. Monitor pod startup:"
echo "   kubectl get pod $POD_NAME -n $NAMESPACE -w"
echo ""
echo "2. View logs:"
echo "   kubectl logs -n $NAMESPACE $POD_NAME -c cassandra-easy-stress -f"
echo ""
echo "3. Access Grafana dashboard (once pod is ready):"
echo "   kubectl port-forward -n $NAMESPACE $POD_NAME 3000:3000"
echo "   Then open: http://localhost:3000"
echo ""
echo "4. Access Prometheus (optional):"
echo "   kubectl port-forward -n $NAMESPACE $POD_NAME 9090:9090"
echo "   Then open: http://localhost:9090"
echo ""
echo "5. View raw metrics (optional):"
echo "   kubectl port-forward -n $NAMESPACE $POD_NAME 9500:9500"
echo "   Then open: http://localhost:9500/metrics"
echo ""
echo "6. Cleanup when done:"
echo "   $SCRIPT_DIR/cleanup.sh -n $JOB_NAME -N $NAMESPACE"
echo ""
echo "For more information, see: $SCRIPT_DIR/README.md"
echo ""

# Made with Bob
