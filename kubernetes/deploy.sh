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
ASTRA_DB_ID=""
ASTRA_TOKEN=""
ASTRA_TOKEN_FILE="token.json"
ASTRA_API_HOST="api.astra.datastax.com"
TEMP_BUNDLE=""

# Function to display usage
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Deploy cassandra-easy-stress with Prometheus and Grafana monitoring to Kubernetes.

OPTIONS:
    -n, --name NAME              Job name (default: cassandra-easy-stress)
    -N, --namespace NAMESPACE    Kubernetes namespace (default: benchmarks)
    -w, --workload WORKLOAD      Workload name (default: BasicTimeSeries)
    -r, --read-rate RATE         Read rate ratio 0-1 (default: 0.25)
    -C, --cl LEVEL               Consistency level (default: LOCAL_QUORUM)
    -R, --rate OPS               Operations per second (default: 10000)
    -t, --threads NUM            Number of threads (default: 10)
    -d, --duration TIME          Duration (e.g., 3h, 30m, 1h30m) (default: 3h)
    -c, --concurrency NUM        Connections per thread (default: 10)
    -k, --keyspace NAME          Target keyspace (default: testks)
    -u, --username USER          Cassandra username (required unless using --astra with token file)
    -p, --password PASS          Cassandra password (required unless using --astra with token file)
    -b, --bundle PATH            Path to Astra bundle zip file (optional)
    -H, --host HOST              Cassandra host (optional)
    --astra DB_ID                Astra database UUID (downloads bundle via API)
    --astra-token TOKEN          Astra authentication token (AstraCS:...)
    --astra-token-file PATH      Path to JSON credentials file (default: token.json)
    --astra-api-host HOST        Astra API host (default: api.astra.datastax.com)
    -h, --help                   Display this help message

CONNECTION OPTIONS (mutually exclusive):
    Choose ONE of the following:
    1. --bundle PATH             Use existing Astra bundle file (requires -u/-p)
    2. --host HOST               Connect directly to Cassandra host (requires -u/-p)
    3. --astra DB_ID             Download Astra bundle via API (credentials from token file)

ASTRA TOKEN AUTHENTICATION:
    When using --astra, credentials can be provided via:
    1. JSON file (default: token.json in current directory)
       - Automatically extracts clientId → username, secret → password
    2. Command-line: --astra-token "AstraCS:..." (requires -u/-p)
    
    JSON file format:
    {
      "clientId": "nfBHuJnmszppzqzdfoQ",
      "secret": ",00grmb3c-vJka,AiHhlhGyyLcEd5UxbUQ9tqerMEXzOM2JSeE6LZYd9lv52ZNzQJspakLQB2.Z7iFZ...",
      "token": "AstraCS:nfBHuJnmszppzqSuELPzdfoQ:33cf5ca5ec48eeeb0c1c1e832e7c26d15f9ee54fde7017bbcf9471"
    }
    
    Note: When using token file, username/password are automatically extracted from
    clientId/secret fields. You can override them with -u/-p if needed.

AVAILABLE WORKLOADS:
    BasicTimeSeries, KeyValue, CountersWide, Maps, Sets, UdtTimeSeries,
    RandomPartitionAccess, MaterializedViews, LWT, SAI, AllowFiltering,
    RangeScan, CreateDrop, DSESearch, Locking, TxnCounter

EXAMPLES:
    # Astra DB with token file (credentials auto-extracted from token.json)
    $0 --astra 1c05f0ab-cd5d-4508-9c67-1da94007f124

    # Astra DB with custom token file
    $0 --astra 1c05f0ab-cd5d-4508-9c67-1da94007f124 \\
       --astra-token-file /path/to/credentials.json

    # Astra DB with command-line token (requires explicit username/password)
    $0 --astra 1c05f0ab-cd5d-4508-9c67-1da94007f124 \\
       --astra-token "AstraCS:..." -u myuser -p mypass

    # Astra DB with token file but override credentials
    $0 --astra 1c05f0ab-cd5d-4508-9c67-1da94007f124 \\
       -u custom_user -p custom_pass

    # Astra DB with existing bundle file
    $0 -u myuser -p mypass -b /path/to/bundle.zip

    # Direct Cassandra cluster connection
    $0 -u myuser -p mypass -H cassandra.example.com

    # Custom configuration with Astra API
    $0 --astra 1c05f0ab-cd5d-4508-9c67-1da94007f124 \\
       -n my-stress-test -N my-namespace \\
       -r 0.5 -R 5000 -t 20 -d 1h

    # High throughput test with direct host
    $0 -R 50000 -t 50 -c 20 -d 30m \\
       -u myuser -p mypass -H 10.0.1.100

EOF
    exit 1
}

# Function to load Astra credentials from token file or command line
load_astra_credentials() {
    local using_token_file=false
    
    # Check if jq is available (needed for JSON parsing)
    if ! command -v jq &> /dev/null; then
        echo "ERROR: 'jq' command not found"
        echo "       Please install jq: brew install jq (macOS) or apt-get install jq (Linux)"
        exit 1
    fi
    
    # If token not provided via command line, load from file
    if [ -z "$ASTRA_TOKEN" ]; then
        # Try to load from token file
        if [ ! -f "$ASTRA_TOKEN_FILE" ]; then
            echo "ERROR: Astra token file not found: $ASTRA_TOKEN_FILE"
            echo "       Either provide --astra-token or create a token.json file with:"
            echo "       {"
            echo "         \"clientId\": \"...\","
            echo "         \"secret\": \"...\","
            echo "         \"token\": \"AstraCS:...\""
            echo "       }"
            exit 1
        fi
        
        echo "Loading Astra credentials from: $ASTRA_TOKEN_FILE"
        using_token_file=true
        
        ASTRA_TOKEN=$(jq -r '.token' "$ASTRA_TOKEN_FILE" 2>/dev/null)
        
        if [ -z "$ASTRA_TOKEN" ] || [ "$ASTRA_TOKEN" == "null" ]; then
            echo "ERROR: Failed to read token from $ASTRA_TOKEN_FILE"
            echo "       Make sure the file contains valid JSON with 'token' field"
            exit 1
        fi
    else
        echo "Using Astra token from command line"
    fi
    
    # If using token file and username/password not provided, extract from JSON
    if [ "$using_token_file" = true ]; then
        if [ -z "$USERNAME" ]; then
            USERNAME=$(jq -r '.clientId' "$ASTRA_TOKEN_FILE" 2>/dev/null)
            if [ -z "$USERNAME" ] || [ "$USERNAME" == "null" ]; then
                echo "ERROR: Failed to read clientId from $ASTRA_TOKEN_FILE"
                echo "       Make sure the file contains valid JSON with 'clientId' field"
                exit 1
            fi
            echo "✓ Using clientId from token file as username"
        fi
        
        if [ -z "$PASSWORD" ]; then
            PASSWORD=$(jq -r '.secret' "$ASTRA_TOKEN_FILE" 2>/dev/null)
            if [ -z "$PASSWORD" ] || [ "$PASSWORD" == "null" ]; then
                echo "ERROR: Failed to read secret from $ASTRA_TOKEN_FILE"
                echo "       Make sure the file contains valid JSON with 'secret' field"
                exit 1
            fi
            echo "✓ Using secret from token file as password"
        fi
    fi
    
    echo "✓ Astra credentials loaded successfully"
}

# Function to download Astra secure connect bundle
download_astra_bundle() {
    echo "==========================================" >&2
    echo "Downloading Astra Secure Connect Bundle" >&2
    echo "==========================================" >&2
    echo "" >&2
    
    # Check if curl is available
    if ! command -v curl &> /dev/null; then
        echo "ERROR: 'curl' command not found" >&2
        echo "       Please install curl" >&2
        exit 1
    fi
    
    # Step 1: Validate database exists and get info
    echo "Validating Astra database: $ASTRA_DB_ID" >&2
    echo "Astra token: $ASTRA_TOKEN" >&2
    
    response=$(curl -sS -X GET \
        -H "Authorization: Bearer ${ASTRA_TOKEN}" \
        "https://${ASTRA_API_HOST}/v2/databases/${ASTRA_DB_ID}")
    
    STATUS=$(echo "$response" | jq -r '.status // "null"')
    DB_NAME=$(echo "$response" | jq -r '.info.name // "null"')
    
    if [ "$STATUS" == "null" ]; then
        echo "ERROR: Database ${ASTRA_DB_ID} not found or token is invalid" >&2
        echo "Response: $response" >&2
        exit 1
    fi
    
    echo "✓ Database found: $DB_NAME (Status: $STATUS)" >&2
    echo "" >&2
    
    # Step 2: Generate download URL for Secure Connect Bundle
    echo "Generating secure connect bundle URL..." >&2
    
    sb_response=$(curl -sS -X POST \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer ${ASTRA_TOKEN}" \
        "https://${ASTRA_API_HOST}/v2/databases/${ASTRA_DB_ID}/secureBundleURL")
    
    DOWNLOAD_URL=$(echo "$sb_response" | jq -r '.downloadURL // ""')
    
    if [ -z "$DOWNLOAD_URL" ] || [ "$DOWNLOAD_URL" == "null" ]; then
        echo "ERROR: Failed to get secure bundle download URL" >&2
        echo "Response: $sb_response" >&2
        exit 1
    fi
    
    echo "✓ Download URL generated" >&2
    echo "" >&2
    
    # Step 3: Download the bundle to a temporary file
    local TEMP_BUNDLE_FILE=$(mktemp /tmp/secure-connect-${DB_NAME}-XXXXXX.zip)
    
    echo "Downloading secure connect bundle..." >&2
    if ! curl -sS -L "$DOWNLOAD_URL" -o "$TEMP_BUNDLE_FILE"; then
        echo "ERROR: Failed to download secure connect bundle" >&2
        rm -f "$TEMP_BUNDLE_FILE"
        exit 1
    fi
    
    # Verify the bundle was downloaded and has content
    if [ ! -f "$TEMP_BUNDLE_FILE" ]; then
        echo "ERROR: Bundle file was not created" >&2
        exit 1
    fi
    
    if [ "$OSTYPE" == "darwin"* ]; then
        # macOS
        SIZE=$(stat -f%z "$TEMP_BUNDLE_FILE" 2>/dev/null)
    else
        # Linux
        SIZE=$(stat -c%s "$TEMP_BUNDLE_FILE" 2>/dev/null)
    fi
    
    if [ "$SIZE" -eq 0 ]; then
        echo "ERROR: Downloaded bundle file is empty" >&2
        rm -f "$TEMP_BUNDLE_FILE"
        exit 1
    fi
    
    echo "✓ Secure connect bundle downloaded successfully" >&2
    echo "  File: $TEMP_BUNDLE_FILE" >&2
    echo "  Size: $SIZE bytes" >&2
    echo "" >&2
    
    # Return the path to the downloaded bundle (stdout only)
    echo "$TEMP_BUNDLE_FILE"
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
        --astra)
            ASTRA_DB_ID="$2"
            shift 2
            ;;
        --astra-token)
            ASTRA_TOKEN="$2"
            shift 2
            ;;
        --astra-token-file)
            ASTRA_TOKEN_FILE="$2"
            shift 2
            ;;
        --astra-api-host)
            ASTRA_API_HOST="$2"
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
# Note: Username and password can be extracted from token file when using --astra
# So we only validate them after credential loading if not using --astra
if [ -z "$ASTRA_DB_ID" ]; then
    # Not using --astra, so username and password are required
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
fi

# Validate that exactly one connection method is provided
CONNECTION_COUNT=0
[ -n "$ASTRA_BUNDLE" ] && CONNECTION_COUNT=$((CONNECTION_COUNT + 1))
[ -n "$HOST" ] && CONNECTION_COUNT=$((CONNECTION_COUNT + 1))
[ -n "$ASTRA_DB_ID" ] && CONNECTION_COUNT=$((CONNECTION_COUNT + 1))

if [ $CONNECTION_COUNT -eq 0 ]; then
    echo "ERROR: One connection method is required:"
    echo "  - Astra bundle file: -b/--bundle PATH"
    echo "  - Cassandra host: -H/--host HOST"
    echo "  - Astra API download: --astra DATABASE_ID"
    echo ""
    usage
fi

if [ $CONNECTION_COUNT -gt 1 ]; then
    echo "ERROR: Only one connection method can be specified:"
    echo "  - Astra bundle file (-b/--bundle)"
    echo "  - Cassandra host (-H/--host)"
    echo "  - Astra API download (--astra)"
    echo ""
    usage
fi

# Handle Astra API download if --astra is specified
if [ -n "$ASTRA_DB_ID" ]; then
    echo "=========================================="
    echo "Astra API Bundle Download"
    echo "=========================================="
    echo ""
    
    # Load credentials (from file or command line)
    # This will also populate USERNAME and PASSWORD from token file if not provided
    load_astra_credentials
    echo ""
    
    # Validate that we now have username and password
    if [ -z "$USERNAME" ]; then
        echo "ERROR: Username is required"
        echo "       Provide via -u/--username or include 'clientId' in token file"
        exit 1
    fi
    
    if [ -z "$PASSWORD" ]; then
        echo "ERROR: Password is required"
        echo "       Provide via -p/--password or include 'secret' in token file"
        exit 1
    fi
    
    # Download the bundle
    TEMP_BUNDLE=$(download_astra_bundle)
    
    # Set ASTRA_BUNDLE to the downloaded file
    ASTRA_BUNDLE="$TEMP_BUNDLE"
    
    # Setup cleanup trap to remove temporary bundle on exit
    trap "rm -f $TEMP_BUNDLE" EXIT
    
    echo "✓ Bundle ready for deployment"
    echo ""
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
              java -Xmx4G -Xms4G -jar /app/cassandra-easy-stress.jar run $WORKLOAD \\
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
              cpu: "4"
            limits:
              memory: "5Gi"
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
              cpu: "1"
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
