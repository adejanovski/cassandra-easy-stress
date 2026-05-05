#!/bin/bash
# Cleanup script for cassandra-easy-stress monitoring deployment
# This script removes the Job, Secret, and optionally the ConfigMaps

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Default values
JOB_NAME="cassandra-stress"
NAMESPACE="amc-benchmarks"
DELETE_CONFIGMAPS=false

# Function to display usage
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Cleanup cassandra-easy-stress monitoring deployment from Kubernetes.

OPTIONS:
    -n, --name NAME              Job name (default: cassandra-stress)
    -N, --namespace NAMESPACE    Kubernetes namespace (default: amc-benchmarks)
    -C, --delete-configmaps      Delete ConfigMaps as well (default: keep them)
    -h, --help                   Display this help message

EXAMPLES:
    # Basic cleanup (keeps ConfigMaps)
    $0 -n my-stress-test -N my-namespace

    # Cleanup including ConfigMaps
    $0 -n my-stress-test -N my-namespace --delete-configmaps

    # Interactive cleanup with default job name
    $0

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
        -C|--delete-configmaps)
            DELETE_CONFIGMAPS=true
            shift
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

SECRET_NAME="${JOB_NAME}-astra-bundle"

echo "=========================================="
echo "Cassandra Easy Stress Monitoring Cleanup"
echo "=========================================="
echo ""
echo "Configuration:"
echo "  Job Name:   $JOB_NAME"
echo "  Namespace:  $NAMESPACE"
echo "  Secret:     $SECRET_NAME"
echo ""

# Check if kubectl is available
if ! command -v kubectl &> /dev/null; then
    echo "ERROR: kubectl is not installed or not in PATH"
    exit 1
fi

# Check if namespace exists
if ! kubectl get namespace "$NAMESPACE" &> /dev/null; then
    echo "ERROR: Namespace '$NAMESPACE' does not exist"
    exit 1
fi

# Delete the Job
echo "Step 1: Deleting the Job..."
echo "--------------------------------"
if kubectl get job "$JOB_NAME" -n "$NAMESPACE" &> /dev/null; then
    kubectl delete job "$JOB_NAME" -n "$NAMESPACE"
    echo "✓ Job '$JOB_NAME' deleted"
else
    echo "Job '$JOB_NAME' not found (already deleted?)"
fi
echo ""

# Wait for pod to be terminated
echo "Step 2: Waiting for pod termination..."
echo "--------------------------------"
sleep 2

POD_NAME=$(kubectl get pods -n "$NAMESPACE" -l job-name="$JOB_NAME" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")

if [ -n "$POD_NAME" ]; then
    echo "Waiting for pod $POD_NAME to terminate..."
    kubectl wait --for=delete pod/"$POD_NAME" -n "$NAMESPACE" --timeout=60s 2>/dev/null || true
fi

echo "✓ Pod terminated"
echo ""

# Delete the Secret (if it exists)
echo "Step 3: Checking for Astra bundle secret..."
echo "--------------------------------"
if kubectl get secret "$SECRET_NAME" -n "$NAMESPACE" &> /dev/null; then
    kubectl delete secret "$SECRET_NAME" -n "$NAMESPACE"
    echo "✓ Secret '$SECRET_NAME' deleted"
else
    echo "No secret found (may have used direct host connection)"
fi
echo ""

# Handle ConfigMaps
echo "Step 4: ConfigMap cleanup..."
echo "--------------------------------"

if [ "$DELETE_CONFIGMAPS" = true ]; then
    echo "Deleting ConfigMaps..."
    kubectl delete configmap prometheus-config -n "$NAMESPACE" 2>/dev/null && echo "  ✓ prometheus-config deleted" || echo "  prometheus-config not found"
    kubectl delete configmap grafana-datasource -n "$NAMESPACE" 2>/dev/null && echo "  ✓ grafana-datasource deleted" || echo "  grafana-datasource not found"
    kubectl delete configmap grafana-dashboard-provider -n "$NAMESPACE" 2>/dev/null && echo "  ✓ grafana-dashboard-provider deleted" || echo "  grafana-dashboard-provider not found"
    kubectl delete configmap grafana-dashboard -n "$NAMESPACE" 2>/dev/null && echo "  ✓ grafana-dashboard deleted" || echo "  grafana-dashboard not found"
    echo "✓ ConfigMaps deleted"
else
    echo "ConfigMaps kept (can be reused for future deployments)"
    echo "To delete ConfigMaps, run with --delete-configmaps flag"
fi
echo ""

echo "=========================================="
echo "Cleanup Complete!"
echo "=========================================="
echo ""
echo "Resources cleaned up:"
echo "  - Job: $JOB_NAME"
echo "  - Secret: $SECRET_NAME"
if [ "$DELETE_CONFIGMAPS" = true ]; then
    echo "  - ConfigMaps: deleted"
else
    echo "  - ConfigMaps: kept"
fi
echo ""
echo "To redeploy, run:"
echo "  $SCRIPT_DIR/deploy.sh -n $JOB_NAME -N $NAMESPACE -u <username> -p <password> -b <bundle-path>"
echo ""

# Made with Bob
