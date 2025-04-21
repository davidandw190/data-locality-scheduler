
import argparse
import logging
import subprocess
import sys
import time
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("troubleshooter")

def check_kubernetes_resources(namespace="scheduler-benchmark"):
    logger.info(f"Checking Kubernetes resources in namespace: {namespace}")
    
    cmd = f"kubectl get namespace {namespace}"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"Namespace {namespace} does not exist")
        logger.info("Creating namespace...")
        create_cmd = f"kubectl create namespace {namespace}"
        subprocess.run(create_cmd, shell=True)
    else:
        logger.info(f"Namespace {namespace} exists")
    
    cmd = f"kubectl get deployments -n {namespace} -l app=minio"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if "No resources found" in result.stdout:
        logger.warning("No MinIO deployments found")
    else:
        logger.info(f"MinIO deployments:\n{result.stdout}")
    
    cmd = f"kubectl get pods -n {namespace} -l app=minio"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if "No resources found" in result.stdout:
        logger.warning("No MinIO pods found")
    else:
        logger.info(f"MinIO pods:\n{result.stdout}")
    
    cmd = f"kubectl get services -n {namespace} -l app=minio"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if "No resources found" in result.stdout:
        logger.warning("No MinIO services found")
    else:
        logger.info(f"MinIO services:\n{result.stdout}")
    
    cmd = f"kubectl get events -n {namespace} --sort-by='.metadata.creationTimestamp' | tail -20"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    logger.info(f"Recent events:\n{result.stdout}")
    
    cmd = "kubectl get nodes --show-labels"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    logger.info(f"Node information:\n{result.stdout}")
    
    return True

def deploy_test_storage():
    """Deploy test storage services from storage.yaml"""
    logger.info("Deploying test storage services")
    
    storage_yaml = Path("benchmarks/kubernetes/storage.yaml")
    if not storage_yaml.exists():
        logger.error(f"Storage YAML file not found: {storage_yaml}")
        return False
    
    cmd = f"kubectl apply -f {storage_yaml}"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"Failed to deploy storage services: {result.stderr}")
        return False
    
    logger.info("Storage services deployed. Waiting for pods to be ready...")
    time.sleep(10)
    
    cmd = "kubectl get pods -n scheduler-benchmark -l app=minio"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    logger.info(f"MinIO pod status:\n{result.stdout}")
    
    return True

def check_storage_connectivity():
    """Check connectivity to MinIO storage services"""
    logger.info("Checking connectivity to MinIO storage services")
    
    test_pod = """
apiVersion: v1
kind: Pod
metadata:
  name: storage-test
  namespace: scheduler-benchmark
spec:
  containers:
  - name: storage-test
    image: minio/mc
    command: ["/bin/sh", "-c"]
    args:
    - |
      echo "Testing MinIO connectivity..."
      
      # Add MinIO hosts
      mc config host add minio-central http://minio-central:9000 minioadmin minioadmin
      mc config host add minio-edge-r1 http://minio-edge-region1:9000 minioadmin minioadmin
      mc config host add minio-edge-r2 http://minio-edge-region2:9000 minioadmin minioadmin
      
      # Check connectivity
      echo "Testing minio-central:"
      mc ls minio-central || echo "Failed to connect to minio-central"
      
      echo "Testing minio-edge-region1:"
      mc ls minio-edge-r1 || echo "Failed to connect to minio-edge-region1"
      
      echo "Testing minio-edge-region2:"
      mc ls minio-edge-r2 || echo "Failed to connect to minio-edge-region2"
      
      # Create test buckets
      echo "Creating test buckets..."
      mc mb -p minio-central/test-bucket || echo "Failed to create bucket in central"
      mc mb -p minio-edge-r1/test-bucket || echo "Failed to create bucket in region1" 
      mc mb -p minio-edge-r2/test-bucket || echo "Failed to create bucket in region2"
      
      # Create and upload test files
      echo "Creating test files..."
      dd if=/dev/urandom of=/tmp/test-10mb.bin bs=1M count=10
      
      echo "Uploading test files..."
      mc cp /tmp/test-10mb.bin minio-central/test-bucket/test-10mb.bin || echo "Failed to upload to central"
      mc cp /tmp/test-10mb.bin minio-edge-r1/test-bucket/test-10mb.bin || echo "Failed to upload to region1"
      mc cp /tmp/test-10mb.bin minio-edge-r2/test-bucket/test-10mb.bin || echo "Failed to upload to region2"
      
      echo "Storage connectivity test complete"
      sleep 30
  restartPolicy: Never
"""
    
    test_pod_file = Path("benchmarks/storage-test-pod.yaml")
    with open(test_pod_file, 'w') as f:
        f.write(test_pod)
    
    cmd = f"kubectl apply -f {test_pod_file}"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"Failed to deploy storage test pod: {result.stderr}")
        return False
    
    logger.info("Storage test pod deployed. Waiting for it to run...")
    time.sleep(10)
    
    cmd = "kubectl get pod storage-test -n scheduler-benchmark"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    logger.info(f"Storage test pod status:\n{result.stdout}")
    
    max_wait = 60  # 60 seconds
    for _ in range(max_wait):
        cmd = "kubectl get pod storage-test -n scheduler-benchmark -o jsonpath='{.status.phase}'"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.stdout == "Succeeded" or result.stdout == "Failed":
            break
        time.sleep(1)
    
    cmd = "kubectl logs storage-test -n scheduler-benchmark"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    logger.info(f"Storage test pod logs:\n{result.stdout}")
    
    # cleanup
    cmd = f"kubectl delete -f {test_pod_file}"
    subprocess.run(cmd, shell=True)
    
    return True

def reset_benchmark_environment():
    """Reset the benchmark environment by deleting all resources"""
    logger.info("Resetting benchmark environment")
    
    namespace = "scheduler-benchmark"
    
    cmd = f"kubectl delete deployments --all -n {namespace}"
    subprocess.run(cmd, shell=True)
    
    cmd = f"kubectl delete pods --all -n {namespace}"
    subprocess.run(cmd, shell=True)
    
    cmd = f"kubectl delete services --all -n {namespace}"
    subprocess.run(cmd, shell=True)
    
    logger.info("Waiting for resources to be deleted...")
    time.sleep(10)
    
    cmd = f"kubectl get all -n {namespace}"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    logger.info(f"Resources after reset:\n{result.stdout}")
    
    return True

def main():
    parser = argparse.ArgumentParser(description='Benchmark Environment Troubleshooter')
    parser.add_argument('--check', action='store_true', help='Check Kubernetes resources')
    parser.add_argument('--deploy-storage', action='store_true', help='Deploy test storage services')
    parser.add_argument('--check-connectivity', action='store_true', help='Check storage connectivity')
    parser.add_argument('--reset', action='store_true', help='Reset benchmark environment')
    parser.add_argument('--all', action='store_true', help='Run all checks')
    
    args = parser.parse_args()
    
    if args.all or args.check:
        check_kubernetes_resources()
    
    if args.all or args.reset:
        reset_benchmark_environment()
    
    if args.all or args.deploy_storage:
        deploy_test_storage()
    
    if args.all or args.check_connectivity:
        check_storage_connectivity()
    
    if not (args.all or args.check or args.deploy_storage or args.check_connectivity or args.reset):
        parser.print_help()

if __name__ == '__main__':
    main()