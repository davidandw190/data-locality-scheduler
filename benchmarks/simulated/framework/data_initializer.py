import argparse
import logging
import os
import subprocess
import sys
import time
import socket
import uuid
from pathlib import Path

import yaml

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("data-initializer")

class DataInitializer:
    def __init__(self, config_file, workloads_dir):
        self.config_file = Path(config_file)
        self.workloads_dir = Path(workloads_dir)
        self.config = self._load_config()
        self.namespace = self.config.get('kubernetes', {}).get('namespace', 'scheduler-benchmark')
        self.temp_dir = Path('/tmp/benchmark-data')
        self.temp_dir.mkdir(exist_ok=True)
        
    def _load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                config = yaml.safe_load(f)
            logger.info(f"Loaded configuration from {self.config_file}")
            return config
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise
    
    def _extract_data_references(self, workload_file):
        """Extract data references from workload YAML"""
        data_refs = {
            'input': [],
            'output': []
        }
        
        try:
            with open(workload_file, 'r') as f:
                workload = list(yaml.safe_load_all(f))
                
                for item in workload:
                    if item.get('kind') == 'Pod' and 'annotations' in item.get('metadata', {}):
                        annotations = item['metadata']['annotations']
                        
                        for k, v in annotations.items():
                            if k.startswith('data.scheduler.thesis/input-'):
                                parts = v.split(',')
                                if len(parts) >= 2:
                                    data_refs['input'].append({
                                        'urn': parts[0],
                                        'size_bytes': int(parts[1])
                                    })
                            
                            elif k.startswith('data.scheduler.thesis/output-'):
                                parts = v.split(',')
                                if len(parts) >= 2:
                                    data_refs['output'].append({
                                        'urn': parts[0],
                                        'size_bytes': int(parts[1])
                                    })
            
            return data_refs
        
        except Exception as e:
            logger.error(f"Failed to extract data references from {workload_file}: {e}")
            return data_refs
    
    def _configure_minio_client(self):
        try:
            logger.info("Configuring MinIO client connections")
            
            self._wait_for_minio_services()
            
            service_configs = [
                ("minio", "http://minio-central:9000"),
                ("region1", "http://minio-edge-region1:9000"),
                ("region2", "http://minio-edge-region2:9000")
            ]
            
            test_cmd = "mc --version"
            logger.info(f"Testing mc installation: {test_cmd}")
            test_result = subprocess.run(test_cmd, shell=True, capture_output=True, text=True)
            if test_result.returncode != 0:
                logger.error(f"MinIO client not properly installed: {test_result.stderr}")
                return False
            
            service_connection_success = False
            for service_name, endpoint in service_configs:
                for attempt in range(5):
                    cmd = f"mc config host add {service_name} {endpoint} minioadmin minioadmin --api s3v4"
                    logger.info(f"Configuring mc for {service_name}: {cmd}")
                    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                    
                    if result.returncode == 0:
                        logger.info(f"Successfully configured {service_name}")
                        service_connection_success = True
                        break
                    else:
                        logger.warning(f"Failed to configure {service_name}: {result.stderr}")
                        if attempt == 2:
                            cmd = f"kubectl get service minio-{service_name.replace('minio', 'central')} -n {self.namespace} -o jsonpath='{{.spec.clusterIP}}'"
                            ip_result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                            if ip_result.returncode == 0 and ip_result.stdout:
                                ip = ip_result.stdout.strip()
                                logger.info(f"Trying direct IP connection to {service_name}: {ip}")
                                alt_cmd = f"mc config host add {service_name} http://{ip}:9000 minioadmin minioadmin --api s3v4"
                                subprocess.run(alt_cmd, shell=True)
                    
                    time.sleep(5)
            
            if not service_connection_success:
                logger.error("Could not configure any MinIO connections. Check service availability.")
                return False
                
            connections_working = False
            for service_name, _ in service_configs:
                test_cmd = f"mc ls {service_name}/ || echo 'Failed'"
                logger.info(f"Testing connection to {service_name}: {test_cmd}")
                test_result = subprocess.run(test_cmd, shell=True, capture_output=True, text=True)
                
                if "Failed" not in test_result.stdout and "ERROR" not in test_result.stdout:
                    logger.info(f"Connection to {service_name} verified: {test_result.stdout}")
                    connections_working = True
                else:
                    logger.warning(f"Connection test to {service_name} failed: {test_result.stdout}{test_result.stderr}")
            
            return connections_working
            
        except Exception as e:
            logger.error(f"Failed to configure MinIO client: {e}")
            return False
    
    def _wait_for_minio_services(self):
        """Wait for MinIO services to be ready before proceeding"""
        logger.info("Waiting for MinIO services to be ready...")
        
        endpoints = [
            ("minio-central", 9000),
            ("minio-edge-region1", 9000),
            ("minio-edge-region2", 9000)
        ]
        
        max_retries = 30
        retry_interval = 5
        
        for host, port in endpoints:
            logger.info(f"Testing DNS resolution for {host}...")
            try:
                import socket
                ip_address = socket.gethostbyname(host)
                logger.info(f"Resolved {host} to {ip_address}")
            except Exception as e:
                logger.warning(f"DNS resolution failed for {host}: {e}")
                logger.warning(f"This may cause connection problems. Check Kubernetes services.")
        
        for host, port in endpoints:
            logger.info(f"Checking if {host}:{port} is accessible...")
            connected = False
            for attempt in range(max_retries):
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(5)
                    result = sock.connect_ex((host, port))
                    sock.close()
                    
                    if result == 0:
                        logger.info(f"Successfully connected to {host}:{port}")
                        connected = True
                        break
                    else:
                        logger.warning(f"Cannot connect to {host}:{port}, attempt {attempt+1}/{max_retries}")
                        if attempt < max_retries - 1:
                            time.sleep(retry_interval)
                except Exception as e:
                    logger.warning(f"Error connecting to {host}:{port}: {e}")
                    if attempt < max_retries - 1:
                        time.sleep(retry_interval)
            
            if not connected:
                logger.warning(f"Could not connect to {host}:{port} after {max_retries} attempts")
                logger.warning(f"Checking if service exists...")
                cmd = f"kubectl get service {host} -n {self.namespace}"
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                if result.returncode == 0:
                    logger.info(f"Service {host} exists: {result.stdout}")
                else:
                    logger.warning(f"Service {host} does not exist: {result.stderr}")
    
    def _create_buckets(self):
        """Create required buckets in MinIO"""
        # Base buckets
        base_buckets = [
            "datasets",
            "intermediate",
            "results", 
            "shared",
            "edge-data",
            "region1-bucket",
            "region2-bucket",
            "test-bucket"
        ]
        
        workload_buckets = set()
        for workload_file in self.workloads_dir.glob('*.yaml'):
            data_refs = self._extract_data_references(workload_file)
            
            for ref_type in ['input', 'output']:
                for ref in data_refs[ref_type]:
                    bucket = ref['urn'].split('/')[0]
                    workload_buckets.add(bucket)
        
        all_buckets = list(set(base_buckets) | workload_buckets)
        
        logger.info(f"Creating {len(all_buckets)} buckets: {all_buckets}")
        
        endpoint_map = {
            "minio": "minio",
            "region1": "region1",
            "region2": "region2"
        }
        
        for endpoint, endpoint_name in endpoint_map.items():
            for bucket in all_buckets:
                cmd = f"mc mb -p {endpoint}/{bucket}"
                logger.info(f"Running: {cmd}")
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                if result.returncode != 0 and "already exists" not in result.stderr:
                    logger.warning(f"Failed to create bucket {bucket} on {endpoint}: {result.stderr}")
        
        return True
    
    def _create_test_data(self, file_size, file_name, content_type="generic"):
        """Create test data file of specified size with characteristics based on content type"""
        file_path = self.temp_dir / file_name
        
        logger.info(f"Creating test file: {file_path} ({file_size} bytes) of type {content_type}")
        
        with open(file_path, 'wb') as f:
            # For certain content types, create more realistic data patterns
            if content_type in ["sensor_data", "iot_data", "streaming_data"]:
                # Create data with repeated patterns to simulate time series data
                chunk_size = min(1 * 1024 * 1024, file_size)  # 1MB chunks
                remaining_bytes = file_size
                
                while remaining_bytes > 0:
                    write_size = min(chunk_size, remaining_bytes)
                    # Add some structured data patterns
                    pattern = os.urandom(min(4096, write_size))
                    repeats = (write_size + len(pattern) - 1) // len(pattern)
                    f.write((pattern * repeats)[:write_size])
                    remaining_bytes -= write_size
            elif content_type in ["model", "feature_data", "image_data"]:
                # Create data with long sequential patterns (like model weights or image data)
                chunk_size = min(10 * 1024 * 1024, file_size)  # 10MB chunks
                remaining_bytes = file_size
                
                while remaining_bytes > 0:
                    write_size = min(chunk_size, remaining_bytes)
                    f.write(os.urandom(write_size))
                    remaining_bytes -= write_size
            else:
                # Generic random data
                chunk_size = min(10 * 1024 * 1024, file_size)  # 10MB or file size
                remaining_bytes = file_size
                
                while remaining_bytes > 0:
                    write_size = min(chunk_size, remaining_bytes)
                    f.write(os.urandom(write_size))
                    remaining_bytes -= write_size
        
        return file_path
    
    def _initialize_data(self):
        all_data_refs = {}
        
        for workload_file in self.workloads_dir.glob('*.yaml'):
            workload_name = workload_file.stem
            data_refs = self._extract_data_references(workload_file)
            all_data_refs[workload_name] = data_refs
            
            logger.info(f"Workload {workload_name}: {len(data_refs['input'])} inputs, {len(data_refs['output'])} outputs")
            
            for ref_type in ['input', 'output']:
                for i, ref in enumerate(data_refs[ref_type]):
                    urn = ref['urn']
                    size = ref['size_bytes']
                    logger.info(f"  {ref_type.capitalize()} {i+1}: {urn} (size: {size} bytes)")
        
        self._create_buckets()
        
        created_items = []
        
        # Strategic data distribution to highlight locality benefits and test different scenarios
        strategic_data = [
            # Large input datasets on edge nodes (region-specific)
            {"urn": "edge-data/sensor-readings.json", "size": 419430400, "service": "region1", "content_type": "sensor_data"},
            {"urn": "edge-data/sensor-logs.json", "size": 209715200, "service": "region1", "content_type": "log_data"},
            {"urn": "edge-data/live-data.json", "size": 10485760, "service": "region1", "content_type": "streaming_data"},
            
            # Stream processing data
            {"urn": "edge-data/stream-buffer-1.dat", "size": 52428800, "service": "region1", "content_type": "streaming_data"},
            {"urn": "edge-data/stream-buffer-2.dat", "size": 52428800, "service": "region1", "content_type": "streaming_data"},
            
            # Image processing datasets
            {"urn": "edge-data/raw-images-batch1.tar", "size": 209715200, "service": "region1", "content_type": "image_data"},
            {"urn": "region2-bucket/raw-images-batch2.tar", "size": 209715200, "service": "region2", "content_type": "image_data"},
            {"urn": "datasets/ml-model-weights.h5", "size": 314572800, "service": "minio", "content_type": "model"},
            
            # ETL raw data
            {"urn": "region1-bucket/raw-data-batch.csv", "size": 104857600, "service": "region1", "content_type": "structured_data"},
            {"urn": "region2-bucket/raw-data-batch.csv", "size": 104857600, "service": "region2", "content_type": "structured_data"},
            {"urn": "datasets/reference-data.json", "size": 20971520, "service": "minio", "content_type": "reference_data"},
            
            # Region 1 datasets
            {"urn": "region1-bucket/reference-models.h5", "size": 157286400, "service": "region1", "content_type": "model"},
            {"urn": "region1-bucket/reference-data.json", "size": 104857600, "service": "region1", "content_type": "reference_data"},
            {"urn": "edge-data/sensor-data-training.json", "size": 52428800, "service": "region1", "content_type": "training_data"},
            {"urn": "edge-data/inference-data.json", "size": 10485760, "service": "region1", "content_type": "inference_data"},
            
            # Region 2 datasets
            {"urn": "region2-bucket/iot-readings.json", "size": 209715200, "service": "region2", "content_type": "iot_data"},
            {"urn": "region2-bucket/reference-data.json", "size": 104857600, "service": "region2", "content_type": "reference_data"},
            {"urn": "region2-bucket/live-data.json", "size": 10485760, "service": "region2", "content_type": "streaming_data"},
            
            # Central cloud datasets (shared between regions)
            {"urn": "datasets/training-data.parquet", "size": 314572800, "service": "minio", "content_type": "combined_data"},
            {"urn": "intermediate/sample-features.npz", "size": 52428800, "service": "minio", "content_type": "feature_data"},
            
            # Additional test datasets with varying sizes
            {"urn": "edge-data/small-dataset.json", "size": 1048576, "service": "region1", "content_type": "small_data"},
            {"urn": "edge-data/medium-dataset.json", "size": 10485760, "service": "region1", "content_type": "medium_data"}, 
            {"urn": "edge-data/large-dataset.json", "size": 104857600, "service": "region1", "content_type": "large_data"},
            
            # Duplicate data across regions to test data locality decisions
            {"urn": "region1-bucket/shared-dataset.parquet", "size": 52428800, "service": "region1", "content_type": "duplicate_data"},
            {"urn": "region2-bucket/shared-dataset.parquet", "size": 52428800, "service": "region2", "content_type": "duplicate_data"},
            {"urn": "datasets/shared-dataset.parquet", "size": 52428800, "service": "minio", "content_type": "duplicate_data"},
            
            # Sensor data for edge processing
            {"urn": "edge-data/sensor-data.json", "size": 20971520, "service": "region1", "content_type": "sensor_data"},
            
            # Test data for validating scheduler choices
            {"urn": "test-bucket/test-data.bin", "size": 10485760, "service": "minio", "content_type": "test_data"}
        ]
        
        logger.info("Creating strategic data distribution for comprehensive locality testing...")
        for data_item in strategic_data:
            urn = data_item["urn"]
            size_bytes = data_item["size"]
            service = data_item["service"]
            content_type = data_item.get("content_type", "generic")
            
            parts = urn.split('/', 1)
            bucket = parts[0]
            path = parts[1] if len(parts) > 1 else f"test-data-{uuid.uuid4()}.dat"
            
            # Create data file with specific characteristics based on content type
            file_name = f"strategic_{bucket}_{uuid.uuid4()}.dat"
            logger.info(f"Creating test file {file_name} ({size_bytes} bytes) for {urn} with type {content_type}")
            file_path = self._create_test_data(size_bytes, file_name, content_type)
            
            success = False
            for attempt in range(3):
                try:
                    cmd = f"mc cp {file_path} {service}/{bucket}/{path}"
                    logger.info(f"Uploading: {cmd}")
                    
                    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                    if result.returncode == 0:
                        logger.info(f"Successfully uploaded {urn} to {service}")
                        success = True
                        created_items.append((service, urn))
                        break
                    else:
                        logger.warning(f"Failed to upload {urn} to {service}: {result.stderr}")
                        if "bucket does not exist" in result.stderr:
                            create_cmd = f"mc mb -p {service}/{bucket}"
                            logger.info(f"Creating bucket: {create_cmd}")
                            subprocess.run(create_cmd, shell=True)
                        time.sleep(2)
                except Exception as e:
                    logger.error(f"Error during upload attempt {attempt+1}: {e}")
                    time.sleep(2)
            
            if not success:
                logger.error(f"Failed to upload {urn} to {service} after multiple attempts")
        
        # Process the original workload data references
        for workload_name, data_refs in all_data_refs.items():
            logger.info(f"Initializing data for workload: {workload_name}")
            
            for i, data_ref in enumerate(data_refs['input']):
                urn = data_ref['urn']
                size_bytes = data_ref['size_bytes']
                
                # Skip if already created in strategic distribution
                if any(created[1] == urn for created in created_items):
                    logger.info(f"Skipping {urn} as it was already created in strategic distribution")
                    continue
                
                parts = urn.split('/', 1)
                bucket = parts[0]
                path = parts[1] if len(parts) > 1 else f"{workload_name}_input_{i}.dat"
                
                file_name = f"{workload_name}_input_{i}_{uuid.uuid4()}.dat"
                file_path = self._create_test_data(size_bytes, file_name)
                
                target_service = "minio"  # Default to central
                if bucket.startswith("region1") or bucket == "edge-data":
                    target_service = "region1"
                elif bucket.startswith("region2"):
                    target_service = "region2"
                
                # Upload workload-defined data
                logger.info(f"Uploading {urn} to service {target_service} (size: {size_bytes} bytes)")
                self._upload_file(file_path, target_service, bucket, path, created_items)
                
        logger.info(f"Created {len(created_items)} data items:")
        for service, urn in created_items:
            logger.info(f"  {service}: {urn}")
        
        logger.info("Verifying created data...")
        self._verify_created_data(created_items)
        
        return True

    def _upload_file(self, file_path, target_service, bucket, path, created_items):
        """Helper to upload a file to MinIO with retry logic"""
        success = False
        for attempt in range(3):
            try:
                cmd = f"mc cp {file_path} {target_service}/{bucket}/{path}"
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                if result.returncode == 0:
                    logger.info(f"Successfully uploaded to {target_service}/{bucket}/{path}")
                    success = True
                    created_items.append((target_service, f"{bucket}/{path}"))
                    break
                else:
                    logger.warning(f"Failed to upload: {result.stderr}")
                    if "bucket does not exist" in result.stderr:
                        create_cmd = f"mc mb -p {target_service}/{bucket}"
                        logger.info(f"Creating bucket: {create_cmd}")
                        subprocess.run(create_cmd, shell=True)
                    time.sleep(2)
            except Exception as e:
                logger.error(f"Error during upload attempt {attempt+1}: {e}")
                time.sleep(2)
        
        return success

    def _verify_created_data(self, created_items):
        """Verify that all created data items exist"""
        verification_failures = 0
        
        for service, urn in created_items:
            bucket = urn.split('/', 1)[0]
            path = urn.split('/', 1)[1] if '/' in urn else ""
            cmd = f"mc stat {service}/{bucket}/{path}"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            if result.returncode != 0:
                logger.warning(f"Verification failed for {service}/{urn}: {result.stderr}")
                verification_failures += 1
        
        if verification_failures > 0:
            logger.warning(f"{verification_failures} data items failed verification")
        else:
            logger.info("All data items successfully verified")
    
    def run(self):
        logger.info("Starting data initialization for benchmarks")
        
        if not self._configure_minio_client():
            logger.error("Failed to configure MinIO client, aborting initialization")
            return False
        
        if not self._create_buckets():
            logger.error("Failed to create buckets, aborting initialization")
            return False
        
        if not self._initialize_data():
            logger.error("Failed to initialize data, aborting initialization")
            return False
        
        logger.info("Data initialization completed successfully")
        return True

def main():
    parser = argparse.ArgumentParser(description='Initialize data for scheduler benchmarks')
    parser.add_argument('--config', type=str, default='benchmarks/simulated/framework/benchmark_config.yaml',
                        help='Path to benchmark configuration file')
    parser.add_argument('--workloads-dir', type=str, default='benchmarks/simulated/workloads',
                        help='Directory containing workload definitions')
    
    args = parser.parse_args()
    
    initializer = DataInitializer(args.config, args.workloads_dir)
    initializer.run()

if __name__ == '__main__':
    main()