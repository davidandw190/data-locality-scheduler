import argparse
import json
import logging
import os
import random
import subprocess
import sys
import time
import uuid
from pathlib import Path

import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("workload-simulator")

class WorkloadSimulator:
    def __init__(self, config_file=None, workload_type=None, duration=None, intensity=None):
        self.config = self._load_config(config_file)
        self.workload_type = workload_type or self.config.get('workloadType', 'generic')
        self.duration = duration or self.config.get('duration', 60)
        self.intensity = intensity or self.config.get('intensity', 'medium')
        self.run_id = str(uuid.uuid4())[:8]
        
        self.node_name = os.environ.get('NODE_NAME', 'unknown')
        self.pod_name = os.environ.get('POD_NAME', 'unknown')
        
        self.input_data = self._parse_data_references('input')
        self.output_data = self._parse_data_references('output')
        
        self.metrics = {
            "node": self.node_name,
            "pod": self.pod_name,
            "workload_type": self.workload_type,
            "duration": self.duration,
            "intensity": self.intensity,
            "start_time": time.time(),
            "cpu_usage": [],
            "memory_usage": [],
            "actions": []
        }
        
        logger.info(f"Initializing {self.workload_type} workload simulator (ID: {self.run_id})")
        logger.info(f"Running on node: {self.node_name}, pod: {self.pod_name}")
        logger.info(f"Workload parameters: duration={self.duration}s, intensity={self.intensity}")
        
        if self.input_data:
            logger.info(f"Input data references:")
            for i, data in enumerate(self.input_data):
                logger.info(f"  Input {i+1}: {data['urn']} ({data['size_bytes']} bytes)")
        
        if self.output_data:
            logger.info(f"Output data references:")
            for i, data in enumerate(self.output_data):
                logger.info(f"  Output {i+1}: {data['urn']} ({data['size_bytes']} bytes)")
    
    def _parse_data_references(self, ref_type):
        data_refs = []
        for k, v in os.environ.items():
            if k.startswith(f"DATA_SCHEDULER_THESIS_{ref_type.upper()}_"):
                try:
                    parts = v.split(',')
                    if len(parts) >= 2:
                        urn = parts[0]
                        size_bytes = int(parts[1])
                        
                        processing_time = int(parts[2]) if len(parts) > 2 else 0
                        priority = int(parts[3]) if len(parts) > 3 else 5
                        data_type = parts[4] if len(parts) > 4 else "generic"
                        
                        data_refs.append({
                            'urn': urn,
                            'size_bytes': size_bytes,
                            'processing_time': processing_time,
                            'priority': priority,
                            'data_type': data_type
                        })
                except (ValueError, IndexError) as e:
                    logger.warning(f"Error parsing {ref_type} data reference: {v}: {e}")
        
        return data_refs
    
    def _load_config(self, config_file):
        if config_file and Path(config_file).exists():
            with open(config_file, 'r') as f:
                try:
                    return json.load(f)
                except json.JSONDecodeError as e:
                    logger.error(f"Error parsing config file: {e}")
                    return {}
        return {}
    
    def _get_intensity_multiplier(self):
        intensity_levels = {
            "very-low": 0.2,
            "low": 0.5,
            "medium": 1.0,
            "high": 2.0,
            "very-high": 4.0
        }
        return intensity_levels.get(self.intensity, 1.0)

    def _download_data(self, data_reference):
        urn = data_reference['urn']
        size_bytes = data_reference['size_bytes']
        
        logger.info(f"Downloading data: {urn} ({size_bytes} bytes)")
        
        parts = urn.split('/', 1)
        bucket = parts[0]
        path = parts[1] if len(parts) > 1 else ""
        
        output_dir = Path('/tmp/data')
        output_dir.mkdir(exist_ok=True)
        output_file = output_dir / f"{bucket}_{path.replace('/', '_')}"
        
        try:
            mc_cmd = f"mc cp minio/{bucket}/{path} {output_file}"
            logger.info(f"Running: {mc_cmd}")
            
            result = subprocess.run(
                mc_cmd, 
                shell=True, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE, 
                text=True
            )
            
            if result.returncode != 0:
                logger.warning(f"MinIO download failed: {result.stderr}")
                
                # we create mock data of appropriate size
                logger.info(f"Creating mock data of size {size_bytes} bytes")
                with open(output_file, 'wb') as f:
                    chunk_size = min(10 * 1024 * 1024, size_bytes)  # 10MB or file size
                    remaining_bytes = size_bytes
                    
                    while remaining_bytes > 0:
                        write_size = min(chunk_size, remaining_bytes)
                        f.write(os.urandom(write_size))
                        remaining_bytes -= write_size
            else:
                logger.info(f"Successfully downloaded {urn} to {output_file}")
        
        except Exception as e:
            logger.error(f"Error downloading data: {e}")
            return None
        
        return str(output_file)
    

    def _upload_data(self, data_reference, local_file=None):
        """Actually upload data to MinIO"""
        urn = data_reference['urn']
        size_bytes = data_reference['size_bytes']
        
        parts = urn.split('/', 1)
        bucket = parts[0]
        path = parts[1] if len(parts) > 1 else ""
        
        # we create mock data if no local file is provided
        if not local_file:
            temp_file = Path(f"/tmp/output_{uuid.uuid4()}.dat")
            with open(temp_file, 'wb') as f:
                chunk_size = min(10 * 1024 * 1024, size_bytes)  # 10MB or file size
                remaining_bytes = size_bytes
                
                while remaining_bytes > 0:
                    write_size = min(chunk_size, remaining_bytes)
                    f.write(os.urandom(write_size))
                    remaining_bytes -= write_size
            
            local_file = str(temp_file)
        
        logger.info(f"Uploading data to: {urn} ({size_bytes} bytes)")
        
        minio_service = "minio"  
        if bucket in ["edge-data", "region1-bucket"]:
            minio_service = "region1"
        elif bucket in ["region2-bucket"]:
            minio_service = "region2"
        
        for attempt in range(3): 
            try:
                create_bucket_cmd = f"mc mb -p {minio_service}/{bucket}"
                logger.info(f"Ensuring bucket exists: {create_bucket_cmd}")
                subprocess.run(create_bucket_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                
                mc_cmd = f"mc cp {local_file} {minio_service}/{bucket}/{path}"
                logger.info(f"Running: {mc_cmd}")
                
                result = subprocess.run(
                    mc_cmd, 
                    shell=True, 
                    stdout=subprocess.PIPE, 
                    stderr=subprocess.PIPE
                )
                
                if result.returncode == 0:
                    logger.info(f"Successfully uploaded to {urn}")
                    return True
                else:
                    logger.warning(f"MinIO upload failed (attempt {attempt+1}): {result.stderr.decode()}")
                    
                    if attempt == 1:  
                        alternate_services = ["minio", "region1", "region2"]
                        alternate_services.remove(minio_service)
                        
                        for alt_service in alternate_services:
                            alt_create_cmd = f"mc mb -p {alt_service}/{bucket}"
                            subprocess.run(alt_create_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                            
                            alt_cmd = f"mc cp {local_file} {alt_service}/{bucket}/{path}"
                            logger.info(f"Trying alternate service: {alt_cmd}")
                            
                            alt_result = subprocess.run(
                                alt_cmd, 
                                shell=True, 
                                stdout=subprocess.PIPE, 
                                stderr=subprocess.PIPE
                            )
                            
                            if alt_result.returncode == 0:
                                logger.info(f"Successfully uploaded to alternate service {alt_service}")
                                return True
                    
                    time.sleep(2) 
            except Exception as e:
                logger.error(f"Error during upload attempt {attempt+1}: {e}")
                time.sleep(2)  
        
        logger.error(f"Failed to upload {urn} after multiple attempts")
        return False
    
    def process_with_real_data(self, action):
        logger.info(f"Starting action: {action} with real data interaction")
        
        start_time = time.time()
        
        node_name = os.environ.get("NODE_NAME", "unknown")
        region = os.environ.get("NODE_REGION", "unknown")
        pod_name = os.environ.get("POD_NAME", "unknown")
        data_refs = []
        
        for k, v in os.environ.items():
            if k.startswith("DATA_SCHEDULER_THESIS_INPUT") or k.startswith("DATA_SCHEDULER_THESIS_OUTPUT"):
                data_refs.append(f"{k}={v}")
        
        logger.info(f"Pod {pod_name} running on node {node_name} (region: {region})")
        logger.info(f"Data references: {data_refs}")
        
        duration_override = os.environ.get("WORKLOAD_DURATION")
        if duration_override:
            try:
                self.duration = int(duration_override)
                logger.info(f"Using override duration: {self.duration}s")
            except ValueError:
                logger.warning(f"Invalid duration override: {duration_override}")
        
        if not hasattr(self, 'mc_configured'):
            self._configure_minio_clients()
        
        # download input data
        input_files = []
        for data_ref in self.input_data:
            for attempt in range(3):  
                try:
                    file_path = self._download_data(data_ref)
                    if file_path:
                        input_files.append(file_path)
                        break
                    else:
                        logger.warning(f"Download failed for {data_ref['urn']}, attempt {attempt+1}")
                except Exception as e:
                    logger.warning(f"Download attempt {attempt+1} failed: {e}")
                    time.sleep(2) 
        
        logger.info(f"Processing {len(input_files)} input files")
        for i, file_path in enumerate(input_files):
            if i < len(self.input_data):
                logger.info(f"Input {i+1}: {self.input_data[i]['urn']} -> {file_path}")
        
        local_refs = 0
        # same_zone_refs = 0
        # same_region_refs = 0
        # cross_region_refs = 0
        total_refs = len(self.input_data) + len(self.output_data)
        local_data_size = 0
        total_data_size = 0
        # same_zone_data_size = 0
        # same_region_data_size = 0
        # cross_region_data_size = 0
        
        
        # edge_to_cloud_transfers = 0
        # cloud_to_edge_transfers = 0
        # edge_to_cloud_data_size = 0
        # cloud_to_edge_data_size = 0
        
        for i, file_path in enumerate(input_files):
            if i < len(self.input_data):
                urn = self.input_data[i]['urn']
                size = self.input_data[i]['size_bytes']
                total_data_size += size
                bucket = urn.split('/')[0] if '/' in urn else urn
                
                is_local = False
                if (bucket in node_name or 
                    node_name in bucket or 
                    ('edge' in bucket and 'edge' in node_name) or
                    ('cloud' in bucket and 'cloud' in node_name) or
                    ('central' in bucket and 'cloud' in node_name) or
                    ('region1' in bucket and 'region-1' in region) or
                    ('region2' in bucket and 'region-2' in region)):
                    is_local = True
                    local_refs += 1
                    local_data_size += size
                    logger.info(f"Data appears to be LOCAL: {urn} on node {node_name}")
                else:
                    logger.info(f"Data appears to be REMOTE: {urn} on node {node_name}")
        
        if action in ['extract', 'collect']:
            self._perform_data_extraction(input_files)
        elif action in ['transform', 'process', 'analyze']:
            self._perform_data_transformation(input_files)
        elif action in ['train', 'predict']:
            self._perform_model_training(input_files)
        else:
            self._perform_generic_processing(input_files)
        
        intensity_multiplier = self._get_intensity_multiplier()
        if self.duration > 0:
            simulated_duration = self.duration * intensity_multiplier
            randomized_duration = max(2, min(120, simulated_duration * (0.8 + 0.4 * random.random())))
            logger.info(f"Simulating workload execution for {randomized_duration:.1f} seconds")
            
            end_time = time.time() + randomized_duration
            while time.time() < end_time:
                # we want to do some actual CPU work
                for _ in range(int(1000000 * intensity_multiplier)):
                    _ = random.random() ** 2
                
                time.sleep(0.1)
        
        # upload output files
        uploaded_outputs = 0
        for data_ref in self.output_data:
            total_data_size += data_ref['size_bytes']
            
            for attempt in range(3): 
                try:
                    success = self._upload_data(data_ref)
                    if success:
                        uploaded_outputs += 1
                        bucket = data_ref['urn'].split('/')[0] if '/' in data_ref['urn'] else data_ref['urn']
                        if (bucket in node_name or 
                            node_name in bucket or
                            ('edge' in bucket and 'edge' in node_name) or
                            ('cloud' in bucket and 'cloud' in node_name) or
                            ('central' in bucket and 'cloud' in node_name) or
                            ('region1' in bucket and 'region-1' in region) or
                            ('region2' in bucket and 'region-2' in region)):
                            local_refs += 1
                            local_data_size += data_ref['size_bytes']
                            logger.info(f"Output appears to be LOCAL: {data_ref['urn']} on node {node_name}")
                        else:
                            logger.info(f"Output appears to be REMOTE: {data_ref['urn']} on node {node_name}")
                        break
                    else:
                        logger.warning(f"Upload attempt {attempt+1} failed")
                        time.sleep(2) 
                except Exception as e:
                    logger.warning(f"Upload attempt {attempt+1} failed with exception: {e}")
                    time.sleep(2) 
        
        end_time = time.time()
        duration = end_time - start_time
        
        data_locality_score = local_refs / total_refs if total_refs > 0 else 0
        size_weighted_locality = local_data_size / total_data_size if total_data_size > 0 else 0
        
        logger.info(f"Completed action: {action} in {duration:.2f} seconds")
        logger.info(f"Processed {len(input_files)} input files, generated {uploaded_outputs} output files")
        logger.info(f"Data locality score: {data_locality_score:.2f} ({local_refs} local refs out of {total_refs} total)")
        logger.info(f"Size-weighted data locality: {size_weighted_locality:.2f} ({local_data_size/1024/1024:.2f}MB local data out of {total_data_size/1024/1024:.2f}MB total)")
        
        return {
            'action': action,
            'start_time': start_time,
            'end_time': end_time,
            'duration': duration,
            'input_files_processed': len(input_files),
            'output_files_generated': uploaded_outputs,
            'data_locality_score': data_locality_score,
            'size_weighted_locality': size_weighted_locality,
            'local_refs': local_refs,
            'total_refs': total_refs,
            'local_data_size': local_data_size,
            'total_data_size': total_data_size
        }
        
    def _configure_minio_clients(self):
        logger.info("Configuring MinIO clients")
        
        minio_services = [
            ("minio", "http://minio-central.scheduler-benchmark.svc.cluster.local:9000"),
            ("region1", "http://minio-edge-region1.scheduler-benchmark.svc.cluster.local:9000"),
            ("region2", "http://minio-edge-region2.scheduler-benchmark.svc.cluster.local:9000"),
            
            ("minio", "http://minio.data-locality-scheduler.svc.cluster.local:9000"),
            ("region1", "http://minio-edge-region1.data-locality-scheduler.svc.cluster.local:9000"),
            ("region2", "http://minio-edge-region2.data-locality-scheduler.svc.cluster.local:9000"),
            
            ("minio", "http://minio-central:9000"),
            ("region1", "http://minio-edge-region1:9000"),
            ("region2", "http://minio-edge-region2:9000"),
            
            ("minio", "http://minio:9000"),
            ("minio-central", "http://minio-central:9000"),
        ]
        
        connected_services = []
        for name, endpoint in minio_services:
            try:
                logger.info(f"Trying to connect to MinIO service {name} at {endpoint}")
                cmd = f"mc config host add {name} {endpoint} minioadmin minioadmin"
                result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                
                if result.returncode == 0:
                    logger.info(f"Successfully configured MinIO client for {name} at {endpoint}")
                    connected_services.append(name)
                    
                    list_cmd = f"mc ls {name}/"
                    list_result = subprocess.run(list_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    if list_result.returncode == 0:
                        logger.info(f"Connection to {name} verified - listing buckets successful")
                        buckets = list_result.stdout.decode().strip().split('\n')
                        if buckets and buckets[0]:  # there are buckets
                            logger.info(f"Found buckets on {name}: {len(buckets)}")
                    else:
                        logger.warning(f"Connection to {name} failed verification: {list_result.stderr.decode()}")
                else:
                    logger.warning(f"Failed to configure MinIO client for {name}: {result.stderr.decode()}")
            except Exception as e:
                logger.warning(f"Error configuring MinIO client for {name}: {e}")
        
        if not connected_services:
            logger.error("Failed to connect to any MinIO service! Will use mock data instead.")
        
        self.mc_configured = True
        self.connected_services = connected_services
        return connected_services
    
    
    def _download_data(self, data_reference):
        urn = data_reference['urn']
        size_bytes = data_reference['size_bytes']
        
        logger.info(f"Downloading data: {urn} ({size_bytes} bytes)")
        

        parts = urn.split('/', 1)
        bucket = parts[0]
        path = parts[1] if len(parts) > 1 else ""
        
        output_dir = Path('/tmp/data')
        output_dir.mkdir(exist_ok=True)
        output_file = output_dir / f"{bucket}_{path.replace('/', '_')}"
        
        minio_service = "minio"  # default to central
        if bucket in ["edge-data", "region1-bucket"]:
            minio_service = "region1"
        elif bucket in ["region2-bucket"]:
            minio_service = "region2"
        
        if not hasattr(self, 'mc_configured'):
            mc_config_services = [
                ("minio", "http://minio-central:9000"),
                ("region1", "http://minio-edge-region1:9000"),
                ("region2", "http://minio-edge-region2:9000")
            ]
            
            for name, endpoint in mc_config_services:
                logger.info(f"Configuring MinIO client for {name} at {endpoint}")
                cmd = f"mc config host add {name} {endpoint} minioadmin minioadmin --api s3v4 --insecure"
                result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                if result.returncode != 0:
                    logger.warning(f"Failed to configure mc for {name}: {result.stderr.decode()}")
                else:
                    logger.info(f"Successfully configured mc for {name}")
            
            self.mc_configured = True
        
        download_success = False
        for attempt in range(3): 
            try:
                mc_cmd = f"mc cp {minio_service}/{bucket}/{path} {output_file}"
                logger.info(f"Running: {mc_cmd}")
                
                result = subprocess.run(
                    mc_cmd, 
                    shell=True, 
                    stdout=subprocess.PIPE, 
                    stderr=subprocess.PIPE
                )
                
                if result.returncode == 0:
                    logger.info(f"Successfully downloaded {urn} to {output_file}")
                    download_success = True
                    break
                else:
                    logger.warning(f"MinIO download failed: {result.stderr.decode()}")
                    
                    alternate_services = ["minio", "region1", "region2"]
                    alternate_services.remove(minio_service)
                    
                    for alt_service in alternate_services:
                        alt_cmd = f"mc cp {alt_service}/{bucket}/{path} {output_file}"
                        logger.info(f"Trying alternate service: {alt_cmd}")
                        
                        alt_result = subprocess.run(
                            alt_cmd, 
                            shell=True, 
                            stdout=subprocess.PIPE, 
                            stderr=subprocess.PIPE
                        )
                        
                        if alt_result.returncode == 0:
                            logger.info(f"Successfully downloaded from alternate service {alt_service}")
                            download_success = True
                            break
                    
                    if download_success:
                        break
                        
                    time.sleep(2)  
            except Exception as e:
                logger.error(f"Error during download attempt {attempt+1}: {e}")
                time.sleep(2)
        
        if not download_success or not os.path.exists(output_file) or os.path.getsize(output_file) == 0:
            logger.info(f"Creating mock data of size {size_bytes} bytes")
            with open(output_file, 'wb') as f:
                chunk_size = min(10 * 1024 * 1024, size_bytes)  # 10MB or file size
                remaining_bytes = size_bytes
                
                while remaining_bytes > 0:
                    write_size = min(chunk_size, remaining_bytes)
                    f.write(os.urandom(write_size))
                    remaining_bytes -= write_size
                    
                    if size_bytes > 100 * 1024 * 1024 and remaining_bytes % (100 * 1024 * 1024) == 0:
                        logger.info(f"Created {size_bytes - remaining_bytes} of {size_bytes} bytes")
        
        if not os.path.exists(output_file):
            logger.error(f"Output file {output_file} does not exist after download/creation")
            return None
            
        if os.path.getsize(output_file) == 0:
            logger.error(f"Output file {output_file} is empty after download/creation")
            return None
        
        return str(output_file)

    def run(self):
        action = os.environ.get('WORKFLOW_ACTION', 'process')
        logger.info(f"Starting workflow action: {action}")
        
        try:
            result = self.process_with_real_data(action)
            
            self.metrics['end_time'] = time.time()
            self.metrics['total_duration'] = self.metrics['end_time'] - self.metrics['start_time']
            self.metrics['action_result'] = result
            
            metrics_dir = Path('/tmp/workload-metrics')
            metrics_dir.mkdir(exist_ok=True)
            
            metrics_file = metrics_dir / f"metrics_{self.pod_name}_{self.run_id}.json"
            with open(metrics_file, 'w') as f:
                json.dump(self.metrics, f, indent=2)
            
            logger.info(f"Saved metrics to {metrics_file}")
            logger.info(f"Workflow completed successfully")
            
            return True
        
        except Exception as e:
            logger.error(f"Error during workflow execution: {e}")
            return False

def main():
    parser = argparse.ArgumentParser(description='Data-aware workload simulator for benchmarks')
    parser.add_argument('--config', type=str, help='Path to configuration file')
    parser.add_argument('--workload-type', type=str, default=os.environ.get('WORKLOAD_TYPE', 'generic'),
                       help='Type of workload to simulate')
    parser.add_argument('--duration', type=int, help='Duration of the simulated workload in seconds')
    parser.add_argument('--intensity', type=str, 
                       choices=['very-low', 'low', 'medium', 'high', 'very-high'],
                       default=os.environ.get('WORKLOAD_INTENSITY', 'medium'),
                       help='Intensity of the workload')
    
    args = parser.parse_args()
    
    simulator = WorkloadSimulator(
        config_file=args.config,
        workload_type=args.workload_type,
        duration=args.duration,
        intensity=args.intensity
    )
    
    simulator.run()

if __name__ == '__main__':
    main()