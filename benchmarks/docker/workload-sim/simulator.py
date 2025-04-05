import argparse
import json
import logging
import os
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
    """Base class for simulating different types of workloads"""
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
        """Parse data references from pod annotations"""
        data_refs = []
        for k, v in os.environ.items():
            if k.startswith(f"DATA_SCHEDULER_THESIS_{ref_type.upper()}_"):
                try:
                    parts = v.split(',')
                    if len(parts) >= 2:
                        urn = parts[0]
                        size_bytes = int(parts[1])
                        
                        # Optional parameters
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
        """Actually download data from MinIO or create mock data if it doesn't exist"""
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
                
                # Create mock data of appropriate size
                logger.info(f"Creating mock data of size {size_bytes} bytes")
                with open(output_file, 'wb') as f:
                    # Create chunks to avoid memory issues with large files
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
        
        # Determine which MinIO service to use based on bucket
        minio_service = "minio"  # Default to central
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
        """Process a workload action with real data access"""
        logger.info(f"Starting action: {action} with real data interaction")
        
        start_time = time.time()
        
        input_files = []
        for data_ref in self.input_data:
            for attempt in range(3):  
                try:
                    file_path = self._download_data(data_ref)
                    if file_path:
                        input_files.append(file_path)
                        break
                except Exception as e:
                    logger.warning(f"Download attempt {attempt+1} failed: {e}")
                    time.sleep(2) 
        
        logger.info(f"Processing {len(input_files)} input files")
        for i, file_path in enumerate(input_files):
            if i < len(self.input_data):
                logger.info(f"Input {i+1}: {self.input_data[i]['urn']} -> {file_path}")
        
        if action in ['extract', 'collect']:
            for file_path in input_files:
                file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
                logger.info(f"Extracting data from {file_path} ({file_size} bytes)")
                
                if file_size == 0:
                    logger.warning(f"Empty or non-existent file: {file_path}")
                    continue
                    
                with open(file_path, 'rb') as f:
                    chunk_size = 1024 * 1024  # 1MB
                    chunks_read = 0
                    while True:
                        data = f.read(chunk_size)
                        if not data:
                            break
                        checksum = sum(data)
                        chunks_read += 1
                        if chunks_read % 10 == 0:
                            logger.info(f"Processed {chunks_read} chunks ({chunks_read * chunk_size / 1024 / 1024:.2f} MB)")
                
                time.sleep(1) 
        
        elif action in ['transform', 'process', 'analyze']:
            # More CPU-intensive operations
            for file_path in input_files:
                file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
                logger.info(f"Processing data from {file_path} ({file_size} bytes)")
                
                if file_size == 0:
                    logger.warning(f"Empty or non-existent file: {file_path}")
                    continue
                    
                processing_start = time.time()
                with open(file_path, 'rb') as f:
                    chunk_size = 1024 * 1024 
                    chunks_read = 0
                    while True:
                        data = f.read(chunk_size)
                        if not data:
                            break
                        
                        for _ in range(100): 
                            _ = hash(data)
                        
                        chunks_read += 1
                        if chunks_read % 10 == 0:
                            logger.info(f"Processed {chunks_read} chunks ({chunks_read * chunk_size / 1024 / 1024:.2f} MB)")
                
                processing_time = time.time() - processing_start
                logger.info(f"Finished processing in {processing_time:.2f} seconds")
                
                intensity_multiplier = self._get_intensity_multiplier()
                computation_time = 2 * intensity_multiplier
                logger.info(f"Performing additional computation for {computation_time:.2f} seconds")
                
                computation_start = time.time()
                while time.time() - computation_start < computation_time:
                    _ = [i * i for i in range(10000)]
                
                time.sleep(1)
        
        uploaded_outputs = 0
        for data_ref in self.output_data:
            for attempt in range(3): 
                try:
                    success = self._upload_data(data_ref)
                    if success:
                        uploaded_outputs += 1
                        break
                    else:
                        logger.warning(f"Upload attempt {attempt+1} failed")
                        time.sleep(2) 
                except Exception as e:
                    logger.warning(f"Upload attempt {attempt+1} failed with exception: {e}")
                    time.sleep(2) 
        
        end_time = time.time()
        duration = end_time - start_time
        
        logger.info(f"Completed action: {action} in {duration:.2f} seconds")
        logger.info(f"Processed {len(input_files)} input files, generated {uploaded_outputs} output files")
        
        return {
            'action': action,
            'start_time': start_time,
            'end_time': end_time,
            'duration': duration,
            'input_files_processed': len(input_files),
            'output_files_generated': uploaded_outputs
        }

    def _download_data(self, data_reference):
        """Actually download data from MinIO or create mock data if it doesn't exist"""
        urn = data_reference['urn']
        size_bytes = data_reference['size_bytes']
        
        logger.info(f"Downloading data: {urn} ({size_bytes} bytes)")
        

        parts = urn.split('/', 1)
        bucket = parts[0]
        path = parts[1] if len(parts) > 1 else ""
        
        output_dir = Path('/tmp/data')
        output_dir.mkdir(exist_ok=True)
        output_file = output_dir / f"{bucket}_{path.replace('/', '_')}"
        
        # Determine which MinIO service to use based on bucket
        minio_service = "minio"  # Default to central
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
        """Run the workflow with real data processing"""
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