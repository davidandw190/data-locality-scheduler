import yaml
import logging
from pathlib import Path

logger = logging.getLogger("configuration-manager")

class ConfigurationManager:
    
    def __init__(self, config_file):
        self.config_file = Path(config_file)
        self.config = self._load_config()
    
    def _load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                config = yaml.safe_load(f)
            logger.info(f"Loaded configuration from {self.config_file}")
            return config
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise
    
    def get_config(self):
        return self.config
    
    def get_namespace(self):
        return self.config.get('kubernetes', {}).get('namespace', 'scheduler-benchmark')
    
    def should_setup_storage(self):
        return self.config.get('kubernetes', {}).get('setup_storage', True)
    
    def get_workloads(self):
        return self.config.get('workloads', [])
    
    def get_schedulers(self):
        return self.config.get('schedulers', [])
    
    def get_execution_config(self):
        return self.config.get('execution', {})
    
    def get_metrics_config(self):
        return self.config.get('metrics', {})