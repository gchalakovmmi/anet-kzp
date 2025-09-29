import yaml

class Config:
    def __init__(self, file_path):
        self.file_path = file_path
        self._markets = []
        self.processing_config = {}
        self._load_config()
    
    def _load_config(self):
        """Load configuration from YAML file"""
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                
                # Load processing configuration
                self.processing_config = config.get('processing', {})
                
                # Load markets list
                self._markets = config.get('markets', [])
                
                # Validate that we have markets
                if not self._markets:
                    raise Exception("No markets defined in configuration file")
                    
        except FileNotFoundError:
            raise Exception(f"Configuration file not found: {self.file_path}")
        except yaml.YAMLError as e:
            raise Exception(f"Error parsing configuration file: {e}")
    
    def get_markets(self):
        """Get list of market configurations"""
        return self._markets
    
    def get_processing_config(self):
        """Get processing configuration"""
        return self.processing_config
