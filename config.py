import yaml

class Config:
	def __init__(self, file_path):
		self.file_path = file_path
		self._details = self._load_config()

	def _load_config(self):
		"""Load configuration from YAML file"""
		try:
			with open(self.file_path, 'r', encoding='utf-8') as f:
				return yaml.safe_load(f)
		except FileNotFoundError:
			raise Exception(f"Configuration file not found: {self.file_path}")
		except yaml.YAMLError as e:
			raise Exception(f"Error parsing configuration file: {e}")

	def get_details(self):
		"""Get configuration details"""
		return self._details
