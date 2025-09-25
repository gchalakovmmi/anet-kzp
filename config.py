import yaml
from datetime import datetime, time
import re

class Config:
	def __init__(self, file_path):
		self.file_path = file_path
		self._config = self._load_config()

	def _load_config(self):
		"""Load configuration from YAML file"""
		try:
			with open(self.file_path, 'r', encoding='utf-8') as f:
				return yaml.safe_load(f)
		except FileNotFoundError:
			raise Exception(f"Configuration file not found: {self.file_path}")
		except yaml.YAMLError as e:
			raise Exception(f"Error parsing configuration file: {e}")

	def get_markets(self):
		"""Get markets configuration"""
		return self._config.get('markets', [])

	def get_automatic_processing(self):
		"""Get automatic processing setting"""
		return self._config.get('automatic_processing', False)

	def should_process_automatically(self):
		"""Check if we should process automatically based on time setting"""
		auto_setting = self.get_automatic_processing()
		
		if auto_setting is False:
			return False
		elif auto_setting is True:
			return True
		elif isinstance(auto_setting, str):
			# Parse time string like "23:31"
			time_match = re.match(r'(\d{1,2}):(\d{2})', auto_setting)
			if time_match:
				target_hour = int(time_match.group(1))
				target_minute = int(time_match.group(2))
				current_time = datetime.now()
				target_time = current_time.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
				
				# Check if current time is within 2 minutes of target time
				time_diff = abs((current_time - target_time).total_seconds())
				return time_diff <= 120  # 2 minute window
				
		return False
