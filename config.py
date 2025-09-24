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
			# Parse time string like "02:00"
			time_match = re.match(r'(\d{1,2}):(\d{2})', auto_setting)
			if time_match:
				target_hour = int(time_match.group(1))
				target_minute = int(time_match.group(2))
				current_time = datetime.now().time()
				target_time = time(target_hour, target_minute)
				
				# Check if current time is after target time (within 1 hour window)
				if current_time >= target_time:
					delta = datetime.combine(datetime.today(), current_time) - datetime.combine(datetime.today(), target_time)
					return delta.total_seconds() <= 3600  # 1 hour window
					
		return False
