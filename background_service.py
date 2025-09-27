"""
Background Service for Automatic Data Processing
This runs independently of the desktop app and handles scheduled processing.
"""

import time
import schedule
import logging
from config import Config
from database import Database
from processor import DataProcessor

# Set up logging
logging.basicConfig(
	level=logging.INFO,
	format='%(asctime)s - %(levelname)s - %(message)s',
	handlers=[
		logging.FileHandler('background_service.log'),
		logging.StreamHandler()
	]
)

logger = logging.getLogger(__name__)

class BackgroundService:
	def __init__(self):
		self.db = Database('./products.sqlite')
		self.processing = False
		
	def initialize_database(self):
		"""Initialize database and category mapping"""
		CATEGORIES = {
			'1': '1. Бял хляб от 500 гр. до 1 кг',
			'2': '2. Хляб Добруджа от 500 гр. до 1 кг',
			# ... (include all categories from app.py)
		}
		self.db.create_tables_if_not_exist()
		self.db.save_category_mapping(CATEGORIES)
		
	def process_data(self):
		"""Process data - called by scheduler"""
		if self.processing:
			logger.info("⏸️ Processing already in progress, skipping")
			return
			
		self.processing = True
		logger.info("🔄 Starting automatic data processing...")
		
		try:
			config = Config('./config.yaml')
			markets = config.get_markets()
			
			# Create a simple status dictionary for the processor
			status_dict = {
				'is_processing': True,
				'current_market': '',
				'progress': 0,
				'total_markets': 0,
				'processed_markets': 0,
				'message': 'Starting...',
				'error': None
			}
			
			# Process data
			processor = DataProcessor(markets, self.db, status_dict)
			processor.paradox_to_sqlite()
			
			logger.info("✅ Data processing completed successfully!")
			
		except Exception as e:
			logger.error(f"❌ Error during processing: {e}")
		finally:
			self.processing = False
	
	def check_automatic_processing(self):
		"""Check if we should process automatically based on config"""
		try:
			config = Config('./config.yaml')
			if config.should_process_automatically():
				logger.info("🕒 Automatic processing triggered by schedule")
				self.process_data()
			else:
				auto_setting = config.get_automatic_processing()
				if isinstance(auto_setting, str):
					logger.debug(f"⏰ Next automatic processing scheduled for: {auto_setting}")
		except Exception as e:
			logger.error(f"❌ Error in automatic processing check: {e}")
	
	def run(self):
		"""Run the background service"""
		logger.info("🚀 Starting background service...")
		self.initialize_database()
		
		# Schedule the job to run every minute
		schedule.every(1).minutes.do(self.check_automatic_processing)
		
		# Run initial check
		self.check_automatic_processing()
		
		logger.info("📅 Background service scheduler started")
		logger.info("💡 Service will run automatically based on config.yaml schedule")
		logger.info("⏰ Current automatic_processing setting will be checked every minute")
		
		# Keep the service running
		try:
			while True:
				schedule.run_pending()
				time.sleep(30)  # Check every 30 seconds
		except KeyboardInterrupt:
			logger.info("👋 Background service stopped by user")
		except Exception as e:
			logger.error(f"💥 Unexpected error in background service: {e}")

if __name__ == "__main__":
	service = BackgroundService()
	service.run()
