from pypxlib import Table
from database import Database
import sys
import time
import os

class DataProcessor:
	def __init__(self, config_details: dict, db: Database, status_dict: dict):
		self.config_details = config_details
		self.db = db
		self.status = status_dict
		self.log_file = './skipped_rows.log'
		
		# Clear the log file at startup
		with open(self.log_file, 'w', encoding='utf-8') as f:
			f.write("Skipped rows log - Started at: " + time.strftime("%Y-%m-%d %H:%M:%S") + "\n")
			f.write("=" * 80 + "\n")
		
	def _update_status(self, market_name: str, progress: int, message: str):
		"""Update the global processing status"""
		self.status['current_market'] = market_name
		self.status['progress'] = progress
		self.status['message'] = message
		
	def _count_total_rows(self):
		"""Count total rows across all markets for progress tracking"""
		total_rows = 0
		for market_name, market_info in self.config_details.items():
			try:
				table = Table(market_info['path_to_db'], encoding='windows-1251')
				total_rows += sum(1 for _ in table)
			except Exception as e:
				print(f"Warning: Could not count rows for {market_name}: {e}")
		return total_rows
		
	def _log_skipped_row(self, market_name: str, row_num: int, product_data: tuple, reason: str):
		"""Log a skipped row with all its data"""
		with open(self.log_file, 'a', encoding='utf-8') as f:
			f.write(f"Market: {market_name}, Row: {row_num}, Reason: {reason}\n")
			f.write(f"  Settlement: {product_data[0]}\n")
			f.write(f"  Market Name: {product_data[1]}\n")
			f.write(f"  Item Name: {product_data[2]}\n")
			f.write(f"  Item Code: {product_data[3]}\n")
			f.write(f"  Retail Price: {product_data[5]}\n")
			f.write("-" * 40 + "\n")
		
	def paradox_to_sqlite(self):
		"""Convert Paradox database data to SQLite with required columns"""
		total_rows = 0
		skipped_rows = 0
		market_count = 0
		
		# Count total rows for accurate progress tracking
		print("Counting total rows across all markets...")
		total_all_rows = self._count_total_rows()
		print(f"Total rows to process: {total_all_rows}")
		
		if total_all_rows == 0:
			self._update_status("Error", 0, "No rows found to process")
			return
			
		processed_rows = 0
		last_percent = -1
		
		for market_name, market_info in self.config_details.items():
			market_count += 1
			
			self._update_status(market_name, 0, f"Starting market {market_count}/{len(self.config_details)}: {market_name}")
			print(f"\nProcessing market {market_count}/{len(self.config_details)}: {market_name}")
			
			try:
				# Open Paradox table
				table = Table(market_info['path_to_db'], encoding='windows-1251')
				
				# Count rows in this market
				market_rows = sum(1 for _ in table)
				table = Table(market_info['path_to_db'], encoding='windows-1251')  # Reopen for iteration
				
				print(f"Processing {market_rows} rows...")
				
				# Process each row
				for row_num, row in enumerate(table, 1):
					processed_rows += 1
					progress = int((processed_rows / total_all_rows) * 100)
					
					# Update status every 100 rows or on last row
					if row_num % 100 == 0 or row_num == market_rows:
						self._update_status(
							market_name, 
							progress, 
							f"Market {market_count}/{len(self.config_details)}: {market_name} ({row_num}/{market_rows} rows)"
						)
					
					# Update terminal progress bar only when percentage changes
					if progress != last_percent:
						last_percent = progress
						bar_length = 40
						filled_length = int(bar_length * processed_rows // total_all_rows)
						bar = '█' * filled_length + '░' * (bar_length - filled_length)
						# Clear the line and write the progress bar
						sys.stdout.write('\r\x1b[K')  # Clear the entire line
						sys.stdout.write(f'Overall Progress: [{bar}] {progress}% ({processed_rows}/{total_all_rows} rows)')
						sys.stdout.flush()
					
					# Check for missing attributes and log warnings
					missing_attributes = []
					
					if not hasattr(row, 'Item') or row.Item is None:
						missing_attributes.append('Item')
					
					if not hasattr(row, 'id') or row.id is None:
						missing_attributes.append('id')
					
					if not hasattr(row, 'ClientPrice') or row.ClientPrice is None:
						missing_attributes.append('ClientPrice')
					
					# Skip row if any required attributes are missing
					if missing_attributes:
						# Prepare product data for logging
						product_data = (
							market_info['settlement'],
							f"{market_info['name']} {market_info['address']}",
							str(row.Item) if hasattr(row, 'Item') and row.Item is not None else None,
							str(row.id) if hasattr(row, 'id') and row.id is not None else None,
							None,
							float(row.ClientPrice) if hasattr(row, 'ClientPrice') and row.ClientPrice is not None else 0.0,
							None
						)
						self._log_skipped_row(market_name, row_num, product_data, f"Missing attributes: {missing_attributes}")
						print(f"\nWarning: Row {row_num} in market {market_name} missing attributes: {missing_attributes}. Skipping.")
						skipped_rows += 1
						continue
					
					try:
						# Prepare data for insertion
						item_name = str(row.Item) if row.Item is not None else ""
						item_code = str(row.id) if row.id is not None else ""
						client_price = float(row.ClientPrice) if row.ClientPrice is not None else 0.0
						
						product_data = (
							market_info['settlement'],
							f"{market_info['name']} {market_info['address']}",
							item_name,
							item_code,
							None,  # item_kzp_category_code (will be set via UI)
							client_price,
							None  # item_promotional_price
						)
						
						# Insert into SQLite database
						success = self.db.insert_product(product_data)
						if not success:
							self._log_skipped_row(market_name, row_num, product_data, "Database insertion failed")
							print(f"\nWarning: Failed to insert product at row {row_num}: {product_data}")
							skipped_rows += 1
						else:
							total_rows += 1
							
					except (ValueError, TypeError) as e:
						product_data = (
							market_info['settlement'],
							f"{market_info['name']} {market_info['address']}",
							str(row.Item) if hasattr(row, 'Item') else None,
							str(row.id) if hasattr(row, 'id') else None,
							None,
							float(row.ClientPrice) if hasattr(row, 'ClientPrice') else None,
							None
						)
						self._log_skipped_row(market_name, row_num, product_data, f"Data format error: {e}")
						print(f"\nWarning: Invalid data format at row {row_num} in market {market_name}: {e}. Skipping.")
						skipped_rows += 1
						continue
				
				# Clear the progress bar and print completion message
				sys.stdout.write('\r\x1b[K')  # Clear the entire line
				print(f"{market_name}: Completed - {market_rows} rows processed")
				
				# Update processed markets count
				self.status['processed_markets'] = market_count
						
			except Exception as e:
				error_msg = f"Error processing market {market_name}: {e}"
				print(f"\n{error_msg}")
				self.status['error'] = error_msg
				# Continue with next market instead of stopping
				continue
				
		# Final status update
		sys.stdout.write('\r\x1b[K')  # Clear the entire line
		self._update_status("Complete", 100, f"Processing completed: {total_rows} rows inserted, {skipped_rows} rows skipped")
		print(f"Paradox to SQLite conversion completed: {total_rows} rows inserted, {skipped_rows} rows skipped")
		print(f"Skipped rows logged to: {self.log_file}")
