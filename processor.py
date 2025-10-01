from pypxlib import Table
from database import Database
import sys
import time
import os
import logging
import shutil
from datetime import datetime

class DataProcessor:
	def __init__(self, markets: list, db: Database, status_dict: dict, category_assignments: dict = None):
		self.markets = markets
		self.db = db
		self.status = status_dict
		self.category_assignments = category_assignments or {}
		self.log_file = './skipped_rows.log'
		with open(self.log_file, 'w', encoding='utf-8') as f:
			f.write("Skipped rows log - Started at: " + time.strftime("%Y-%m-%d %H:%M:%S") + "\n")
			f.write("=" * 80 + "\n")
		logging.basicConfig(
			level=logging.INFO,
			format='%(asctime)s - %(levelname)s - %(message)s',
			handlers=[
				logging.FileHandler(self.log_file, mode='a', encoding='utf-8'),
			],
			force=True
		)

	def _create_backup(self):
		"""Create a backup of the current SQLite database - MANDATORY first step"""
		backup_dir = './backup'
		current_db_path = './products.sqlite'
		
		self._update_status("Backup", 0, "Starting database backup...")
		logging.info("Starting database backup process")
		
		# Create backup directory if it doesn't exist
		if not os.path.exists(backup_dir):
			os.makedirs(backup_dir)
			logging.info(f"Created backup directory: {backup_dir}")
		
		# Check if source database exists
		if not os.path.exists(current_db_path):
			error_msg = f"Cannot create backup: Source database not found: {current_db_path}"
			logging.error(error_msg)
			self._update_status("Backup Failed", 0, error_msg)
			return None
		
		# Generate backup filename with date and incrementing ID
		today = datetime.now().strftime('%Y%m%d_%H%M%S')
		backup_pattern = f"products_{today}_"
		
		# Find existing backups for this exact timestamp to determine next ID
		existing_backups = []
		for filename in os.listdir(backup_dir):
			if filename.startswith(backup_pattern) and filename.endswith('.sqlite'):
				try:
					# Extract the ID from filename: products_YYYYMMDD_HHMMSS_ID.sqlite
					id_part = filename.replace(backup_pattern, '').replace('.sqlite', '')
					backup_id = int(id_part)
					existing_backups.append(backup_id)
				except ValueError:
					continue
		
		# Determine next backup ID
		next_id = 1
		if existing_backups:
			next_id = max(existing_backups) + 1
		
		# Create backup filename
		backup_filename = f"products_{today}_{next_id}.sqlite"
		backup_path = os.path.join(backup_dir, backup_filename)
		
		try:
			# Copy the database file
			self._update_status("Backup", 0, f"Creating backup: {backup_filename}")
			shutil.copy2(current_db_path, backup_path)
			success_msg = f"Database backup created successfully: {backup_path}"
			logging.info(success_msg)
			self._update_status("Backup Complete", 0, success_msg)
			return backup_path
		except Exception as e:
			error_msg = f"Failed to create database backup: {e}"
			logging.error(error_msg)
			self._update_status("Backup Failed", 0, error_msg)
			return None

	def _update_status(self, market_name: str, progress: int, message: str):
		"""Update the global processing status"""
		self.status['current_market'] = market_name
		self.status['progress'] = progress
		self.status['message'] = message

	def _count_total_rows(self):
		"""Count total rows across all markets for progress tracking"""
		total_rows = 0
		for market_info in self.markets:
			try:
				table = Table(market_info['path_to_db'], encoding='windows-1251')
				total_rows += sum(1 for _ in table)
			except Exception as e:
				logging.warning(f"Could not count rows for {market_info['name']}: {e}")
		return total_rows

	def _log_skipped_row(self, market_name: str, row_num: int, product_data: tuple, reason: str):
		"""Log a skipped row with all its data"""
		with open(self.log_file, 'a', encoding='utf-8') as f:
			f.write(f"Market: {market_name}, Row: {row_num}, Reason: {reason}\n")
			f.write(f"  Settlement: {product_data[0]}\n")
			f.write(f"  Market Name: {product_data[1]}\n")
			f.write(f"  Item Name: {product_data[2]}\n")
			f.write(f"  Item Code: {product_data[3]}\n")
			f.write(f"  Retail Price: {product_data[4]}\n")
			f.write("-" * 40 + "\n")
		logging.warning(f"Skipped row {row_num} in {market_name}: {reason}")

	def paradox_to_sqlite(self):
		"""Convert Paradox database data to SQLite with persistent categories - WITH MANDATORY BACKUP"""
		
		# MANDATORY FIRST STEP: Create backup
		backup_path = self._create_backup()
		if not backup_path:
			# If backup fails, stop processing immediately
			error_msg = "Processing stopped: Database backup failed"
			self.status['error'] = error_msg
			self.status['message'] = error_msg
			logging.error("PROCESSING STOPPED: Backup creation failed")
			return

		# Continue with processing only if backup was successful
		total_rows = 0
		skipped_rows = 0
		market_count = 0
		
		logging.info("Counting total rows across all markets...")
		print("Counting total rows across all markets...")
		total_all_rows = self._count_total_rows()
		logging.info(f"Total rows to process: {total_all_rows}")
		print(f"Total rows to process: {total_all_rows}")
		
		if total_all_rows == 0:
			self._update_status("Error", 0, "No rows found to process")
			return

		processed_rows = 0
		last_percent = -1
		
		for market_info in self.markets:
			market_count += 1
			market_name = market_info['name']
			
			# Update status at the start of each market with current progress
			current_progress = int((processed_rows / total_all_rows) * 100)
			self._update_status(market_name, current_progress, f"Starting market {market_count}/{len(self.markets)}: {market_name}")
			logging.info(f"Processing market {market_count}/{len(self.markets)}: {market_name}")
			print(f"\nProcessing market {market_count}/{len(self.markets)}: {market_name}")
			
			try:
				table = Table(market_info['path_to_db'], encoding='windows-1251')
				market_rows = sum(1 for _ in table)
				table = Table(market_info['path_to_db'], encoding='windows-1251')
				logging.info(f"Processing {market_rows} rows in {market_name}")
				print(f"Processing {market_rows} rows...")
				
				current_batch = []
				inserted_item_keys = []
				
				for row_num, row in enumerate(table, 1):
					processed_rows += 1
					progress = int((processed_rows / total_all_rows) * 100)
					
					# Update status more frequently for better progress tracking
					if row_num % 50 == 0 or row_num == market_rows or progress != last_percent:
						self._update_status(
							market_name,
							progress,
							f"Market {market_count}/{len(self.markets)}: {market_name} ({row_num}/{market_rows} rows)"
						)
					
					if progress != last_percent:
						last_percent = progress
						bar_length = 40
						filled_length = int(bar_length * processed_rows // total_all_rows)
						bar = '█' * filled_length + '░' * (bar_length - filled_length)
						sys.stdout.write('\r\x1b[K')
						sys.stdout.write(f'Overall Progress: [{bar}] {progress}% ({processed_rows}/{total_all_rows} rows)')
						sys.stdout.flush()

					# Check Act column - skip if not equal to '*'
					if hasattr(row, 'Act') and row.Act is not None and row.Act != '*':
						product_data = (
							market_info['settlement'],
							f"{market_info['name']} {market_info['address']}",
							str(row.Item) if hasattr(row, 'Item') and row.Item is not None else None,
							str(row.id) if hasattr(row, 'id') and row.id is not None else None,
							float(row.ClientPrice) if hasattr(row, 'ClientPrice') and row.ClientPrice is not None else 0.0,
							None
						)
						self._log_skipped_row(market_name, row_num, product_data, f"Act column not equal to '*' (value: {row.Act})")
						logging.info(f"Row {row_num} in {market_name} skipped due to Act column value: {row.Act}")
						skipped_rows += 1
						continue

					# Pre-validate row attributes
					missing_attributes = []
					if not hasattr(row, 'Item') or row.Item is None:
						missing_attributes.append('Item')
					if not hasattr(row, 'id') or row.id is None:
						missing_attributes.append('id')
					if not hasattr(row, 'ClientPrice') or row.ClientPrice is None:
						missing_attributes.append('ClientPrice')
					
					if missing_attributes:
						product_data = (
							market_info['settlement'],
							f"{market_info['name']} {market_info['address']}",
							str(row.Item) if hasattr(row, 'Item') and row.Item is not None else None,
							str(row.id) if hasattr(row, 'id') and row.id is not None else None,
							float(row.ClientPrice) if hasattr(row, 'ClientPrice') and row.ClientPrice is not None else 0.0,
							None
						)
						self._log_skipped_row(market_name, row_num, product_data, f"Missing attributes: {missing_attributes}")
						logging.warning(f"Row {row_num} in {market_name} missing attributes: {missing_attributes}. Skipping.")
						skipped_rows += 1
						continue

					try:
						item_name = str(row.Item) if row.Item is not None else ""
						item_code = str(row.id) if row.id is not None else ""
						client_price = float(row.ClientPrice) if row.ClientPrice is not None else 0.0
						db_market_name = f"{market_info['name']} {market_info['address']}"
						
						# Updated product data structure without category code
						product_data = (
							market_info['settlement'],
							db_market_name,
							item_name,
							item_code,
							client_price,
							None  # promotional_price
						)
						current_batch.append(product_data)
						inserted_item_keys.append((db_market_name, item_code))
					except (ValueError, TypeError) as e:
						product_data = (
							market_info['settlement'],
							f"{market_info['name']} {market_info['address']}",
							str(row.Item) if hasattr(row, 'Item') else None,
							str(row.id) if hasattr(row, 'id') else None,
							float(row.ClientPrice) if hasattr(row, 'ClientPrice') and row.ClientPrice is not None else None,
							None
						)
						self._log_skipped_row(market_name, row_num, product_data, f"Data format error during preparation: {e}")
						logging.warning(f"Invalid data format at row {row_num} in {market_name} during preparation: {e}. Skipping.")
						skipped_rows += 1
						continue

				# Insert batch for current market
				logging.info(f"Inserting batch of {len(current_batch)} products from {market_name}...")
				success = self.db.insert_products_batch(current_batch)
				if success:
					total_rows += len(current_batch)
					logging.info(f"Successfully inserted batch of {len(current_batch)} products from {market_name}.")
					print(f"  -> Inserted {len(current_batch)} valid rows.")
					
					# Apply category assignments after batch insert
					assignments_to_update = []
					for item_key in inserted_item_keys:
						assigned_category = self.category_assignments.get(item_key)
						if assigned_category:
							assignments_to_update.append((assigned_category, item_key[0], item_key[1]))
					
					if assignments_to_update:
						logging.info(f"Updating categories for {len(assignments_to_update)} products in {market_name}...")
						self.db.update_categories_batch(assignments_to_update)
						logging.info(f"Categories updated for {market_name}.")
				else:
					error_msg = f"Failed to insert batch of {len(current_batch)} products from {market_name}."
					logging.error(error_msg)
					raise Exception(error_msg)
				
				sys.stdout.write('\r\x1b[K')
				logging.info(f"{market_name}: Completed - {market_rows} rows processed ({len(current_batch)} inserted, {market_rows - len(current_batch)} skipped)")
				print(f"{market_name}: Completed - {market_rows} rows processed ({len(current_batch)} inserted, {market_rows - len(current_batch)} skipped)")
				self.status['processed_markets'] = market_count
				
			except Exception as e:
				error_msg = f"Critical error processing market {market_name}: {e}"
				logging.critical(error_msg)
				print(f"\n{error_msg}")
				self.status['error'] = error_msg
				self.status['message'] = error_msg
				raise e

		# Clean up orphaned categories after all processing
		logging.info("Cleaning up orphaned category assignments...")
		orphaned_count = self.db.cleanup_orphaned_categories()
		print(f"Cleaned up {orphaned_count} orphaned category assignments.")
		
		# Rebuild FTS index
		logging.info("Rebuilding FTS5 index...")
		print("\nRebuilding FTS5 index...")
		self.db.rebuild_fts_index()
		
		# Final status update
		sys.stdout.write('\r\x1b[K')
		success_msg = f"Processing completed successfully! {total_rows} rows inserted, {skipped_rows} rows skipped"
		self._update_status("Complete", 100, success_msg)
		logging.info(f"Paradox to SQLite conversion completed: {total_rows} rows inserted, {skipped_rows} rows skipped")
		print(f"\nParadox to SQLite conversion completed: {total_rows} rows inserted, {skipped_rows} rows skipped")
		print(f"Skipped rows logged to: {self.log_file}")
