from pypxlib import Table
from database import Database
import sys
import time
import os
import logging # Add logging

class DataProcessor:
	def __init__(self, config_details: dict, db: Database, status_dict: dict, category_assignments: dict = None):
		self.config_details = config_details
		self.db = db
		self.status = status_dict
		self.category_assignments = category_assignments or {} # Use the passed dictionary or an empty one
		self.log_file = './skipped_rows.log'
		# Check if this is the 'reloader' process in Flask debug mode
		self.is_reloader = os.environ.get('WERKZEUG_RUN_MAIN') == 'true'

		# Clear the log file only in the main process
		if not self.is_reloader:
			with open(self.log_file, 'w', encoding='utf-8') as f:
				f.write("Skipped rows log - Started at: " + time.strftime("%Y-%m-%d %H:%M:%S") + "\n")
				f.write("=" * 80 + "\n")

		# Configure logging to write *only* to the skipped_rows.log file
		# Remove the StreamHandler to prevent console output from the processor
		logging.basicConfig(
			level=logging.INFO,
			format='%(asctime)s - %(levelname)s - %(message)s',
			handlers=[
				logging.FileHandler(self.log_file, mode='a', encoding='utf-8'),
				# logging.StreamHandler() # Removed this line
			],
			force=True # This ensures the basicConfig is applied even if logging was configured elsewhere before
		)

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
				logging.warning(f"Could not count rows for {market_name}: {e}") # Use logging
				# print(f"Warning: Could not count rows for {market_name}: {e}") # Remove print
		return total_rows

	def _log_skipped_row(self, market_name: str, row_num: int, product_data: tuple, reason: str): # Fixed typo: was 'product_ tuple'
		"""Log a skipped row with all its data"""
		with open(self.log_file, 'a', encoding='utf-8') as f:
			f.write(f"Market: {market_name}, Row: {row_num}, Reason: {reason}\n")
			f.write(f"  Settlement: {product_data[0]}\n")
			f.write(f"  Market Name: {product_data[1]}\n")
			f.write(f"  Item Name: {product_data[2]}\n")
			f.write(f"  Item Code: {product_data[3]}\n")
			f.write(f"  Retail Price: {product_data[5]}\n")
			f.write("-" * 40 + "\n")
		logging.warning(f"Skipped row {row_num} in {market_name}: {reason}") # Use logging

	def paradox_to_sqlite(self):
		"""Convert Paradox database data to SQLite with required columns using batched inserts."""
		# If this is the reloader process, skip processing entirely
		if self.is_reloader:
			logging.info("Skipping data processing in reloader process.")
			print("INFO: Skipping data processing in reloader process (Flask debug mode).")
			return

		total_rows = 0
		skipped_rows = 0
		market_count = 0
		# Note: We now batch per market, so batch_size is not needed here as a loop variable.
		# current_batch will hold all valid rows for the *current* market.

		# Count total rows for accurate progress tracking
		logging.info("Counting total rows across all markets...")
		print("Counting total rows across all markets...") # Keep print for initial console feedback
		total_all_rows = self._count_total_rows()
		logging.info(f"Total rows to process: {total_all_rows}")
		print(f"Total rows to process: {total_all_rows}") # Keep print for console feedback

		if total_all_rows == 0:
			self._update_status("Error", 0, "No rows found to process")
			return

		processed_rows = 0
		last_percent = -1

		for market_name, market_info in self.config_details.items():
			market_count += 1
			self._update_status(market_name, 0, f"Starting market {market_count}/{len(self.config_details)}: {market_name}")
			logging.info(f"Processing market {market_count}/{len(self.config_details)}: {market_name}")
			print(f"\nProcessing market {market_count}/{len(self.config_details)}: {market_name}") # Keep print for market start

			try:
				# Open Paradox table
				table = Table(market_info['path_to_db'], encoding='windows-1251')
				# Count rows in this market
				market_rows = sum(1 for _ in table)
				table = Table(market_info['path_to_db'], encoding='windows-1251') # Reopen for iteration
				logging.info(f"Processing {market_rows} rows in {market_name}")
				print(f"Processing {market_rows} rows...") # Keep print for row count feedback

				# Initialize batch for this market
				current_batch = [] # List to hold the current batch of *valid* products for this market
				# Track (item_code, market_name) tuples inserted in this batch for potential category updates
				inserted_item_keys = []

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

					# Pre-validate row attributes
					missing_attributes = []
					if not hasattr(row, 'Item') or row.Item is None:
						missing_attributes.append('Item')
					if not hasattr(row, 'id') or row.id is None:
						missing_attributes.append('id')
					if not hasattr(row, 'ClientPrice') or row.ClientPrice is None:
						missing_attributes.append('ClientPrice')

					# Skip row if any required attributes are missing
					if missing_attributes:
						# Prepare product data for logging (some fields might be None)
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
						logging.warning(f"Row {row_num} in {market_name} missing attributes: {missing_attributes}. Skipping.")
						# print(f"\nWarning: Row {row_num} in market {market_name} missing attributes: {missing_attributes}. Skipping.")
						skipped_rows += 1
						continue # Skip to the next row

					# Attempt to prepare data for insertion, handling potential type errors
					try:
						# Prepare data for insertion - validation happens here
						item_name = str(row.Item) if row.Item is not None else ""
						item_code = str(row.id) if row.id is not None else ""
						client_price = float(row.ClientPrice) if row.ClientPrice is not None else 0.0 # Handle potential TypeError for non-numeric ClientPrice
						# Construct the market name as stored in the DB
						db_market_name = f"{market_info['name']} {market_info['address']}"

						# Determine initial category code: use restored one if available, otherwise None
						# Use the composite key (item_code, market_name) for lookup
						initial_category_code = self.category_assignments.get((item_code, db_market_name))

						product_data = (
							market_info['settlement'],
							db_market_name, # Use the constructed name
							item_name,
							item_code,
							initial_category_code, # Use the restored category code or None
							client_price,
							None
						)
						# Add product data to the current batch for this market
						current_batch.append(product_data)
						# Track the key (item_code, market_name) for this inserted item
						inserted_item_keys.append((item_code, db_market_name))

					except (ValueError, TypeError) as e:
						# This handles cases like float("some_non_numeric_string")
						product_data = (
							market_info['settlement'],
							f"{market_info['name']} {market_info['address']}",
							str(row.Item) if hasattr(row, 'Item') else None,
							str(row.id) if hasattr(row, 'id') else None,
							None,
							float(row.ClientPrice) if hasattr(row, 'ClientPrice') and row.ClientPrice is not None else None,
							None
						)
						self._log_skipped_row(market_name, row_num, product_data, f"Data format error during preparation: {e}")
						logging.warning(f"Invalid data format at row {row_num} in {market_name} during preparation: {e}. Skipping.")
						# print(f"\nWarning: Invalid data format at row {row_num} in market {market_name}: {e}. Skipping.")
						skipped_rows += 1
						continue # Continue to the next row, this one is invalid

				# --- End of Market Loop ---
				# Insert the batch containing all valid rows for this market
				logging.info(f"Inserting batch of {len(current_batch)} products from {market_name}...")
				success = self.db.insert_products_batch(current_batch)
				if success:
					total_rows += len(current_batch)
					logging.info(f"Successfully inserted batch of {len(current_batch)} products from {market_name}.")
					print(f"  -> Inserted {len(current_batch)} valid rows.")

					# --- Apply category assignments after batch insert for this market ---
					# Find which of the *inserted* (item_code, market_name) keys had a category assigned previously
					assignments_to_update = []
					for item_key in inserted_item_keys:
						 assigned_category = self.category_assignments.get(item_key)
						 if assigned_category:
							 # Append (category_code, item_code, market_name) tuple for the batch update
							 # item_key is a tuple (item_code, market_name)
							 assignments_to_update.append((assigned_category, item_key[0], item_key[1]))

					if assignments_to_update:
						logging.info(f"Updating categories for {len(assignments_to_update)} products in {market_name}...")
						self.db.update_categories_batch(assignments_to_update)
						logging.info(f"Categories updated for {market_name}.")

				else:
					# This should ideally not happen due to the exception being raised in insert_products_batch
					# If it does, it indicates a deeper problem
					error_msg = f"Failed to insert batch of {len(current_batch)} products from {market_name} (unexpected return value)."
					logging.error(error_msg)
					raise Exception(error_msg) # Raise an exception to halt processing

				# Clear the progress bar and print completion message for the market
				sys.stdout.write('\r\x1b[K')  # Clear the entire line
				logging.info(f"{market_name}: Completed - {market_rows} rows processed ({len(current_batch)} inserted, {market_rows - len(current_batch)} skipped)")
				print(f"{market_name}: Completed - {market_rows} rows processed ({len(current_batch)} inserted, {market_rows - len(current_batch)} skipped)") # Keep print for market completion

				# Update processed markets count
				self.status['processed_markets'] = market_count

			except Exception as e:
				error_msg = f"Critical error processing market {market_name} (or during batch insert): {e}"
				logging.critical(error_msg) # Use critical level for this
				print(f"\n{error_msg}")
				self.status['error'] = error_msg
				self.status['message'] = error_msg
				# Raise the exception to halt the entire process
				raise e
				# Do NOT continue with next market if there's a critical error in one market's batch

		# --- End of All Markets Loop ---
		# Rebuild the FTS index after all batch inserts are complete
		logging.info("Rebuilding FTS5 index...")
		print("\nRebuilding FTS5 index...")
		self.db.rebuild_fts_index()

		# Final status update
		sys.stdout.write('\r\x1b[K')  # Clear the entire line
		self._update_status("Complete", 100, f"Processing completed: {total_rows} rows inserted, {skipped_rows} rows skipped")
		logging.info(f"Paradox to SQLite conversion completed: {total_rows} rows inserted, {skipped_rows} rows skipped")
		print(f"\nParadox to SQLite conversion completed: {total_rows} rows inserted, {skipped_rows} rows skipped")
		print(f"Skipped rows logged to: {self.log_file}")

