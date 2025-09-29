import sqlite3
import logging # Add logging for better error tracking

class Database:
	def __init__(self, db_path: str):
		self.db_path = db_path
		self.connection = None

	def connect(self):
		"""Establish database connection"""
		self.connection = sqlite3.connect(self.db_path)
		self.connection.row_factory = sqlite3.Row
		return self.connection

	def close(self):
		"""Close database connection"""
		if self.connection:
			self.connection.close()

	def drop_tables(self):
		"""Drop all tables if they exist"""
		drop_tables_sql = [
			"DROP TABLE IF EXISTS products",
			"DROP TABLE IF EXISTS products_fts",
			"DROP TABLE IF EXISTS categories"
		]
		with self.connect() as conn:
			for sql in drop_tables_sql:
				conn.execute(sql)

	def create_tables(self):
		"""Create the products table with required structure and FTS5 index"""
		# Create main products table
		create_table_sql = """
		CREATE TABLE products (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			settlement TEXT NOT NULL,
			market_name TEXT NOT NULL,
			item_name TEXT NOT NULL,
			item_code TEXT NOT NULL,
			item_kzp_category_code TEXT,
			item_retail_price REAL,
			item_promotional_price REAL
		)
		"""
		# Create FTS5 virtual table for fast searching
		create_fts_sql = """
		CREATE VIRTUAL TABLE products_fts USING fts5(
			settlement,
			market_name,
			item_name,
			item_code,
			content='products',
			content_rowid='id'
		)
		"""
		# Create categories mapping table
		create_categories_sql = """
		CREATE TABLE IF NOT EXISTS categories (
			code TEXT PRIMARY KEY,
			name TEXT NOT NULL
		)
		"""
		with self.connect() as conn:
			conn.execute(create_table_sql)
			conn.execute(create_fts_sql)
			conn.execute(create_categories_sql)
			# Populate FTS table with existing data (will be empty initially)
			conn.execute("INSERT INTO products_fts(products_fts) VALUES('rebuild')")

	def insert_product(self, product_data: tuple) -> bool:
		"""Insert a single product into the database"""
		insert_sql = """
		INSERT INTO products
		(settlement, market_name, item_name, item_code, item_kzp_category_code, item_retail_price, item_promotional_price)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		"""
		try:
			with self.connect() as conn:
				cursor = conn.execute(insert_sql, product_data)
				product_id = cursor.lastrowid
				# Also insert into FTS table - This is inefficient for batch operations.
				# FTS rebuild will be triggered after all inserts are done.
				# fts_sql = """
				# INSERT INTO products_fts (rowid, settlement, market_name, item_name, item_code)
				# VALUES (?, ?, ?, ?, ?)
				# """
				# conn.execute(fts_sql, (product_id, product_data[0], product_data[1], product_data[2], product_data[3]))
				return True
		except sqlite3.Error as e:
			logging.error(f"Error inserting single product: {e}") # Use logging
			print(f"Error inserting product: {e}")
			return False

	def insert_products_batch(self, products_data: list) -> bool:
		"""Insert multiple products into the database in a single transaction."""
		if not products_data: # Fixed typo: was 'products_'
			logging.info("No products to insert for this batch.")
			return True # Nothing to insert, consider it successful

		insert_sql = """
		INSERT INTO products
		(settlement, market_name, item_name, item_code, item_kzp_category_code, item_retail_price, item_promotional_price)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		"""
		try:
			with self.connect() as conn:
				# Begin an explicit transaction for the batch
				# conn.execute("BEGIN TRANSACTION") # Not strictly necessary if using 'with', but good practice
				conn.executemany(insert_sql, products_data)
				# Commit the batch transaction
				conn.commit()
				logging.info(f"Successfully inserted batch of {len(products_data)} products.") # Log success, fixed typo
				return True
		except sqlite3.Error as e:
			logging.error(f"Error inserting batch of {len(products_data)} products: {e}") # Use logging, fixed typo
			print(f"Error inserting batch of {len(products_data)} products: {e}")
			# Raise the exception to signal failure to the caller
			raise e # Re-raise the exception to halt processing

	def rebuild_fts_index(self):
		"""Rebuild the FTS5 index after bulk inserts."""
		rebuild_sql = "INSERT INTO products_fts(products_fts) VALUES('rebuild')"
		try:
			with self.connect() as conn:
				conn.execute(rebuild_sql)
				logging.info("FTS5 index rebuilt successfully after batch inserts.")
				return True
		except sqlite3.Error as e:
			logging.error(f"Error rebuilding FTS5 index: {e}")
			print(f"Error rebuilding FTS5 index: {e}")
			return False


	def search_products(self, search_term: str) -> list:
		"""Search products using FTS5 across multiple fields with flexible matching"""
		if not search_term.strip():
			# Return empty list when no search term to show prompt message
			return []
		# Split search term into individual words
		search_words = search_term.strip().split()
		if not search_words:
			return []
		# Build FTS5 query that searches each word independently
		# Using NEAR operator to allow words to appear in any order with proximity
		fts_conditions = []
		for word in search_words:
			if word:  # Skip empty words
				# Search for the word as prefix in any field
				fts_conditions.append(f'"{word}"*')
		if not fts_conditions:
			return []
		# Join with NEAR operator to allow flexible ordering
		# NEAR allows words to appear in any order within a reasonable distance
		fts_query = ' NEAR('.join(fts_conditions) + ')' * (len(fts_conditions) - 1)
		search_sql = """
		SELECT p.*
		FROM products p
		JOIN products_fts fts ON p.id = fts.rowid
		WHERE products_fts MATCH ?
		ORDER BY rank, market_name, item_name
		"""
		try:
			with self.connect() as conn:
				cursor = conn.execute(search_sql, (fts_query,))
				return cursor.fetchall()
		except sqlite3.Error as e:
			print(f"Error searching products: {e}")
			# Fallback to simple search if NEAR query fails
			try:
				# Try with simple AND search
				simple_query = ' AND '.join([f'"{word}"*' for word in search_words if word])
				cursor = conn.execute(search_sql, (simple_query,))
				return cursor.fetchall()
			except:
				return []

	def get_all_products(self) -> list:
		"""Get all products from the database"""
		select_sql = "SELECT * FROM products ORDER BY market_name, item_name"
		try:
			with self.connect() as conn:
				cursor = conn.execute(select_sql)
				return cursor.fetchall()
		except sqlite3.Error as e:
			print(f"Error getting all products: {e}")
			return []

	def update_product_category(self, product_ids: list, category_code: str) -> bool:
		"""Update the category for multiple products"""
		if not product_ids:
			return True
		placeholders = ','.join('?' * len(product_ids))
		update_sql = f"""
		UPDATE products
		SET item_kzp_category_code = ?
		WHERE id IN ({placeholders})
		"""
		try:
			with self.connect() as conn:
				conn.execute(update_sql, [category_code] + product_ids)
				return True
		except sqlite3.Error as e:
			print(f"Error updating product categories: {e}")
			return False

	def get_products_by_category(self, category_code: str = None) -> list:
		"""Get products filtered by category"""
		if category_code:
			select_sql = "SELECT * FROM products WHERE item_kzp_category_code = ? ORDER BY market_name, item_name"
			params = [category_code]
		else:
			select_sql = "SELECT * FROM products WHERE item_kzp_category_code IS NOT NULL ORDER BY market_name, item_name"
			params = []
		try:
			with self.connect() as conn:
				cursor = conn.execute(select_sql, params)
				return cursor.fetchall()
		except sqlite3.Error as e:
			print(f"Error getting products by category: {e}")
			return []

	def get_category_name(self, category_code: str) -> str:
		"""Get category name by code"""
		if not category_code:
			return ""
		select_sql = "SELECT name FROM categories WHERE code = ?"
		try:
			with self.connect() as conn:
				cursor = conn.execute(select_sql, [category_code])
				result = cursor.fetchone()
				return result['name'] if result else ""
		except sqlite3.Error as e:
			print(f"Error getting category name: {e}")
			return ""

	def save_category_mapping(self, categories: dict):
		"""Save category code to name mapping"""
		insert_sql = "INSERT OR REPLACE INTO categories (code, name) VALUES (?, ?)"
		try:
			with self.connect() as conn:
				for code, name in categories.items():
					conn.execute(insert_sql, [code, name])
		except sqlite3.Error as e:
			print(f"Error saving category mapping: {e}")

