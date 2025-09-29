import sqlite3
import logging

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
		"""Drop products table but preserve product_categories"""
		drop_tables_sql = [
			"DROP TABLE IF EXISTS products",
			"DROP TABLE IF EXISTS products_fts"
			# Note: We don't drop product_categories to preserve category assignments
		]
		with self.connect() as conn:
			for sql in drop_tables_sql:
				conn.execute(sql)

	def create_tables(self):
		"""Create the products table with composite primary key and separate product_categories table"""
		# Create main products table with composite primary key AND an id column for FTS
		create_table_sql = """
		CREATE TABLE products (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			settlement TEXT NOT NULL,
			market_name TEXT NOT NULL,
			item_name TEXT NOT NULL,
			item_code TEXT NOT NULL,
			item_retail_price REAL,
			item_promotional_price REAL,
			UNIQUE(market_name, item_code)
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
		
		# Create persistent product categories table
		create_product_categories_sql = """
		CREATE TABLE IF NOT EXISTS product_categories (
			market_name TEXT NOT NULL,
			item_code TEXT NOT NULL,
			category_code TEXT NOT NULL,
			PRIMARY KEY (market_name, item_code),
			FOREIGN KEY (market_name, item_code) REFERENCES products(market_name, item_code) ON DELETE CASCADE
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
			conn.execute(create_product_categories_sql)
			conn.execute(create_categories_sql)
			
			# Enable foreign keys
			conn.execute("PRAGMA foreign_keys = ON")
			
			# Populate FTS table with existing data (will be empty initially)
			conn.execute("INSERT INTO products_fts(products_fts) VALUES('rebuild')")

	def get_current_categories(self) -> dict:
		"""
		Fetches current category assignments from product_categories table.
		Returns a dictionary mapping (market_name, item_code) tuple to category_code.
		"""
		select_sql = """
		SELECT market_name, item_code, category_code
		FROM product_categories
		WHERE category_code IS NOT NULL
		"""
		
		category_map = {}
		try:
			with self.connect() as conn:
				cursor = conn.execute(select_sql)
				rows = cursor.fetchall()
				for row in rows:
					key = (row['market_name'], row['item_code'])
					if row['category_code']:
						category_map[key] = row['category_code']
		except sqlite3.Error as e:
			logging.error(f"Error fetching current categories: {e}")
		
		return category_map

	def insert_products_batch(self, products_data: list) -> bool:
		"""Insert multiple products into the database in a single transaction."""
		if not products_data:
			logging.info("No products to insert for this batch.")
			return True
		
		insert_sql = """
		INSERT OR REPLACE INTO products
		(settlement, market_name, item_name, item_code, item_retail_price, item_promotional_price)
		VALUES (?, ?, ?, ?, ?, ?)
		"""
		
		try:
			with self.connect() as conn:
				conn.executemany(insert_sql, products_data)
				conn.commit()
				logging.info(f"Successfully inserted batch of {len(products_data)} products.")
				return True
		except sqlite3.Error as e:
			logging.error(f"Error inserting batch of {len(products_data)} products: {e}")
			raise e

	def update_categories_batch(self, category_assignments: list) -> bool:
		"""
		Updates the product_categories table for multiple products.
		:param category_assignments: A list of tuples (category_code, market_name, item_code).
		:return: True if successful, False otherwise.
		"""
		if not category_assignments:
			logging.info("No category assignments to update.")
			return True
		
		update_sql = """
		INSERT OR REPLACE INTO product_categories
		(market_name, item_code, category_code)
		VALUES (?, ?, ?)
		"""
		
		try:
			with self.connect() as conn:
				# Convert (category_code, market_name, item_code) to (market_name, item_code, category_code)
				assignments_for_db = [(market_name, item_code, category_code)
									for category_code, market_name, item_code in category_assignments]
				conn.executemany(update_sql, assignments_for_db)
				conn.commit()
				logging.info(f"Successfully updated categories for {len(category_assignments)} products.")
				return True
		except sqlite3.Error as e:
			logging.error(f"Error updating categories batch for {len(category_assignments)} products: {e}")
			raise e

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
			return False

	def search_products(self, search_term: str) -> list:
		"""Search products using FTS5 and join with product_categories to get category info"""
		if not search_term.strip():
			return []
		
		search_words = search_term.strip().split()
		if not search_words:
			return []
		
		fts_conditions = []
		for word in search_words:
			if word:
				fts_conditions.append(f'"{word}"*')
		
		if not fts_conditions:
			return []
		
		fts_query = ' NEAR('.join(fts_conditions) + ')' * (len(fts_conditions) - 1)
		
		search_sql = """
		SELECT p.*, pc.category_code as item_kzp_category_code
		FROM products p
		JOIN products_fts fts ON p.id = fts.rowid
		LEFT JOIN product_categories pc ON p.market_name = pc.market_name AND p.item_code = pc.item_code
		WHERE products_fts MATCH ?
		ORDER BY rank, p.market_name, p.item_name
		"""
		
		try:
			with self.connect() as conn:
				cursor = conn.execute(search_sql, (fts_query,))
				return cursor.fetchall()
		except sqlite3.Error as e:
			print(f"Error searching products: {e}")
			try:
				simple_query = ' AND '.join([f'"{word}"*' for word in search_words if word])
				cursor = conn.execute(search_sql, (simple_query,))
				return cursor.fetchall()
			except:
				return []

	def get_all_products(self) -> list:
		"""Get all products from the database with their categories"""
		select_sql = """
		SELECT p.*, pc.category_code as item_kzp_category_code
		FROM products p
		LEFT JOIN product_categories pc ON p.market_name = pc.market_name AND p.item_code = pc.item_code
		ORDER BY p.market_name, p.item_name
		"""
		
		try:
			with self.connect() as conn:
				cursor = conn.execute(select_sql)
				return cursor.fetchall()
		except sqlite3.Error as e:
			print(f"Error getting all products: {e}")
			return []

	def update_product_category(self, product_ids: list, category_code: str) -> bool:
		"""Update the category for multiple products in product_categories table"""
		if not product_ids:
			return True
		
		# Get the product details for the given IDs
		placeholders = ','.join('?' * len(product_ids))
		get_products_sql = f"""
		SELECT market_name, item_code FROM products WHERE id IN ({placeholders})
		"""
		
		update_sql = """
		INSERT OR REPLACE INTO product_categories
		(market_name, item_code, category_code)
		VALUES (?, ?, ?)
		"""
		
		try:
			with self.connect() as conn:
				# Get product details
				cursor = conn.execute(get_products_sql, product_ids)
				products = cursor.fetchall()
				
				if not products:
					return False
				
				# Update categories
				assignments = [(product['market_name'], product['item_code'], category_code) for product in products]
				conn.executemany(update_sql, assignments)
				return True
		except sqlite3.Error as e:
			print(f"Error updating product categories: {e}")
			return False

	def remove_product_category(self, product_ids: list) -> bool:
		"""Remove category assignments for multiple products"""
		if not product_ids:
			return True
		
		# Get the product details for the given IDs
		placeholders = ','.join('?' * len(product_ids))
		get_products_sql = f"""
		SELECT market_name, item_code FROM products WHERE id IN ({placeholders})
		"""
		
		delete_sql = """
		DELETE FROM product_categories 
		WHERE market_name = ? AND item_code = ?
		"""
		
		try:
			with self.connect() as conn:
				# Get product details
				cursor = conn.execute(get_products_sql, product_ids)
				products = cursor.fetchall()
				
				if not products:
					return False
				
				# Remove category assignments
				assignments = [(product['market_name'], product['item_code']) for product in products]
				conn.executemany(delete_sql, assignments)
				return True
		except sqlite3.Error as e:
			print(f"Error removing product categories: {e}")
			return False

	def get_products_by_category(self, category_code: str = None) -> list:
		"""Get products filtered by category from product_categories table"""
		if category_code:
			select_sql = """
			SELECT p.*, pc.category_code as item_kzp_category_code
			FROM products p
			JOIN product_categories pc ON p.market_name = pc.market_name AND p.item_code = pc.item_code
			WHERE pc.category_code = ?
			ORDER BY p.market_name, p.item_name
			"""
			params = [category_code]
		else:
			select_sql = """
			SELECT p.*, pc.category_code as item_kzp_category_code
			FROM products p
			JOIN product_categories pc ON p.market_name = pc.market_name AND p.item_code = pc.item_code
			ORDER BY p.market_name, p.item_name
			"""
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

	def cleanup_orphaned_categories(self):
		"""Remove category assignments for products that no longer exist"""
		cleanup_sql = """
		DELETE FROM product_categories
		WHERE (market_name, item_code) NOT IN (
			SELECT market_name, item_code FROM products
		)
		"""
		
		try:
			with self.connect() as conn:
				cursor = conn.execute(cleanup_sql)
				deleted_count = cursor.rowcount
				if deleted_count > 0:
					logging.info(f"Cleaned up {deleted_count} orphaned category assignments.")
				return deleted_count
		except sqlite3.Error as e:
			logging.error(f"Error cleaning up orphaned categories: {e}")
			return 0
