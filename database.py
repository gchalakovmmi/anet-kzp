import sqlite3

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
			
	def drop_fts_table(self):
		"""Drop only the FTS table for rebuilding"""
		drop_sql = "DROP TABLE IF EXISTS products_fts"
		with self.connect() as conn:
			conn.execute(drop_sql)
			
	def create_fts_table(self):
		"""Create FTS5 virtual table for fast searching"""
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
		with self.connect() as conn:
			conn.execute(create_fts_sql)
			# Populate FTS table with existing data
			conn.execute("INSERT INTO products_fts(products_fts) VALUES('rebuild')")
			
	def create_tables_if_not_exist(self):
		"""Create tables if they don't exist"""
		# Create main products table
		create_table_sql = """
		CREATE TABLE IF NOT EXISTS products (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			settlement TEXT NOT NULL,
			market_name TEXT NOT NULL,
			item_name TEXT NOT NULL,
			item_code TEXT NOT NULL,
			item_kzp_category_code TEXT,
			item_retail_price REAL,
			item_promotional_price REAL,
			UNIQUE(market_name, item_code)
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
			conn.execute(create_categories_sql)
			
	def insert_products_batch(self, products_data: list) -> bool:
		"""Insert products in batch for better performance"""
		if not products_data:
			return True
			
		insert_sql = """
		INSERT OR REPLACE INTO products 
		(settlement, market_name, item_name, item_code, item_kzp_category_code, item_retail_price, item_promotional_price)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		"""
		try:
			with self.connect() as conn:
				conn.executemany(insert_sql, products_data)
				return True
		except sqlite3.Error as e:
			print(f"Error inserting products batch: {e}")
			return False
			
	def remove_deleted_products(self, current_item_codes: set):
		"""Remove categories from products that no longer exist in the current data"""
		if not current_item_codes:
			return
			
		# Create a placeholder string for the SQL query
		placeholders = ','.join(['?'] * len(current_item_codes))
		
		update_sql = f"""
		UPDATE products 
		SET item_kzp_category_code = NULL 
		WHERE item_code NOT IN ({placeholders})
		"""
		
		try:
			with self.connect() as conn:
				conn.execute(update_sql, list(current_item_codes))
		except sqlite3.Error as e:
			print(f"Error removing categories from deleted products: {e}")
			
	def search_products(self, search_term: str, category_filter: str = None) -> list:
		"""Search products using FTS5 across multiple fields with flexible matching"""
		if not search_term.strip() and not category_filter:
			# Return empty list when no search term or category filter
			return []
			
		base_sql = """
		SELECT p.* 
		FROM products p
		"""
		
		where_conditions = []
		params = []
		
		if search_term.strip():
			# Split search term into individual words
			search_words = search_term.strip().split()
			if search_words:
				base_sql += " JOIN products_fts fts ON p.id = fts.rowid"
				
				# Build FTS5 query that searches each word independently
				fts_conditions = []
				for word in search_words:
					if word:  # Skip empty words
						# Search for the word as prefix in any field
						fts_conditions.append(f'"{word}"*')
				
				if fts_conditions:
					# Join with NEAR operator to allow flexible ordering
					fts_query = ' NEAR('.join(fts_conditions) + ')' * (len(fts_conditions) - 1)
					where_conditions.append("products_fts MATCH ?")
					params.append(fts_query)
		
		if category_filter:
			where_conditions.append("p.item_kzp_category_code = ?")
			params.append(category_filter)
			
		if where_conditions:
			base_sql += " WHERE " + " AND ".join(where_conditions)
			
		base_sql += " ORDER BY p.market_name, p.item_name"
		
		try:
			with self.connect() as conn:
				cursor = conn.execute(base_sql, params)
				return cursor.fetchall()
		except sqlite3.Error as e:
			print(f"Error searching products: {e}")
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
			
	def remove_product_category(self, product_ids: list) -> bool:
		"""Remove category from multiple products"""
		if not product_ids:
			return True
			
		placeholders = ','.join('?' * len(product_ids))
		update_sql = f"""
		UPDATE products 
		SET item_kzp_category_code = NULL
		WHERE id IN ({placeholders})
		"""
		try:
			with self.connect() as conn:
				conn.execute(update_sql, product_ids)
				return True
		except sqlite3.Error as e:
			print(f"Error removing product categories: {e}")
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
