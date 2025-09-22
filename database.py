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
				
				# Also insert into FTS table
				fts_sql = """
				INSERT INTO products_fts (rowid, settlement, market_name, item_name, item_code)
				VALUES (?, ?, ?, ?, ?)
				"""
				conn.execute(fts_sql, (product_id, product_data[0], product_data[1], product_data[2], product_data[3]))
				
				return True
		except sqlite3.Error as e:
			print(f"Error inserting product: {e}")
			return False
			
	def search_products(self, search_term: str) -> list:
		"""Search products using FTS5 across multiple fields"""
		if not search_term.strip():
			# If search term is empty, return all products
			return self.get_all_products()
			
		search_sql = """
		SELECT p.* 
		FROM products p
		JOIN products_fts fts ON p.id = fts.rowid
		WHERE products_fts MATCH ?
		ORDER BY rank, market_name, item_name
		"""
		try:
			with self.connect() as conn:
				# Prepare the search term for FTS5
				# FTS5 uses a different syntax: we need to search each column separately
				fts_query = f'"{search_term}"* OR {search_term}*'
				cursor = conn.execute(search_sql, (fts_query,))
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
