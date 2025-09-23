from flask import Flask, render_template, jsonify, request, Response
import threading
import time
import csv
import io
from config import Config
from database import Database
from processor import DataProcessor

app = Flask(__name__)

# Global variables to track processing status
processing_status = {
	'is_processing': False,
	'current_market': '',
	'progress': 0,
	'total_markets': 0,
	'processed_markets': 0,
	'message': 'Initializing...',
	'error': None
}

# Global database instance
db = None

# Category mapping (from the HTML select options)
CATEGORIES = {
	'1': '1. Бял хляб от 500 гр. до 1 кг',
	'2': '2. Хляб Добруджа от 500 гр. до 1 кг',
	'3': '3. Ръжен хляб от 400 гр. до 600 гр.',
	'4': '4. Типов хляб от 400 гр. до 600 гр.',
	'5': '5. Точени кори от 400 гр. до 500 гр.',
	'6': '6. Прясно мляко от 2 % до 3.6 % 1 л',
	'7': '7. Кисело мляко от 2 % до 3.6 % в кофички от 370 гр. до 500 гр.',
	'8': '8. Сирене от краве мляко насипно 1 кг',
	'9': '9. Сирене от краве мляко пакетирано за 1 кг',
	'10': '10. Кашкавал от краве мляко насипно 1 кг',
	'11': '11. Кашкавал от краве мляко пакетирано за 1 кг',
	'12': '12. Краве масло от 125 гр. до 250 гр.',
	'13': '13. Извара насипна 1 кг.',
	'14': '14. Извара пакетирана от 200 гр. до 1 кг.',
	'15': '15. Прясно охладено пиле 1 кг (цяло)',
	'16': '16. Пилешко филе, охладено, 1 кг',
	'17': '17. Пилешки бут, цял, охладен 1 кг',
	'18': '18. Прясно свинско месо плешка 1 кг',
	'19': '19. Прясно свинско месо бут 1 кг',
	'20': '20. Прясно свинско месо шол 1 кг',
	'21': '21. Прясно свинско месо врат 1 кг',
	'22': '22. Свинско месо за готвене 1 кг',
	'23': '23. Телешко месо шол 1 кг',
	'24': '24. Телешко месо за готвене 1 кг',
	'25': '25. Мляно месо смес 60/40, насипно за 1 кг',
	'26': '26. Кренвирши, насипни за 1 кг.',
	'27': '27. Колбаси пресни от 300 гр. до 1 кг.',
	'28': '28. Колбаси сухи (Шпек, Бургас, Деликатесен) от 250 гр. до 1 кг.',
	'29': '29. Риба замразена (скумрия, пъстърва, лаврак, ципура) 1 кг',
	'31': '31. Яйца размер М от 6 бр. до 10 бр. Подово отглеждане',
	'32': '32. Яйца размер L 6 бр. до 10 бр. Подово отглеждане',
	'33': '33. Боб, пакетиран 1 кг',
	'34': '34. Леща, пакетиран 1 кг',
	'35': '35. Бисерен ориз 1 кг',
	'36': '36. Макарони от 400 гр. до 500 гр.',
	'37': '37. Спагети (№ 3, № 5 и № 10) 500 гр.',
	'38': '38. Бяла захар 1 кг',
	'39': '39. Готварска сол 1 кг',
	'40': '40. Брашно тип 500 1 кг',
	'41': '41. Брашно екстра 1 кг',
	'42': '42. Олио слънчогледово 1 л',
	'43': '43. Зехтин 1л',
	'44': '44. Винен оцет 700 мл.',
	'45': '45. Ябълков оцет 700 мл.',
	'46': '46. Консерви боб, от 400 гр. до 800 гр.',
	'47': '47. Консерви грах, от 400 гр. до 800 гр.',
	'48': '48. Консервирани домати, от 400 гр. до 800 гр.',
	'49': '49. Лютеница, от 400 гр. до 800 гр.',
	'62': '62. Маслини, насипни 1 кг',
	'63': '63. Каша (млечна, плодова) от 190 гр. до 250 гр.',
	'64': '64. Детско пюре от 190 гр. до 250 гр.',
	'65': '65. Адаптирани млека от 400 гр. до 800 гр.',
	'66': '66. Обикновени бисквити',
	'67': '67. Кроасани от 50 гр. до 110 гр.',
	'68': '68. Баница от 100 гр. до 500 гр.',
	'69': '69. Шоколад, млечен, от 80 гр. до 100 гр.',
	'70': '70. Кафе мляно от 200 гр. до 250 гр.',
	'71': '71. Кафе на зърна 1 кг',
	'72': '72. Чай (билков на пакетчета)',
	'73': '73. Минерална вода, 6 бр. в опаковка по 1,5 л.',
	'74': '74. Светла бира 2 л.',
	'75': '75. Бяло вино бутилирано, произход България 750 мл.',
	'76': '76. Червено вино бутилирано, произход България 750 мл.',
	'77': '77. Ракия, произход България 700 мл.',
	'78': '78. Тютюневи изделия, произход България, кутия, пакет',
	'79': '79. Течен препарат за миене на съдове от 400 мл.',
	'80': '80. Четка за зъби – средна твърдост',
	'81': '81. Паста за зъби, туба от 50 мл. до 125 мл.',
	'82': '82. Шампоан за нормална коса – от 250 мл. до 500 мл.',
	'83': '83. Сапун, твърд',
	'84': '84. Класически мокри кърпи пакет',
	'85': '85. Тоалетна хартия 8 ролки'
}

def initialize_app():
	"""Initialize the application with database setup and data processing"""
	global processing_status, db
	
	processing_status['is_processing'] = True
	processing_status['message'] = 'Starting database setup...'
	
	try:
		config = Config('./config.yaml')
		db = Database('./products.sqlite')
		
		# Get total number of markets for progress tracking
		total_markets = len(config.get_details())
		processing_status['total_markets'] = total_markets
		
		# Drop and recreate tables for clean start
		processing_status['message'] = 'Setting up database tables...'
		db.drop_tables()
		db.create_tables()
		
		# Save category mapping to database
		db.save_category_mapping(CATEGORIES)
		
		# Process Paradox data
		processing_status['message'] = 'Starting data processing...'
		processor = DataProcessor(config.get_details(), db, processing_status)
		processor.paradox_to_sqlite()
		
		processing_status['is_processing'] = False
		processing_status['message'] = 'Data processing completed successfully!'
		
	except Exception as e:
		processing_status['is_processing'] = False
		processing_status['error'] = str(e)
		processing_status['message'] = f'Error during processing: {e}'

# Start processing in a background thread
processing_thread = threading.Thread(target=initialize_app)
processing_thread.daemon = True
processing_thread.start()

@app.route('/')
def categories():
	return render_template('categories.html')

@app.route('/api/processing-status')
def get_processing_status():
	return jsonify(processing_status)

@app.route('/api/search')
def search_products():
	search_term = request.args.get('q', '')
	if not db:
		return jsonify({'error': 'Database not ready'})
	
	# Always use search function, even for empty terms (which will return empty)
	products = db.search_products(search_term)
	
	# Convert rows to dictionaries for JSON serialization
	product_list = []
	for product in products:
		category_name = db.get_category_name(product['item_kzp_category_code']) if product['item_kzp_category_code'] else ""
		product_list.append({
			'id': product['id'],
			'settlement': product['settlement'],
			'market_name': product['market_name'],
			'item_name': product['item_name'],
			'item_code': product['item_code'],
			'item_kzp_category_code': product['item_kzp_category_code'],
			'item_kzp_category_name': category_name,
			'item_retail_price': product['item_retail_price'],
			'item_promotional_price': product['item_promotional_price']
		})
	
	return jsonify({'products': product_list, 'search_term': search_term})

@app.route('/api/update-category', methods=['POST'])
def update_category():
	if not db:
		return jsonify({'success': False, 'error': 'Database not ready'})
	
	data = request.json
	product_ids = data.get('product_ids', [])
	category_code = data.get('category_code', '')
	
	if not product_ids:
		return jsonify({'success': False, 'error': 'No products selected'})
	
	if not category_code:
		return jsonify({'success': False, 'error': 'No category selected'})
	
	success = db.update_product_category(product_ids, category_code)
	category_name = CATEGORIES.get(category_code, '')
	
	return jsonify({
		'success': success,
		'category_name': category_name,
		'category_code': category_code,
		'updated_count': len(product_ids)
	})

@app.route('/api/export-csv')
def export_csv():
	if not db:
		return Response("Database not ready", status=500)
	
	# Get categorized products
	products = db.get_products_by_category()
	
	# Create CSV in memory
	output = io.StringIO()
	writer = csv.writer(output, quoting=csv.QUOTE_ALL)
	
	# Write header
	writer.writerow([
		'Населено място',
		'Търговски обект', 
		'Наименование на продукта',
		'Код на продукта',
		'Код на категория',  # Changed from 'Категория' to 'Код на категория'
		'Цена на дребно',
		'Цена в промоция'
	])
	
	# Write data - using category CODE, not name
	for product in products:
		writer.writerow([
			product['settlement'],
			product['market_name'],
			product['item_name'],
			product['item_code'],
			product['item_kzp_category_code'] or "",  # Use the code, not the name
			str(product['item_retail_price']) if product['item_retail_price'] is not None else "",
			str(product['item_promotional_price']) if product['item_promotional_price'] is not None else ""
		])
	
	# Prepare response
	output.seek(0)
	response = Response(output.getvalue(), mimetype='text/csv')
	response.headers['Content-Disposition'] = 'attachment; filename=categorized_products.csv'
	
	return response

if __name__ == '__main__':
	app.run(debug=True, host='0.0.0.0', port=5000)
