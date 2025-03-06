from flask import Flask, request, jsonify
import requests
from bs4 import BeautifulSoup
import json
import logging
from logging.handlers import TimedRotatingFileHandler
import concurrent.futures
from functools import wraps
import time
import os
from datetime import datetime
import base64

app = Flask(__name__)

# Create logs directory if it doesn't exist
logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(logs_dir, exist_ok=True)

def apply_discount(price, brand, config):
	"""
	Apply discount to a price based on brand and configuration
	
	Args:
		price (str): The price string, which may contain currency symbols or formatting
		brand (str): The brand name
		config (dict): Configuration with discount ratios
		
	Returns:
		float or str: Discounted price as float, or original price if processing fails
	"""
	logger.info(f"Applying discount for brand '{brand}'")
	
	if not price or not isinstance(price, str):
		logger.warning(f"Invalid price value: {price}")
		return 0.0
	
	try:
		# Clean price string - remove currency symbols, commas, and spaces
		cleaned_price = price.replace('฿', '').replace('THB', '').replace(',', '').strip()
		
		# Convert to float
		price_float = float(cleaned_price)
		logger.info(f"Original price: {price_float}")
		
		# Normalize brand name for lookup
		normalized_brand = brand.lower().strip() if brand else "unknown"
		
		# Get discount ratio from config, default to OTHER_BRAND_DC_RATIO if brand not found
		if "SP_BRAND_DC_RATIO" not in config or "OTHER_BRAND_DC_RATIO" not in config:
			logger.warning("Missing required config keys: SP_BRAND_DC_RATIO or OTHER_BRAND_DC_RATIO")
			return price_float
			
		discount_ratio = config["SP_BRAND_DC_RATIO"].get(normalized_brand, config["OTHER_BRAND_DC_RATIO"])
		logger.info(f"Using discount ratio for '{normalized_brand}': {discount_ratio}")
		
		# Apply discount and round to 2 decimal places
		discounted_price = round(price_float * discount_ratio, 2)
		logger.info(f"Discounted price: {discounted_price}")
		
		return str(discounted_price)
		
	except (ValueError, TypeError) as e:
		logger.error(f"Error applying discount to price '{price}': {str(e)}")
		# Return original price if conversion fails
		return str(price)
	except Exception as e:
		logger.error(f"Unexpected error in apply_discount: {str(e)}")
		return str(price)

# Configure logging with daily rotation
def setup_logger():
	# Main logger
	logger = logging.getLogger('ktw_scraper')
	logger.setLevel(logging.INFO)
	
	# Format for the logs
	formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	
	# Daily rotating file handler for general logs
	log_file = os.path.join(logs_dir, 'scraper.log')
	file_handler = TimedRotatingFileHandler(
		log_file, 
		when='midnight', 
		interval=1,
		backupCount=30  # Keep logs for 30 days
	)
	file_handler.setFormatter(formatter)
	file_handler.suffix = "%Y-%m-%d"
	
	# Create a separate handler for request logs
	request_log_file = os.path.join(logs_dir, 'requests.log')
	request_handler = TimedRotatingFileHandler(
		request_log_file, 
		when='midnight', 
		interval=1,
		backupCount=30  # Keep logs for 30 days
	)
	request_handler.setFormatter(formatter)
	request_handler.suffix = "%Y-%m-%d"
	
	# Console handler
	console_handler = logging.StreamHandler()
	console_handler.setFormatter(formatter)
	
	# Clear existing handlers
	if logger.hasHandlers():
		logger.handlers.clear()
		
	# Add handlers to logger
	logger.addHandler(file_handler)
	logger.addHandler(console_handler)
	
	# Create a request logger
	request_logger = logging.getLogger('ktw_requests')
	request_logger.setLevel(logging.INFO)
	
	# Clear existing handlers
	if request_logger.hasHandlers():
		request_logger.handlers.clear()
		
	request_logger.addHandler(request_handler)
	request_logger.addHandler(console_handler)
	
	return logger, request_logger

# Set up the loggers
logger, request_logger = setup_logger()

# Authentication configuration
# Replace with your actual expected token
EXPECTED_AUTH = "c2hvcF9tYW5hZ2VyMTooTFBySW0mKWdJcTk1N1VoMUQxdTF3d28="

# Authentication decorator
def token_required(f):
	@wraps(f)
	def decorated(*args, **kwargs):
		auth_header = request.headers.get('Authorization')
		
		if not auth_header:
			request_logger.warning(f"API request missing Authorization header: {request.remote_addr} - {request.path}")
			return jsonify({
				'message': 'Authorization header is missing',
				'error': 'Unauthorized'
			}), 401
		
		# Check for Basic auth
		if auth_header.startswith('Basic '):
			auth_token = auth_header.split(' ')[1]
			
			# Direct comparison with the expected token
			if auth_token == EXPECTED_AUTH:
				return f(*args, **kwargs)
			else:
				# For debugging, log the provided token (be careful with this in production)
				logger.debug(f"Invalid token provided: {auth_token[:10]}...")
		
		request_logger.warning(f"Invalid API authorization: {request.remote_addr} - {request.path}")
		return jsonify({
			'message': 'Invalid authorization',
			'error': 'Unauthorized'
		}), 401
			
	return decorated

# Request logging middleware
@app.before_request
def log_request_info():
	request_id = datetime.now().strftime('%Y%m%d%H%M%S-') + str(hash(request.remote_addr) % 10000)
	request_logger.info(f"Request ID: {request_id} | Method: {request.method} | Path: {request.path} | IP: {request.remote_addr}")
	
	# Log request headers and body for debugging
	headers = dict(request.headers)
	# Remove sensitive information
	if 'Authorization' in headers:
		headers['Authorization'] = '[REDACTED]'
	
	request_logger.debug(f"Request ID: {request_id} | Headers: {headers}")
	
	if request.is_json:
		body = request.get_json()
		# Truncate large request bodies
		if isinstance(body, dict) and "sku_ids" in body and isinstance(body["sku_ids"], list):
			sku_count = len(body["sku_ids"])
			if sku_count > 10:
				body["sku_ids"] = body["sku_ids"][:5] + ["..."] + body["sku_ids"][-5:]
				body["sku_ids_count"] = sku_count
		request_logger.debug(f"Request ID: {request_id} | Body: {body}")
	
	# Store request_id in Flask's g object for use in other parts of the request
	request.request_id = request_id

# Response logging middleware
@app.after_request
def log_response_info(response):
	if hasattr(request, 'request_id'):
		request_id = request.request_id
		status_code = response.status_code
		response_size = len(response.get_data())
		
		# Log basic response info
		request_logger.info(f"Request ID: {request_id} | Status: {status_code} | Size: {response_size} bytes")
		
		# Log response time if available
		if hasattr(request, 'start_time'):
			elapsed_time = time.time() - request.start_time
			request_logger.info(f"Request ID: {request_id} | Processing time: {elapsed_time:.2f} seconds")
	
	return response

class KTWScraper:
	def __init__(self, config_path="config.json"):
		try:
			with open(config_path, 'r') as f:
				self.config = json.load(f)
		except Exception as e:
			logger.error(f"Failed to load config: {e}")
			self.config = {
				"user_name": "",
				"password": "",
				"shop_url": "https://shop.ktw.co.th",
				"base_url": "https://ktw.co.th"
			}
		
		self.session = requests.Session()
		self.session.headers.update({
			"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
			"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
			"Accept-Language": "en-US,en;q=0.5",
			"Accept-Encoding": "gzip, deflate, br, zstd",
			"Cache-Control": "no-cache",
			"Pragma": "no-cache"
		})

	def get_csrf_token(self):
		"""Get CSRF token from login page"""
		try:
			login_page_url = f"{self.config['shop_url']}/ktw/th/THB/login"
			response = self.session.get(login_page_url)
			
			if not response.ok:
				logger.error(f"Failed to get login page: {response.status_code}")
				return None
				
			soup = BeautifulSoup(response.text, 'html.parser')
			csrf_input = soup.select_one("input[name='CSRFToken']")
			
			if csrf_input and csrf_input.get('value'):
				return csrf_input.get('value')
			else:
				logger.error("CSRF token not found")
				return None
		except Exception as e:
			logger.error(f"Error getting CSRF token: {e}")
			return None

	def login(self):
		"""Login to KTW shop"""
		try:
			csrf_token = self.get_csrf_token()
			if not csrf_token:
				return False
				
			login_post_url = f"{self.config['shop_url']}/ktw/th/THB/j_spring_security_check"
			
			form_data = {
				"j_username": self.config["user_name"],
				"j_password": self.config["password"],
				"CSRFToken": csrf_token,
				"_csrf": csrf_token
			}
			
			headers = {
				"Origin": self.config["shop_url"],
				"Referer": f"{self.config['shop_url']}/ktw/th/THB/login"
			}
			
			response = self.session.post(login_post_url, data=form_data, headers=headers)
			logger.debug("Login response status: %s", response.status_code)
			return self.verify_login()
		except Exception as e:
			logger.error(f"Login error: {e}")
			return False

	def verify_login(self):
		"""Verify login success"""
		try:
			account_url = f"{self.config['shop_url']}/ktw/th/THB/my-account/update-profile"
			response = self.session.get(account_url)
			
			# Fix for 'str' object has no attribute 'path' error
			if hasattr(response, 'url') and isinstance(response.url, str) and "/login" in response.url:
				return False
				
			if not response.ok:
				return False
				
			soup = BeautifulSoup(response.text, 'html.parser')
			
			# Check for multiple indicators of successful login
			selectors = [
				"form#updateProfileForm",
				"input#profile.email", 
				"a[href*='logout']"
			]
			
			for selector in selectors:
				if soup.select_one(selector):
					return True
					
			return False
		except Exception as e:
			logger.error(f"Verify login error: {e}")
			return False

	def get_product_info_from_base_url(self, sku):
		"""Get product information from base_url search page"""
		try:
			search_url = f"{self.config['base_url']}/search/?searchType=All&viewType=grid&text={sku}"
			logger.info(f"Fetching product data for SKU {sku} from search page")
			
			response = self.session.get(search_url)
			
			if not response.ok:
				logger.error(f"Failed to get search page for SKU {sku}: {response.status_code}")
				return {"brand": "", "sale_price": "", "regular_price": ""}
			
			soup = BeautifulSoup(response.text, 'html.parser')
			
			# Find the correct product grid item
			product_items = soup.select(".grid-item")
			product_info = {"brand": "", "sale_price": "", "regular_price": ""}
			
			for item in product_items:
				# Check if this grid item matches our SKU
				product_sku_elem = item.select_one(".grid-item__sku")
				if product_sku_elem and sku in product_sku_elem.text.strip():
					# Extract brand
					brand_elem = item.select_one(".grid-item__brand")
					if brand_elem:
						product_info["brand"] = brand_elem.text.strip()
					
					# Extract sale price
					sale_price_elem = item.select_one(".grid-item__saleprice")
					if sale_price_elem:
						product_info["sale_price"] = sale_price_elem.text.strip()
					
					# Extract regular price
					regular_price_elem = item.select_one(".grid-item__wasprice")
					if regular_price_elem:
						product_info["regular_price"] = regular_price_elem.text.strip()
					
					break
			
			logger.info(f"Product info for SKU {sku}: {product_info}")
			return product_info
		except Exception as e:
			logger.error(f"Error getting product info for SKU {sku}: {e}")
			return {"brand": "", "sale_price": "", "regular_price": ""}

	def check_stock(self, sku):
		"""Check stock for a single SKU and get product info from base_url"""
		try:
			# Load xconfig
			x_cfg_path = os.path.join(os.getcwd(), "xconfig.json")
			file_path = os.path.abspath(x_cfg_path)
			
			try:
				with open(file_path, mode='r', encoding='utf-8', newline='') as file:
					xconfig = json.load(file)
				logger.info(f"Successfully loaded xconfig from {file_path}")
			except Exception as e:
				logger.error(f"Error loading xconfig from {file_path}: {str(e)}")
				# Fallback default config
				xconfig = {
					"SP_BRAND_DC_RATIO": {},
					"OTHER_BRAND_DC_RATIO": 1.0  # No discount by default
				}
			
			# Get stock information from shop_url
			shop_url = f"{self.config['shop_url']}/ktw/th/THB/p/{sku}"
			logger.info(f"Checking stock for SKU: {sku}")
			
			response = self.session.get(shop_url)
			
			if not response.ok:
				logger.error(f"Failed to get product page for SKU: {sku}")
				return {
					"sku": sku,
					"brand": "",
					"stock_quantity": 0,
					"stock_status": 0,
					"sale_price": 0.0,
					"regular_price": ""
				}
				
			soup = BeautifulSoup(response.text, 'html.parser')
			
			# Initialize product data
			product_data = {
				"sku": sku,
				"brand": "",
				"stock_quantity": 0,
				"sale_price": "",
				"regular_price": ""
			}
			
			# Find the stock table
			table = soup.select_one("div.table-responsive.stock-striped table")
			stock_total = 0
			
			if table:
				# Find headers to get the stock column index
				headers = table.select("th")
				stock_index = 1  # Default to second column
				
				for i, header in enumerate(headers):
					if "ในสต๊อก" in header.text.strip():
						stock_index = i
						break
				
				# Process each row
				for row in table.select("tr"):
					cells = row.select("td")
					if len(cells) > stock_index:
						stock_text = cells[stock_index].text.strip()
						# Extract the last part of the text (number)
						if stock_text:
							last_part = stock_text.split()[-1]
							try:
								stock_num = int(last_part)
								stock_total += stock_num
							except ValueError:
								pass
			
			product_data["stock_quantity"] = stock_total
			product_data["stock_status"] = 1 if stock_total > 0 else 0
			
			# Get product information from base_url
			product_info = self.get_product_info_from_base_url(sku)
			
			# Set product brand
			product_data["brand"] = product_info["brand"]
			
			# Set and apply discount to regular price
			product_data["regular_price"] = product_info["regular_price"]
			
			# Set and apply discount to sale price
			original_sale_price = product_info["sale_price"]
			product_data["sale_price"] = apply_discount(original_sale_price, product_info["brand"], xconfig)
			
			logger.info(f"Found data for SKU {sku}: {product_data}")
			return product_data
		except Exception as e:
			logger.error(f"Error checking stock for SKU {sku}: {e}")
			return {
				"sku": sku,
				"brand": "",
				"stock_quantity": 0,
				"stock_status": 0,
				"sale_price": "",
				"regular_price": ""
			}

	def get_products_data(self, sku_list, max_workers=10):
		"""Get data for multiple SKUs concurrently"""
		results = []
		
		# Ensure we're logged in
		if not self.is_logged_in():
			logger.info("Not logged in, attempting login")
			if not self.login():
				logger.error("Login failed")
				return []
		
		# Use thread pool for concurrent requests
		with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
			# Submit all tasks
			future_to_sku = {executor.submit(self.check_stock, sku): sku for sku in sku_list}
			
			# Process results as they complete
			for future in concurrent.futures.as_completed(future_to_sku):
				sku = future_to_sku[future]
				try:
					product_data = future.result()
					if product_data:
						results.append(product_data)
				except Exception as e:
					logger.error(f"Error processing SKU {sku}: {e}")
		
		return results

	def is_logged_in(self):
		"""Check if still logged in"""
		try:
			home_url = f"{self.config['shop_url']}/ktw/th/THB"
			response = self.session.get(home_url)
			
			if not response.ok:
				return False
				
			soup = BeautifulSoup(response.text, 'html.parser')
			
			profile_selector = "a[href='/ktw/th/THB/my-account/update-profile']"
			username_selector = "span.header__user-name"
			
			return bool(soup.select_one(profile_selector) or soup.select_one(username_selector))
		except Exception as e:
			logger.error(f"Error checking login status: {e}")
			return False

# Create a global scraper instance
scraper = KTWScraper()

@app.route('/health', methods=['GET'])
def health_check():
	"""Health check endpoint - no auth required"""
	return jsonify({"status": "ok"})

@app.route('/api/products', methods=['POST'])
@token_required
def get_products():
	"""Get product data for multiple SKUs"""
	# Set start time for performance tracking
	request.start_time = time.time()
	
	try:
		data = request.get_json()
		
		if not data or not isinstance(data, dict) or "sku_ids" not in data:
			return jsonify({"error": "Invalid request. Expected JSON with 'sku_ids' list"}), 400
			
		sku_ids = data["sku_ids"]
		
		if not isinstance(sku_ids, list):
			return jsonify({"error": "sku_ids must be a list"}), 400
			
		# Get the concurrent request limit
		max_workers = data.get("max_workers", 10)
		max_workers = min(max_workers, 100)  # Cap at 100 concurrent requests
		
		request_logger.info(f"Processing request for {len(sku_ids)} SKUs with {max_workers} workers")
		
		# Get product data
		results = scraper.get_products_data(sku_ids, max_workers)
		
		elapsed_time = time.time() - request.start_time
		request_logger.info(f"Request processed in {elapsed_time:.2f} seconds, found {len(results)} products")
		
		return jsonify({
			"products": results,
			"count": len(results),
			"processing_time": elapsed_time
		})
		
	except Exception as e:
		logger.error(f"Error in API request: {e}")
		if hasattr(request, 'request_id'):
			request_logger.error(f"Request ID: {request.request_id} | Error: {str(e)}")
		return jsonify({"error": str(e)}), 500

@app.route('/api/product/<sku>', methods=['GET'])
@token_required
def get_single_product(sku):
	"""Get product data for a single SKU"""
	# Set start time for performance tracking
	request.start_time = time.time()
	
	try:
		request_logger.info(f"Processing request for SKU: {sku}")
		
		# Ensure login
		if not scraper.is_logged_in():
			request_logger.info("Not logged in, attempting login")
			if not scraper.login():
				request_logger.error("Login failed")
				return jsonify({"error": "Login failed"}), 500
		
		# Get product data
		product_data = scraper.check_stock(sku)
		
		elapsed_time = time.time() - request.start_time
		request_logger.info(f"Request for SKU {sku} processed in {elapsed_time:.2f} seconds")
		
		return jsonify({
			"product": product_data,
			"processing_time": elapsed_time
		})
		
	except Exception as e:
		logger.error(f"Error in API request for SKU {sku}: {e}")
		if hasattr(request, 'request_id'):
			request_logger.error(f"Request ID: {request.request_id} | Error: {str(e)}")
		return jsonify({"error": str(e)}), 500

@app.route('/login', methods=['POST'])
def api_login():
	"""Login endpoint"""
	# Set start time for performance tracking
	request.start_time = time.time()
	
	try:
		request_logger.info("Processing login request")
		
		if scraper.login():
			request_logger.info("Login successful")
			return jsonify({"status": "success", "message": "Login successful"})
		else:
			request_logger.warning("Login failed")
			return jsonify({"status": "error", "message": "Login failed"}), 401
	except Exception as e:
		logger.error(f"Login error: {e}")
		if hasattr(request, 'request_id'):
			request_logger.error(f"Request ID: {request.request_id} | Error: {str(e)}")
		return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
	# Try to login on startup
	try:
		logger.info("Attempting initial login...")
		scraper.login()
	except Exception as e:
		logger.error(f"Initial login failed: {e}")
	
	# Start server
	app.run(host='0.0.0.0', port=5000, debug=True)