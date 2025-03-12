import csv
import json
import requests
import logging
import base64
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"verification_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger('verification')

class ResultVerifier:
    def __init__(self, api_base_url, username, password, csv_path):
        """
        Initialize the verifier with API settings and input CSV path
        
        Args:
            api_base_url (str): Base URL for API requests
            username (str): Username for API authentication
            password (str): Password for API authentication
            csv_path (str): Path to the CSV file containing product data
        """
        self.api_base_url = api_base_url
        self.username = username
        self.password = password
        self.csv_path = csv_path
        
        # Create Basic auth token
        auth_string = f"{self.username}:{self.password}"
        auth_bytes = auth_string.encode('ascii')
        base64_bytes = base64.b64encode(auth_bytes)
        base64_auth = base64_bytes.decode('ascii')
        
        self.headers = {
            'Authorization': f'Basic {base64_auth}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        self.mismatches = []
        
    def read_csv_data(self):
        """Read product data from CSV file"""
        products = []
        try:
            with open(self.csv_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    products.append(row)
            logger.info(f"Successfully read {len(products)} products from CSV")
            return products
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            return []
    
    def call_api_for_product(self, sku):
        """
        Call the API to get product data for a single SKU
        
        Args:
            sku (str): The product SKU to look up
            
        Returns:
            dict: The product data or empty dict if API call fails
        """
        try:
            url = f"{self.api_base_url}/api/product/{sku}"
            logger.info(f"Calling API: {url}")
            
            response = requests.get(url, headers=self.headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                return data.get('product', {})
            else:
                logger.error(f"API error for SKU {sku}: Status code {response.status_code}")
                logger.error(f"Response: {response.text[:500]}")  # Log first 500 chars of response
                return {}
        except Exception as e:
            logger.error(f"Error calling API for SKU {sku}: {e}")
            return {}
    
    def call_api_for_products_batch(self, sku_list):
        """
        Call the API to get product data for multiple SKUs in one request
        
        Args:
            sku_list (list): List of SKUs to look up
            
        Returns:
            dict: Dictionary mapping SKUs to product data
        """
        try:
            url = f"{self.api_base_url}/api/products"
            logger.info(f"Calling bulk API with {len(sku_list)} SKUs")
            
            payload = {
                "sku_ids": sku_list,
                "max_workers": min(len(sku_list), 10)  # Use at most 10 workers
            }
            
            response = requests.post(url, headers=self.headers, json=payload, timeout=60)
            
            if response.status_code == 200:
                data = response.json()
                # Create a dictionary mapping SKU to product data
                products_dict = {}
                for product in data.get('products', []):
                    if 'sku' in product:
                        products_dict[product['sku'].upper()] = product
                
                logger.info(f"API returned data for {len(products_dict)} products")
                return products_dict
            else:
                logger.error(f"API error for batch request: Status code {response.status_code}")
                logger.error(f"Response: {response.text[:500]}")  # Log first 500 chars of response
                return {}
        except Exception as e:
            logger.error(f"Error calling API for batch request: {e}")
            return {}
    
    def compare_product_data(self, csv_data, api_data):
        """
        Compare product data from CSV and API
        
        Args:
            csv_data (dict): Product data from CSV
            api_data (dict): Product data from API
            
        Returns:
            dict: Comparison results with mismatch details
        """
        mismatches = []
        
        # Fields to compare
        fields = ['brand', 'stock_quantity', 'stock_status', 'sale_price', 'regular_price']
        
        for field in fields:
            csv_value = str(csv_data.get(field, '')).strip()
            api_value = str(api_data.get(field, '')).strip()
            
            # Handle special cases for numeric fields
            if field in ['sale_price', 'regular_price', 'stock_quantity']:
                try:
                    # Clean and normalize the values for comparison
                    if csv_value:
                        csv_value = str(float(csv_value.replace(',', '')))
                    if api_value:
                        api_value = str(float(api_value.replace(',', '')))
                except ValueError:
                    # If conversion fails, use original values
                    pass
            
            # Check if values match
            if csv_value != api_value:
                mismatches.append({
                    'field': field,
                    'csv_value': csv_data.get(field, ''),
                    'api_value': api_data.get(field, '')
                })
        
        return {
            'sku': csv_data.get('sku', ''),
            'has_mismatches': len(mismatches) > 0,
            'mismatches': mismatches
        }
    
    def process_all_products_individual(self):
        """
        Process all products in the CSV and compare with API data using individual API calls
        
        Returns:
            list: All products with mismatch information
        """
        csv_products = self.read_csv_data()
        results = []
        
        for i, product in enumerate(csv_products):
            sku = product.get('sku', '')
            if not sku:
                logger.warning(f"Skipping row {i+1}: No SKU found")
                continue
                
            logger.info(f"Processing {i+1}/{len(csv_products)}: SKU {sku}")
            
            # Get API data
            api_data = self.call_api_for_product(sku)
            
            if not api_data:
                logger.warning(f"No API data returned for SKU {sku}")
                # Add to mismatches with empty API data
                self.mismatches.append({
                    'sku': sku,
                    'csv_data': product,
                    'api_data': {},
                    'comparison': {
                        'has_mismatches': True,
                        'mismatches': [{'field': 'all', 'reason': 'No API data returned'}]
                    }
                })
                continue
            
            # Compare the data
            comparison = self.compare_product_data(product, api_data)
            
            # If there are mismatches, add to the mismatches list
            if comparison['has_mismatches']:
                self.mismatches.append({
                    'sku': sku,
                    'csv_data': product,
                    'api_data': api_data,
                    'comparison': comparison
                })
            
            # Add to results
            results.append({
                'sku': sku,
                'csv_data': product,
                'api_data': api_data,
                'comparison': comparison
            })
        
        return results
    
    def process_all_products_batch(self, batch_size=10):
        """
        Process all products in the CSV and compare with API data using batch API calls
        
        Args:
            batch_size (int): Number of SKUs to include in each batch API call
            
        Returns:
            list: All products with mismatch information
        """
        csv_products = self.read_csv_data()
        results = []
        
        # Group SKUs into batches for API calls
        all_skus = [product.get('sku', '').upper() for product in csv_products if product.get('sku')]
        
        # Create a SKU to product index mapping
        sku_to_product = {product.get('sku', '').upper(): product for product in csv_products if product.get('sku')}
        
        # Process SKUs in batches
        total_batches = (len(all_skus) + batch_size - 1) // batch_size
        
        api_data_dict = {}
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min((batch_num + 1) * batch_size, len(all_skus))
            batch_skus = all_skus[start_idx:end_idx]
            
            logger.info(f"Processing batch {batch_num + 1}/{total_batches}: {len(batch_skus)} SKUs")
            
            # Call API for this batch
            batch_api_data = self.call_api_for_products_batch(batch_skus)
            api_data_dict.update(batch_api_data)
        
        # Process each product using the collected API data
        for i, product in enumerate(csv_products):
            sku = product.get('sku', '').upper()
            if not sku:
                logger.warning(f"Skipping row {i+1}: No SKU found")
                continue
                
            logger.info(f"Processing {i+1}/{len(csv_products)}: SKU {sku}")
            
            # Get API data for this SKU
            api_data = api_data_dict.get(sku, {})
            
            if not api_data:
                logger.warning(f"No API data returned for SKU {sku}")
                # Add to mismatches with empty API data
                self.mismatches.append({
                    'sku': sku,
                    'csv_data': product,
                    'api_data': {},
                    'comparison': {
                        'has_mismatches': True,
                        'mismatches': [{'field': 'all', 'reason': 'No API data returned'}]
                    }
                })
                continue
            
            # Compare the data
            comparison = self.compare_product_data(product, api_data)
            
            # If there are mismatches, add to the mismatches list
            if comparison['has_mismatches']:
                self.mismatches.append({
                    'sku': sku,
                    'csv_data': product,
                    'api_data': api_data,
                    'comparison': comparison
                })
            
            # Add to results
            results.append({
                'sku': sku,
                'csv_data': product,
                'api_data': api_data,
                'comparison': comparison
            })
        
        return results
    
    def save_mismatch_report(self):
        """
        Save mismatches to a CSV report file
        
        Returns:
            str: Path to the saved report
        """
        if not self.mismatches:
            logger.info("No mismatches found, no report to generate")
            return None
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_path = f"mismatch_report_{timestamp}.csv"
        
        try:
            with open(report_path, 'w', encoding='utf-8', newline='') as file:
                # Create fieldnames from all mismatches
                fieldnames = ['sku', 'mismatch_details']
                
                # Add CSV data fields with 'csv_' prefix
                csv_fields = set()
                for mismatch in self.mismatches:
                    for key in mismatch['csv_data'].keys():
                        csv_fields.add(key)
                
                for field in sorted(csv_fields):
                    fieldnames.append(f'csv_{field}')
                
                # Add API data fields with 'api_' prefix
                api_fields = set()
                for mismatch in self.mismatches:
                    if mismatch['api_data']:
                        for key in mismatch['api_data'].keys():
                            api_fields.add(key)
                
                for field in sorted(api_fields):
                    fieldnames.append(f'api_{field}')
                
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                writer.writeheader()
                
                for mismatch in self.mismatches:
                    row = {'sku': mismatch['sku']}
                    
                    # Add mismatch details
                    mismatch_details = []
                    for field_mismatch in mismatch['comparison'].get('mismatches', []):
                        if 'field' in field_mismatch and 'csv_value' in field_mismatch and 'api_value' in field_mismatch:
                            mismatch_details.append(
                                f"{field_mismatch['field']}: CSV={field_mismatch['csv_value']} API={field_mismatch['api_value']}"
                            )
                        elif 'field' in field_mismatch and 'reason' in field_mismatch:
                            mismatch_details.append(f"{field_mismatch['field']}: {field_mismatch['reason']}")
                    
                    row['mismatch_details'] = "; ".join(mismatch_details)
                    
                    # Add CSV data with 'csv_' prefix
                    for key, value in mismatch['csv_data'].items():
                        row[f'csv_{key}'] = value
                    
                    # Add API data with 'api_' prefix
                    for key, value in mismatch['api_data'].items():
                        row[f'api_{key}'] = value
                    
                    writer.writerow(row)
            
            logger.info(f"Mismatch report saved to {report_path}")
            return report_path
        except Exception as e:
            logger.error(f"Error saving mismatch report: {e}")
            return None
    
    def run_verification_individual(self):
        """Run the verification process using individual API calls for each SKU"""
        logger.info("Starting verification process with individual API calls")
        
        # Process all products
        results = self.process_all_products_individual()
        
        # Save mismatch report
        report_path = self.save_mismatch_report()
        
        # Summary
        total = len(results)
        mismatches = len(self.mismatches)
        matched = total - mismatches
        
        logger.info(f"Verification completed: {total} products processed")
        logger.info(f"Matched: {matched}, Mismatches: {mismatches}")
        
        if report_path:
            logger.info(f"Mismatch report saved to: {report_path}")
        
        return {
            'total': total,
            'matched': matched,
            'mismatches': mismatches,
            'report_path': report_path
        }
    
    def run_verification_batch(self, batch_size=10):
        """Run the verification process using batch API calls"""
        logger.info(f"Starting verification process with batch API calls (batch size: {batch_size})")
        
        # Process all products
        results = self.process_all_products_batch(batch_size)
        
        # Save mismatch report
        report_path = self.save_mismatch_report()
        
        # Summary
        total = len(results)
        mismatches = len(self.mismatches)
        matched = total - mismatches
        
        logger.info(f"Verification completed: {total} products processed")
        logger.info(f"Matched: {matched}, Mismatches: {mismatches}")
        
        if report_path:
            logger.info(f"Mismatch report saved to: {report_path}")
        
        return {
            'total': total,
            'matched': matched,
            'mismatches': mismatches,
            'report_path': report_path
        }

# Example usage
if __name__ == "__main__":
    # Configuration
    API_BASE_URL = "http://116.206.127.63:5000"  # Your API base URL
    USERNAME = "shop_manager1"
    PASSWORD = "(LPrIm&)gIq957Uh1D1u1wwo"
    CSV_PATH = "products.csv"  # Path to your CSV file
    BATCH_SIZE = 10  # Number of SKUs to send in each batch request
    
    # Create verifier with proper authentication
    verifier = ResultVerifier(API_BASE_URL, USERNAME, PASSWORD, CSV_PATH)
    
    # Choose verification method:
    # 1. For testing a few SKUs or debugging, use individual API calls
    # results = verifier.run_verification_individual()
    
    # 2. For processing many SKUs efficiently, use batch API calls
    results = verifier.run_verification_batch(BATCH_SIZE)
    
    # Print summary
    print("\nVerification Summary:")
    print(f"Total products processed: {results['total']}")
    print(f"Products matched: {results['matched']}")
    print(f"Products with mismatches: {results['mismatches']}")
    
    if results['report_path']:
        print(f"Mismatch report saved to: {results['report_path']}")