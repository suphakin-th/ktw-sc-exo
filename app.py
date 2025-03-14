import csv
import json
import requests
import logging
import base64
from datetime import datetime
import time

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
    def __init__(self, api_url, username, password, csv_path, batch_size=10):
        """
        Initialize the verifier with API settings and input CSV path
        
        Args:
            api_url (str): URL for the bulk products API endpoint
            username (str): API username
            password (str): API password
            csv_path (str): Path to the CSV file containing product data
            batch_size (int): Number of SKUs to send in each API request
        """
        self.api_url = api_url
        self.username = username
        self.password = password
        self.csv_path = csv_path
        self.batch_size = batch_size
        
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
    
    def call_api_for_products(self, sku_list):
        """
        Call the API to get product data for multiple SKUs
        
        Args:
            sku_list (list): List of SKUs to look up
            
        Returns:
            dict: Dictionary mapping SKUs to product data
        """
        try:
            payload = {
                "sku_ids": sku_list,
                "max_workers": min(len(sku_list), 10)  # Use at most 10 workers
            }
            
            logger.info(f"Calling API with {len(sku_list)} SKUs")
            
            response = requests.post(self.api_url, headers=self.headers, json=payload, timeout=60)
            
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
    
    def process_all_products(self):
        """
        Process all products in the CSV and compare with API data
        
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
        total_batches = (len(all_skus) + self.batch_size - 1) // self.batch_size
        
        api_data_dict = {}
        
        for batch_num in range(total_batches):
            start_idx = batch_num * self.batch_size
            end_idx = min((batch_num + 1) * self.batch_size, len(all_skus))
            batch_skus = all_skus[start_idx:end_idx]
            
            logger.info(f"Processing batch {batch_num + 1}/{total_batches}: {len(batch_skus)} SKUs")
            
            # Call API for this batch
            batch_api_data = self.call_api_for_products(batch_skus)
            api_data_dict.update(batch_api_data)
            
            # Add a small delay between batches to avoid overloading the API
            if batch_num < total_batches - 1:
                time.sleep(1)
        
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
                # Create fieldnames for the CSV
                fieldnames = ['sku']
                
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
                
                # Add a column for the mismatch details
                fieldnames.append('mismatch_details')
                
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                writer.writeheader()
                
                for mismatch in self.mismatches:
                    row = {'sku': mismatch['sku']}
                    
                    # Add CSV data with 'csv_' prefix
                    for key, value in mismatch['csv_data'].items():
                        row[f'csv_{key}'] = value
                    
                    # Add API data with 'api_' prefix
                    for key, value in mismatch['api_data'].items():
                        row[f'api_{key}'] = value
                    
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
                    
                    writer.writerow(row)
            
            logger.info(f"Mismatch report saved to {report_path}")
            return report_path
        except Exception as e:
            logger.error(f"Error saving mismatch report: {e}")
            return None
    
    def run_verification(self):
        """Run the complete verification process"""
        logger.info("Starting verification process")
        
        # Process all products
        results = self.process_all_products()
        
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
    API_URL = "http://116.206.127.63:5000/api/products"  # Your bulk API endpoint
    USERNAME = "shop_manager1"
    PASSWORD = "(LPrIm&)gIq957Uh1D1u1wwo"
    CSV_PATH = "products.csv"  # Path to your CSV file
    BATCH_SIZE = 10  # Number of SKUs to send in each request
    
    # Create and run the verifier
    verifier = ResultVerifier(API_URL, USERNAME, PASSWORD, CSV_PATH, BATCH_SIZE)
    results = verifier.run_verification()
    
    # Print summary
    print("\nVerification Summary:")
    print(f"Total products processed: {results['total']}")
    print(f"Products matched: {results['matched']}")
    print(f"Products with mismatches: {results['mismatches']}")
    
    if results['report_path']:
        print(f"Mismatch report saved to: {results['report_path']}")