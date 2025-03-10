# KTW Product Data Scraper

A Python Flask API service for extracting product data from KTW website, including stock availability, pricing, and brand information.

## Overview

This service provides a REST API that scrapes product information from the KTW website. It supports fetching stock quantities, pricing data, and brand information for products via their SKU numbers. The service also applies configurable discounts to prices based on brand.

## Features

- API endpoints for fetching single or multiple product details
- Stock level checking from shop.ktw.co.th
- Pricing and brand information extraction from ktw.co.th
- Configurable brand-based discount system
- Concurrent processing for multiple SKUs
- Authentication using API tokens (Basic Auth)
- Comprehensive logging with daily rotation
- Simple health check endpoint

## Requirements

- Python 3.8 or higher
- Flask
- BeautifulSoup4
- Requests
- Additional requirements listed in `requirements.txt`

## Installation

1. Clone this repository or download the source code

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Create a `config.json` file with your KTW credentials:
   ```json
   {
     "user_name": "your_username",
     "password": "your_password",
     "shop_url": "https://shop.ktw.co.th",
     "base_url": "https://ktw.co.th"
   }
   ```

4. Create an `xconfig.json` file for discount configuration:
   ```json
   {
     "SP_BRAND_DC_RATIO": {
       "brand1": 0.85,
       "brand2": 0.9
     },
     "OTHER_BRAND_DC_RATIO": 0.95
   }
   ```

## Usage

### Starting the Service

Run the service with:

```
python app.py
```

The server will start on port 5000 and be accessible at http://localhost:5000.

### API Endpoints

#### Health Check

```
GET /health
```

Returns a simple status check to verify the service is running.

#### Get Single Product

```
GET /api/product/{sku}
```

Fetches data for a single product by SKU.

Example:
```
GET /api/product/NA0520P0810
```

#### Get Multiple Products

```
POST /api/products
```

Fetches data for multiple products.

Request Body:
```json
{
  "sku_ids": ["SKU1", "SKU2", "SKU3"],
  "max_workers": 10
}
```

The `max_workers` parameter is optional and defaults to 10. It controls the number of concurrent requests.

### Authentication

All API endpoints (except `/health`) require authentication using Basic Auth.

Example:
```
Authorization: Basic token
```

### Response Format

Example response for a single product:
```json
{
  "product": {
    "sku": "NA0520P0810",
    "brand": "BrandName",
    "stock_quantity": 15,
    "stock_status": 1,
    "sale_price": 1275.0,
    "regular_price": "฿1,500"
  },
  "processing_time": 1.45
}
```

Example response for multiple products:
```json
{
  "products": [
    {
      "sku": "SKU1",
      "brand": "BrandName",
      "stock_quantity": 5,
      "stock_status": 1,
      "sale_price": 850.0,
      "regular_price": "฿1,000"
    },
    {
      "sku": "SKU2",
      "brand": "OtherBrand",
      "stock_quantity": 0,
      "stock_status": 0,
      "sale_price": 950.0,
      "regular_price": "฿1,000"
    }
  ],
  "count": 2,
  "processing_time": 2.34
}
```

## Discount Configuration

The `xconfig.json` file controls price discounting:

- `SP_BRAND_DC_RATIO`: A dictionary mapping brand names (lowercase) to discount ratios
- `OTHER_BRAND_DC_RATIO`: The default discount ratio for brands not listed in `SP_BRAND_DC_RATIO`

For example, a ratio of 0.9 means the price will be 90% of the original (a 10% discount).

## Logging

Logs are stored in the `logs` directory:

- `scraper.log`: General application logs
- `requests.log`: API request/response logs

Logs rotate daily with a 30-day retention period.

## Deployment

This service can be deployed on any server with Python installed. For a Windows deployment:

1. Install Python 3.8+
2. Copy all files to a directory
3. Install dependencies as described above
4. Create or modify config files as needed
5. Run the script manually or set up as a Windows service using NSSM

## Troubleshooting

If you encounter encoding errors, especially with Thai text, ensure your console and files use UTF-8 encoding.

For other issues, check the log files for details.

## License

[Specify license here]

## Disclaimer

This tool is intended for legitimate use only. Please respect the terms of service of the KTW website when using this tool.
