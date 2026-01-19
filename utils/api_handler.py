"""
API Handler Module
Handles integration with external APIs for product information
"""

import requests
import json
from datetime import datetime

# =====================================================
# DummyJSON API Integration
# =====================================================

def fetch_all_products():
    """
    Fetches all products from DummyJSON API
        
    """
    try:
        print("[API] Fetching all products from DummyJSON API...")
        
        # Fetch products from DummyJSON with limit=100
        response = requests.get('https://dummyjson.com/products?limit=100', timeout=10)
        response.raise_for_status()  # Raise exception for bad status codes
        
        data = response.json()
        products = data.get('products', [])
        
        # Extract relevant fields from each product
        product_list = []
        for product in products:
            product_list.append({
                'id': product.get('id'),
                'title': product.get('title'),
                'category': product.get('category'),
                'brand': product.get('brand'),
                'price': product.get('price'),
                'rating': product.get('rating')
            })
        
        print(f"[SUCCESS] Fetched {len(product_list)} products from API")
        return product_list
        
    except requests.exceptions.ConnectionError as e:
        print(f"[ERROR] Connection error: Unable to connect to API")
        return []
    except requests.exceptions.Timeout as e:
        print(f"[ERROR] Timeout error: API request timed out")
        return []
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Request error: {str(e)}")
        return []
    except ValueError as e:
        print(f"[ERROR] JSON parsing error: {str(e)}")
        return []
    except Exception as e:
        print(f"[ERROR] Unexpected error: {str(e)}")
        return []


def create_product_mapping(api_products):
    """
    Creates a mapping of product IDs to product info
    
    """
    product_mapping = {}
    
    for product in api_products:
        product_id = product.get('id')
        
        if product_id is not None:
            product_mapping[product_id] = {
                'title': product.get('title'),
                'category': product.get('category'),
                'brand': product.get('brand'),
                'rating': product.get('rating')
            }
    
    print(f"[INFO] Created mapping for {len(product_mapping)} products")
    return product_mapping


def enrich_sales_data_with_api(transactions, product_mapping):
    """
    Enriches transaction data with API product information
   
    """
    enriched_transactions = []
    
    for transaction in transactions:
        enriched_tx = transaction.copy()
        
        try:
            # Extract numeric ID from ProductID (e.g., P101 → 101)
            product_id_str = transaction.get('ProductID', '')
            # Remove leading 'P' and convert to integer
            numeric_id = int(product_id_str.lstrip('P'))
            
            # Check if ID exists in product mapping
            if numeric_id in product_mapping:
                api_data = product_mapping[numeric_id]
                enriched_tx['API_Category'] = api_data.get('category')
                enriched_tx['API_Brand'] = api_data.get('brand')
                enriched_tx['API_Rating'] = api_data.get('rating')
                enriched_tx['API_Match'] = True
            else:
                # Product not found in API
                enriched_tx['API_Category'] = None
                enriched_tx['API_Brand'] = None
                enriched_tx['API_Rating'] = None
                enriched_tx['API_Match'] = False
        
        except (ValueError, AttributeError, TypeError) as e:
            # Error extracting numeric ID or handling data
            enriched_tx['API_Category'] = None
            enriched_tx['API_Brand'] = None
            enriched_tx['API_Rating'] = None
            enriched_tx['API_Match'] = False
        
        enriched_transactions.append(enriched_tx)
    
    return enriched_transactions


def save_enriched_data(enriched_transactions, filename='data/enriched_sales_data.txt'):
    """
    Saves enriched transactions back to file
      
    """
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            # Write header
            header = 'TransactionID|Date|ProductID|ProductName|Quantity|UnitPrice|CustomerID|Region|API_Category|API_Brand|API_Rating|API_Match\n'
            f.write(header)
            
            # Write data rows
            for tx in enriched_transactions:
                row = (
                    f"{tx.get('TransactionID', '')}|"
                    f"{tx.get('Date', '')}|"
                    f"{tx.get('ProductID', '')}|"
                    f"{tx.get('ProductName', '')}|"
                    f"{tx.get('Quantity', '')}|"
                    f"{tx.get('UnitPrice', '')}|"
                    f"{tx.get('CustomerID', '')}|"
                    f"{tx.get('Region', '')}|"
                    f"{tx.get('API_Category', '')}|"
                    f"{tx.get('API_Brand', '')}|"
                    f"{tx.get('API_Rating', '')}|"
                    f"{tx.get('API_Match', '')}\n"
                )
                f.write(row)
        
        print(f"[SUCCESS] Saved {len(enriched_transactions)} enriched transactions to {filename}")
        return True
    
    except IOError as e:
        print(f"[ERROR] Failed to write file {filename}: {str(e)}")
        return False
    except Exception as e:
        print(f"[ERROR] Unexpected error saving file: {str(e)}")
        return False
