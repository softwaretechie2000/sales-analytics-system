"""
API Handler Module

This module handles integration with external APIs for product information.
It provides functionality to fetch products from DummyJSON API and enrich
sales transaction data with API product details.
"""

import requests
import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

# =====================================================
# Constants
# =====================================================

API_BASE_URL = 'https://dummyjson.com/products'
API_PRODUCT_LIMIT = 100
API_TIMEOUT_SECONDS = 10
PRODUCT_ID_PREFIX = 'P'
ENRICHED_DATA_FILENAME = 'data/enriched_sales_data.txt'

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# =====================================================
# DummyJSON API Integration
# =====================================================

def fetch_all_products() -> List[Dict[str, Any]]:
    """
    Fetches all products from DummyJSON API.
    
    Returns:
        List[Dict[str, Any]]: List of product dictionaries containing id, title,
                              category, brand, price, and rating.
                              Returns empty list on failure.
    """
    try:
        logger.info("Fetching all products from DummyJSON API...")
        
        # Fetch products from DummyJSON with specified limit
        response = requests.get(
            f'{API_BASE_URL}?limit={API_PRODUCT_LIMIT}',
            timeout=API_TIMEOUT_SECONDS
        )
        response.raise_for_status()
        
        data = response.json()
        products = data.get('products', [])
        
        # Extract relevant fields from each product
        product_list = [
            {
                'id': product.get('id'),
                'title': product.get('title'),
                'category': product.get('category'),
                'brand': product.get('brand'),
                'price': product.get('price'),
                'rating': product.get('rating')
            }
            for product in products
        ]
        
        logger.info(f"Successfully fetched {len(product_list)} products from API")
        return product_list
        
    except requests.exceptions.ConnectionError:
        logger.error("Connection error: Unable to connect to DummyJSON API")
        return []
    except requests.exceptions.Timeout:
        logger.error(f"Timeout error: API request exceeded {API_TIMEOUT_SECONDS}s timeout")
        return []
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error: {str(e)}")
        return []
    except ValueError as e:
        logger.error(f"JSON parsing error: {str(e)}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error fetching products: {str(e)}")
        return []


def create_product_mapping(api_products: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    """
    Creates a mapping of product IDs to product information.
    
    Args:
        api_products: List of product dictionaries from API.
    
    Returns:
        Dict[int, Dict[str, Any]]: Dictionary mapping product IDs to product details
                                    (title, category, brand, rating).
    """
    product_mapping = {
        product.get('id'): {
            'title': product.get('title'),
            'category': product.get('category'),
            'brand': product.get('brand'),
            'rating': product.get('rating')
        }
        for product in api_products
        if product.get('id') is not None
    }
    
    logger.info(f"Created product mapping for {len(product_mapping)} products")
    return product_mapping


def enrich_sales_data_with_api(
    transactions: List[Dict[str, Any]],
    product_mapping: Dict[int, Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Enriches transaction data with API product information.
    
    Extracts numeric product ID from ProductID field, matches against product
    mapping, and adds API category, brand, rating, and match status to each
    transaction.
    
    Args:
        transactions: List of transaction dictionaries.
        product_mapping: Dictionary mapping product IDs to product info.
    
    Returns:
        List[Dict[str, Any]]: List of enriched transaction dictionaries with
                              API fields added.
    """
    enriched_transactions = []
    
    for transaction in transactions:
        enriched_tx = transaction.copy()
        
        # Try to extract numeric ID and enrich with API data
        numeric_id = _extract_product_id(transaction.get('ProductID', ''))
        
        if numeric_id is not None and numeric_id in product_mapping:
            _add_api_fields(enriched_tx, product_mapping[numeric_id], api_match=True)
        else:
            _add_api_fields(enriched_tx, None, api_match=False)
        
        enriched_transactions.append(enriched_tx)
    
    return enriched_transactions


def _extract_product_id(product_id_str: str) -> Optional[int]:
    """
    Extracts numeric product ID from string (e.g., 'P101' → 101).
    
    Args:
        product_id_str: Product ID string with prefix.
    
    Returns:
        Optional[int]: Numeric product ID or None if extraction fails.
    """
    try:
        return int(product_id_str.lstrip(PRODUCT_ID_PREFIX))
    except (ValueError, AttributeError, TypeError):
        return None


def _add_api_fields(
    transaction: Dict[str, Any],
    api_data: Optional[Dict[str, Any]],
    api_match: bool
) -> None:
    """
    Adds API enrichment fields to a transaction dictionary (in-place).
    
    Args:
        transaction: Transaction dictionary to enrich.
        api_data: Product data from API or None if not matched.
        api_match: Whether the product was found in API mapping.
    """
    if api_data:
        transaction['API_Category'] = api_data.get('category')
        transaction['API_Brand'] = api_data.get('brand')
        transaction['API_Rating'] = api_data.get('rating')
    else:
        transaction['API_Category'] = None
        transaction['API_Brand'] = None
        transaction['API_Rating'] = None
    
    transaction['API_Match'] = api_match


def save_enriched_data(
    enriched_transactions: List[Dict[str, Any]],
    filename: str = ENRICHED_DATA_FILENAME
) -> bool:
    """
    Saves enriched transactions to a pipe-delimited text file.
    
    Args:
        enriched_transactions: List of enriched transaction dictionaries.
        filename: Output file path. Defaults to ENRICHED_DATA_FILENAME.
    
    Returns:
        bool: True if save successful, False otherwise.
    """
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            # Write header
            header = 'TransactionID|Date|ProductID|ProductName|Quantity|UnitPrice|CustomerID|Region|API_Category|API_Brand|API_Rating|API_Match\n'
            f.write(header)
            
            # Write data rows
            for tx in enriched_transactions:
                row = _format_transaction_row(tx)
                f.write(row)
        
        logger.info(f"Successfully saved {len(enriched_transactions)} enriched transactions to {filename}")
        return True
    
    except IOError as e:
        logger.error(f"Failed to write file {filename}: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error saving file: {str(e)}")
        return False


def _format_transaction_row(transaction: Dict[str, Any]) -> str:
    """
    Formats a transaction dictionary as a pipe-delimited row.
    
    Args:
        transaction: Transaction dictionary.
    
    Returns:
        str: Formatted row string with newline.
    """
    return (
        f"{transaction.get('TransactionID', '')}|"
        f"{transaction.get('Date', '')}|"
        f"{transaction.get('ProductID', '')}|"
        f"{transaction.get('ProductName', '')}|"
        f"{transaction.get('Quantity', '')}|"
        f"{transaction.get('UnitPrice', '')}|"
        f"{transaction.get('CustomerID', '')}|"
        f"{transaction.get('Region', '')}|"
        f"{transaction.get('API_Category', '')}|"
        f"{transaction.get('API_Brand', '')}|"
        f"{transaction.get('API_Rating', '')}|"
        f"{transaction.get('API_Match', '')}\n"
    )
