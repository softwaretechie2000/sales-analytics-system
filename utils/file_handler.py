"""
File Handler Module

This module provides file I/O operations for the sales analytics system,
including reading, parsing, validating, and writing transaction data in
both plain text and JSON formats.
"""

import json
import logging
from typing import List, Dict, Any, Optional, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =====================================================
# Constants
# =====================================================

SUPPORTED_ENCODINGS = ['utf-8', 'latin-1', 'cp1252']
EXPECTED_FIELD_COUNT = 8
TRANSACTION_ID_PREFIX = 'T'
PRODUCT_ID_PREFIX = 'P'
CUSTOMER_ID_PREFIX = 'C'
DELIMITER = '|'
REPORT_LINE_WIDTH = 60

# Required field names for transaction validation
REQUIRED_TRANSACTION_FIELDS = [
    'TransactionID', 'Date', 'ProductID', 'ProductName',
    'Quantity', 'UnitPrice', 'CustomerID', 'Region'
]


def read_sales_data(filename: str) -> List[str]:
    """
    Reads sales data from file with automatic encoding detection.
    
    Attempts to read the file using multiple supported encodings in order,
    skipping empty lines and the header row.
    
    Args:
        filename: Path to the sales data file.
    
    Returns:
        List of raw transaction lines (header excluded, empty lines removed).
        Returns empty list if file cannot be read or doesn't exist.
    """
    try:
        for encoding in SUPPORTED_ENCODINGS:
            try:
                with open(filename, 'r', encoding=encoding) as file:
                    raw_lines = []
                    for i, line in enumerate(file):
                        line = line.strip()
                        # Skip header (first line) and empty lines
                        if i == 0 or not line:
                            continue
                        raw_lines.append(line)
                    
                    logger.info(f"Successfully read {len(raw_lines)} records from {filename} using {encoding} encoding")
                    return raw_lines
            
            except (UnicodeDecodeError, UnicodeEncodeError):
                continue
        
        logger.error(f"Unable to read file {filename} with any supported encoding")
        return []
    
    except FileNotFoundError:
        logger.error(f"File not found: {filename}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error reading file {filename}: {str(e)}")
        return []


def parse_transactions(raw_lines: List[str]) -> List[Dict[str, Any]]:
    """
    Parses raw pipe-delimited lines into transaction dictionaries.
    
    Each line is expected to contain exactly 8 pipe-delimited fields.
    Lines with incorrect format are silently skipped.
    
    Args:
        raw_lines: List of raw transaction lines in pipe-delimited format.
    
    Returns:
        List of parsed transaction dictionaries with calculated TotalSales field.
    """
    transactions = []
    
    for line in raw_lines:
        try:
            fields = line.split(DELIMITER)
            
            if len(fields) != EXPECTED_FIELD_COUNT:
                continue
            
            # Extract and clean fields
            transaction_id = fields[0].strip()
            date = fields[1].strip()
            product_id = fields[2].strip()
            product_name = fields[3].strip().replace(',', '')
            quantity_str = fields[4].strip().replace(',', '')
            unit_price_str = fields[5].strip().replace(',', '')
            customer_id = fields[6].strip()
            region = fields[7].strip()
            
            # Convert numeric fields
            quantity = int(quantity_str)
            unit_price = float(unit_price_str)
            
            # Create transaction dictionary
            transaction = {
                'TransactionID': transaction_id,
                'Date': date,
                'ProductID': product_id,
                'ProductName': product_name,
                'Quantity': quantity,
                'UnitPrice': unit_price,
                'CustomerID': customer_id,
                'Region': region,
                'TotalSales': quantity * unit_price
            }
            
            transactions.append(transaction)
        
        except (ValueError, IndexError) as e:
            continue
    
    logger.info(f"Parsed {len(transactions)} transactions from {len(raw_lines)} raw lines")
    return transactions


def _is_valid_transaction(transaction: Dict[str, Any]) -> bool:
    """
    Validates a single transaction against all rules.
    
    Checks for required fields, valid ID prefixes, and positive numeric values.
    
    Args:
        transaction: Transaction dictionary to validate.
    
    Returns:
        True if transaction is valid, False otherwise.
    """
    # Check all required fields exist
    if not all(field in transaction for field in REQUIRED_TRANSACTION_FIELDS):
        return False
    
    # Validate ID prefixes
    if not transaction['TransactionID'].startswith(TRANSACTION_ID_PREFIX):
        return False
    if not transaction['ProductID'].startswith(PRODUCT_ID_PREFIX):
        return False
    if not transaction['CustomerID'].startswith(CUSTOMER_ID_PREFIX):
        return False
    
    # Validate numeric fields are positive
    if transaction['Quantity'] <= 0 or transaction['UnitPrice'] <= 0:
        return False
    
    return True


def validate_and_filter(
    transactions: List[Dict[str, Any]],
    region: Optional[str] = None,
    min_amount: Optional[float] = None,
    max_amount: Optional[float] = None
) -> Tuple[List[Dict[str, Any]], int, Dict[str, int]]:
    """
    Validates transactions and applies optional filters.
    
    First validates all transactions, then applies region and amount filters.
    Logs available filter options and filter results.
    
    Args:
        transactions: List of transaction dictionaries.
        region: Optional region to filter by.
        min_amount: Optional minimum transaction amount filter.
        max_amount: Optional maximum transaction amount filter.
    
    Returns:
        Tuple containing:
        - List of filtered and valid transactions
        - Count of invalid records
        - Dictionary with filter statistics
    """
    # Validate all transactions
    total_input = len(transactions)
    valid_transactions = [t for t in transactions if _is_valid_transaction(t)]
    invalid_count = total_input - len(valid_transactions)
    
    # Display available filter options
    _log_filter_options(valid_transactions)
    
    # Apply filters
    filtered_transactions = valid_transactions.copy()
    filtered_by_region = 0
    filtered_by_amount = 0
    
    if region:
        initial_count = len(filtered_transactions)
        filtered_transactions = [t for t in filtered_transactions if t['Region'] == region]
        filtered_by_region = initial_count - len(filtered_transactions)
        logger.info(f"After region filter ({region}): {len(filtered_transactions)} records")
    
    if min_amount is not None:
        initial_count = len(filtered_transactions)
        filtered_transactions = [
            t for t in filtered_transactions
            if t['Quantity'] * t['UnitPrice'] >= min_amount
        ]
        filtered_by_amount += initial_count - len(filtered_transactions)
        logger.info(f"After min_amount filter (>= ${min_amount:,.2f}): {len(filtered_transactions)} records")
    
    if max_amount is not None:
        initial_count = len(filtered_transactions)
        filtered_transactions = [
            t for t in filtered_transactions
            if t['Quantity'] * t['UnitPrice'] <= max_amount
        ]
        filtered_by_amount += initial_count - len(filtered_transactions)
        logger.info(f"After max_amount filter (<= ${max_amount:,.2f}): {len(filtered_transactions)} records")
    
    # Build summary
    filter_summary = {
        'total_input': total_input,
        'invalid': invalid_count,
        'valid': len(valid_transactions),
        'filtered_by_region': filtered_by_region,
        'filtered_by_amount': filtered_by_amount,
        'final_count': len(filtered_transactions)
    }
    
    return filtered_transactions, invalid_count, filter_summary


def _log_filter_options(valid_transactions: List[Dict[str, Any]]) -> None:
    """
    Logs available filter options (regions and amount range).
    
    Args:
        valid_transactions: List of valid transaction dictionaries.
    """
    logger.info("\n" + "=" * REPORT_LINE_WIDTH)
    logger.info("AVAILABLE FILTER OPTIONS")
    logger.info("=" * REPORT_LINE_WIDTH)
    
    if valid_transactions:
        # Log available regions
        regions = sorted(set(t['Region'] for t in valid_transactions))
        logger.info(f"Available Regions: {', '.join(regions)}")
        
        # Log transaction amount range
        amounts = [t['Quantity'] * t['UnitPrice'] for t in valid_transactions]
        if amounts:
            min_transaction = min(amounts)
            max_transaction = max(amounts)
            logger.info(f"Transaction Amount Range: ${min_transaction:,.2f} to ${max_transaction:,.2f}")
    
    logger.info("=" * REPORT_LINE_WIDTH + "\n")


def write_file(file_path: str, data: str) -> bool:
    """
    Writes text data to a file.
    
    Args:
        file_path: Path to the output file.
        data: Text content to write.
    
    Returns:
        True if write successful, False otherwise.
    """
    try:
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(data)
        logger.info(f"Successfully written to {file_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to write to file {file_path}: {str(e)}")
        return False


def write_json_file(file_path: str, data: Any) -> bool:
    """
    Writes data to a JSON file.
    
    Args:
        file_path: Path to the output JSON file.
        data: Data to serialize to JSON.
    
    Returns:
        True if write successful, False otherwise.
    """
    try:
        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump(data, file, indent=2)
        logger.info(f"Successfully written JSON to {file_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to write JSON file {file_path}: {str(e)}")
        return False


def read_json_file(file_path: str) -> Optional[Any]:
    """
    Reads and parses a JSON file.
    
    Args:
        file_path: Path to the JSON file to read.
    
    Returns:
        Parsed JSON data or None if read fails.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        logger.info(f"Successfully read JSON from {file_path}")
        return data
    except FileNotFoundError:
        logger.error(f"JSON file not found: {file_path}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON file {file_path}: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error reading JSON file {file_path}: {str(e)}")
        return None
