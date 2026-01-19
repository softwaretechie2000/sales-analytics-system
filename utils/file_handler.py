"""
File Handler Module
Handles reading and writing data files
"""

import json


def read_sales_data(filename):
    """Read sales data from file with encoding handling."""
    encodings = ['utf-8', 'latin-1', 'cp1252']
    raw_lines = []
    try:
        for encoding in encodings:
            try:
                with open(filename, 'r', encoding=encoding) as file:
                    lines = file.readlines()
                    for i, line in enumerate(lines):
                        line = line.strip()
                        if i == 0 or not line:
                            continue
                        raw_lines.append(line)
                    return raw_lines
            except (UnicodeDecodeError, UnicodeEncodeError):
                continue
        raise ValueError(f"Unable to read file {filename} with any supported encoding")
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found")
        return []
    except Exception as e:
        print(f"Error reading file: {e}")
        return []


def parse_transactions(raw_lines):
    """Parse raw lines into transaction dictionaries."""
    transactions = []
    for line in raw_lines:
        try:
            fields = line.split('|')
            if len(fields) != 8:
                continue
            transaction_id = fields[0].strip()
            date = fields[1].strip()
            product_id = fields[2].strip()
            product_name = fields[3].strip().replace(',', '')
            quantity_str = fields[4].strip().replace(',', '')
            unit_price_str = fields[5].strip().replace(',', '')
            customer_id = fields[6].strip()
            region = fields[7].strip()
            quantity = int(quantity_str)
            unit_price = float(unit_price_str)
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
        except (ValueError, IndexError):
            continue
    return transactions


def validate_and_filter(transactions, region=None, min_amount=None, max_amount=None):
    """Validate and filter transactions."""
    total_input = len(transactions)
    valid_transactions = []
    invalid_count = 0
    filtered_by_region = 0
    filtered_by_amount = 0
    
    for transaction in transactions:
        required_fields = ['TransactionID', 'Date', 'ProductID', 'ProductName',
                          'Quantity', 'UnitPrice', 'CustomerID', 'Region']
        if not all(field in transaction for field in required_fields):
            invalid_count += 1
            continue
        if not transaction['TransactionID'].startswith('T'):
            invalid_count += 1
            continue
        if not transaction['ProductID'].startswith('P'):
            invalid_count += 1
            continue
        if not transaction['CustomerID'].startswith('C'):
            invalid_count += 1
            continue
        if transaction['Quantity'] <= 0 or transaction['UnitPrice'] <= 0:
            invalid_count += 1
            continue
        valid_transactions.append(transaction)
    
    print("\n" + "="*60)
    print("AVAILABLE FILTER OPTIONS")
    print("="*60)
    available_regions = list(set(t['Region'] for t in valid_transactions))
    available_regions.sort()
    print(f"\nAvailable Regions: {', '.join(available_regions)}")
    amounts = [t['Quantity'] * t['UnitPrice'] for t in valid_transactions]
    if amounts:
        min_transaction = min(amounts)
        max_transaction = max(amounts)
        print(f"Transaction Amount Range: ${min_transaction:,.2f} to ${max_transaction:,.2f}")
    print("="*60 + "\n")
    
    filtered_transactions = valid_transactions.copy()
    if region:
        initial_count = len(filtered_transactions)
        filtered_transactions = [t for t in filtered_transactions if t['Region'] == region]
        filtered_by_region = initial_count - len(filtered_transactions)
        print(f"After region filter ({region}): {len(filtered_transactions)} records")
    if min_amount is not None:
        initial_count = len(filtered_transactions)
        filtered_transactions = [t for t in filtered_transactions if t['Quantity'] * t['UnitPrice'] >= min_amount]
        filtered_by_amount += initial_count - len(filtered_transactions)
        print(f"After min_amount filter (>= ${min_amount:,.2f}): {len(filtered_transactions)} records")
    if max_amount is not None:
        initial_count = len(filtered_transactions)
        filtered_transactions = [t for t in filtered_transactions if t['Quantity'] * t['UnitPrice'] <= max_amount]
        filtered_by_amount += initial_count - len(filtered_transactions)
        print(f"After max_amount filter (<= ${max_amount:,.2f}): {len(filtered_transactions)} records")
    
    filter_summary = {
        'total_input': total_input,
        'invalid': invalid_count,
        'valid': len(valid_transactions),
        'filtered_by_region': filtered_by_region,
        'filtered_by_amount': filtered_by_amount,
        'final_count': len(filtered_transactions)
    }
    return filtered_transactions, invalid_count, filter_summary


def write_file(file_path, data):
    try:
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(data)
        print(f"Successfully written to {file_path}")
        return True
    except Exception as e:
        print(f"Error writing to file: {e}")
        return False


def write_json_file(file_path, data):
    try:
        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump(data, file, indent=2)
        print(f"Successfully written JSON to {file_path}")
        return True
    except Exception as e:
        print(f"Error writing JSON file: {e}")
        return False


def read_json_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return json.load(file)
    except Exception as e:
        print(f"Error reading JSON file: {e}")
        return None
