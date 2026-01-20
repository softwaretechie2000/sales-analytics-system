"""
Data Processor Module

This module provides comprehensive functionality for data cleaning, validation,
and analysis of sales transactions. It includes:
- Data cleaning and validation
- Sales analysis by region, product, and customer
- Trend analysis
- Report generation
"""

import logging
from typing import List, Dict, Any, Tuple, Optional, Set
from datetime import datetime
from collections import defaultdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MIN_REQUIRED_FIELDS = 8
TRANSACTION_ID_PREFIX = 'T'
DEFAULT_TOP_N = 10
DEFAULT_LOW_PRODUCT_THRESHOLD = 10
DEFAULT_OUTPUT_FILE = 'output/sales_report.txt'
REPORT_LINE_WIDTH = 60

def clean_sales_data(raw_data: List[str]) -> Tuple[List[Dict[str, Any]], int, int]:
    """
    Cleans and validates sales transaction data.
    
    Processes raw pipe-delimited data, validates each record, and returns
    cleaned data with calculated total sales.
    
    Args:
        raw_data: List of raw transaction lines in pipe-delimited format.
    
    Returns:
        Tuple containing:
        - List of cleaned transaction dictionaries
        - Total records parsed
        - Count of invalid records removed
    """
    valid_records = []
    invalid_count = 0
    total_count = 0
    
    for line in raw_data:
        line = line.strip()
        if not line or line.startswith('TransactionID'):
            continue
        
        total_count += 1
        fields = line.split('|')
        if len(fields) < MIN_REQUIRED_FIELDS:
            invalid_count += 1
            continue
        
        try:
            transaction_id = fields[0].strip()
            date = fields[1].strip()
            product_id = fields[2].strip()
            product_name = fields[3].strip()
            quantity_str = fields[4].strip()
            unit_price_str = fields[5].strip()
            customer_id = fields[6].strip()
            region = fields[7].strip()
            
            if not transaction_id.startswith(TRANSACTION_ID_PREFIX):
                invalid_count += 1
                continue
            if not customer_id:
                invalid_count += 1
                continue
            if not region:
                invalid_count += 1
                continue
            
            quantity_str = quantity_str.replace(',', '')
            quantity = int(quantity_str)
            if quantity <= 0:
                invalid_count += 1
                continue
            
            unit_price_str = unit_price_str.replace(',', '')
            unit_price = float(unit_price_str)
            if unit_price <= 0:
                invalid_count += 1
                continue
            
            product_name = product_name.replace(',', '')
            
            cleaned_record = {
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
            
            valid_records.append(cleaned_record)
        
        except (ValueError, IndexError):
            invalid_count += 1
            continue
    
    return valid_records, total_count, invalid_count


def print_cleaning_summary(total_records: int, invalid_records: int, valid_records: int) -> None:
    """
    Logs data cleaning summary statistics.
    
    Args:
        total_records: Total records processed.
        invalid_records: Count of invalid records removed.
        valid_records: Count of valid records retained.
    """
    logger.info("\n" + "="*50)
    logger.info("DATA CLEANING SUMMARY")
    logger.info("="*50)
    logger.info(f"Total records parsed: {total_records}")
    logger.info(f"Invalid records removed: {invalid_records}")
    logger.info(f"Valid records after cleaning: {valid_records}")
    logger.info("="*50 + "\n")


def analyze_sales_by_region(records: List[Dict[str, Any]]) -> Dict[str, float]:
    """
    Analyzes and aggregates sales data by region.
    
    Args:
        records: List of cleaned transaction dictionaries.
    
    Returns:
        Dictionary mapping region names to total sales amounts.
    """
    region_sales = defaultdict(float)
    
    for record in records:
        region_sales[record['Region']] += record['TotalSales']
    
    return dict(region_sales)


def analyze_sales_by_product(records: List[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    """
    Analyzes and aggregates sales data by product.
    
    Args:
        records: List of cleaned transaction dictionaries.
    
    Returns:
        Dictionary mapping product names to stats dict with 'total' and 'quantity' keys.
    """
    product_sales = defaultdict(lambda: {'total': 0.0, 'quantity': 0})
    
    for record in records:
        product_name = record['ProductName']
        product_sales[product_name]['total'] += record['TotalSales']
        product_sales[product_name]['quantity'] += record['Quantity']
    
    return dict(product_sales)


def analyze_sales_by_customer(records: List[Dict[str, Any]]) -> Dict[str, float]:
    """
    Analyzes and aggregates sales data by customer.
    
    Args:
        records: List of cleaned transaction dictionaries.
    
    Returns:
        Dictionary mapping customer IDs to total sales amounts.
    """
    customer_sales = defaultdict(float)
    
    for record in records:
        customer_sales[record['CustomerID']] += record['TotalSales']
    
    return dict(customer_sales)


def get_top_customers(records: List[Dict[str, Any]], top_n: int = DEFAULT_TOP_N) -> List[Tuple[str, float]]:
    """
    Retrieves top N customers by total spending.
    
    Args:
        records: List of cleaned transaction dictionaries.
        top_n: Number of top customers to return. Defaults to DEFAULT_TOP_N.
    
    Returns:
        List of (customer_id, total_spent) tuples, sorted by spending descending.
    """
    customer_sales = analyze_sales_by_customer(records)
    return sorted(customer_sales.items(), key=lambda x: x[1], reverse=True)[:top_n]


def get_top_products(records: List[Dict[str, Any]], top_n: int = DEFAULT_TOP_N) -> List[Tuple[str, Dict[str, float]]]:
    """
    Retrieves top N products by total sales value.
    
    Args:
        records: List of cleaned transaction dictionaries.
        top_n: Number of top products to return. Defaults to DEFAULT_TOP_N.
    
    Returns:
        List of (product_name, stats_dict) tuples, sorted by total sales descending.
    """
    product_sales = analyze_sales_by_product(records)
    return sorted(product_sales.items(), key=lambda x: x[1]['total'], reverse=True)[:top_n]


def calculate_statistics(records: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Calculates comprehensive sales statistics.
    
    Args:
        records: List of cleaned transaction dictionaries.
    
    Returns:
        Dictionary containing sales statistics (total, average, counts, etc.)
        or None if records list is empty.
    """
    if not records:
        return None
    
    total_sales = sum(record['TotalSales'] for record in records)
    total_transactions = len(records)
    average_transaction = total_sales / total_transactions if total_transactions > 0 else 0
    total_quantity = sum(record['Quantity'] for record in records)
    
    return {
        'total_sales': total_sales,
        'total_transactions': total_transactions,
        'average_transaction_value': average_transaction,
        'total_quantity_sold': total_quantity,
        'num_unique_customers': len(set(r['CustomerID'] for r in records)),
        'num_unique_products': len(set(r['ProductID'] for r in records)),
        'num_unique_regions': len(set(r['Region'] for r in records))
    }


def calculate_total_revenue(transactions: List[Dict[str, Any]]) -> float:
    """
    Calculates total revenue from all transactions.
    
    Args:
        transactions: List of transaction dictionaries.
    
    Returns:
        Total revenue as float.
    """
    return sum(t['Quantity'] * t['UnitPrice'] for t in transactions)


def region_wise_sales(transactions: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Analyzes sales performance by region with percentages.
    
    Args:
        transactions: List of transaction dictionaries.
    
    Returns:
        Dictionary mapping regions to stats dict with 'total_sales', 
        'transaction_count', and 'percentage' keys, sorted by sales descending.
    """
    total_revenue = calculate_total_revenue(transactions)
    region_stats = defaultdict(lambda: {'total_sales': 0.0, 'transaction_count': 0})
    
    for transaction in transactions:
        region = transaction['Region']
        sales = transaction['Quantity'] * transaction['UnitPrice']
        region_stats[region]['total_sales'] += sales
        region_stats[region]['transaction_count'] += 1
    
    for region in region_stats:
        percentage = (region_stats[region]['total_sales'] / total_revenue * 100) if total_revenue > 0 else 0
        region_stats[region]['percentage'] = round(percentage, 2)
    
    return dict(sorted(
        region_stats.items(),
        key=lambda x: x[1]['total_sales'],
        reverse=True
    ))


def top_selling_products(transactions: List[Dict[str, Any]], n: int = 5) -> List[Tuple[str, int, float]]:
    """
    Finds top N products by total quantity sold.
    
    Args:
        transactions: List of transaction dictionaries.
        n: Number of top products to return. Defaults to 5.
    
    Returns:
        List of (product_name, total_quantity, total_revenue) tuples, 
        sorted by quantity descending.
    """
    product_stats = defaultdict(lambda: {'total_quantity': 0, 'total_revenue': 0.0})
    
    for transaction in transactions:
        product_name = transaction['ProductName']
        quantity = transaction['Quantity']
        revenue = transaction['Quantity'] * transaction['UnitPrice']
        product_stats[product_name]['total_quantity'] += quantity
        product_stats[product_name]['total_revenue'] += revenue
    
    product_list = [
        (name, stats['total_quantity'], stats['total_revenue'])
        for name, stats in product_stats.items()
    ]
    product_list.sort(key=lambda x: x[1], reverse=True)
    return product_list[:n]


def customer_analysis(transactions: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Analyzes customer purchase patterns and spending.
    
    Args:
        transactions: List of transaction dictionaries.
    
    Returns:
        Dictionary mapping customer IDs to stats dict with 'total_spent',
        'purchase_count', 'avg_order_value', and 'products_bought' keys,
        sorted by total spending descending.
    """
    customer_stats = defaultdict(lambda: {
        'total_spent': 0.0,
        'purchase_count': 0,
        'products_bought': set()
    })
    
    for transaction in transactions:
        customer_id = transaction['CustomerID']
        amount_spent = transaction['Quantity'] * transaction['UnitPrice']
        customer_stats[customer_id]['total_spent'] += amount_spent
        customer_stats[customer_id]['purchase_count'] += 1
        customer_stats[customer_id]['products_bought'].add(transaction['ProductName'])
    
    for stats in customer_stats.values():
        stats['avg_order_value'] = round(
            stats['total_spent'] / stats['purchase_count'],
            2
        )
        stats['products_bought'] = sorted(list(stats['products_bought']))
    
    return dict(sorted(
        customer_stats.items(),
        key=lambda x: x[1]['total_spent'],
        reverse=True
    ))


def daily_sales_trend(transactions: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Analyzes sales trends by date.
    
    Args:
        transactions: List of transaction dictionaries.
    
    Returns:
        Dictionary mapping dates to stats dict with 'revenue',
        'transaction_count', and 'unique_customers' keys,
        sorted chronologically by date.
    """
    daily_stats = defaultdict(lambda: {
        'revenue': 0.0,
        'transaction_count': 0,
        'unique_customers': set()
    })
    
    for transaction in transactions:
        date = transaction['Date']
        revenue = transaction['Quantity'] * transaction['UnitPrice']
        daily_stats[date]['revenue'] += revenue
        daily_stats[date]['transaction_count'] += 1
        daily_stats[date]['unique_customers'].add(transaction['CustomerID'])
    
    for stats in daily_stats.values():
        stats['unique_customers'] = len(stats['unique_customers'])
    
    return dict(sorted(daily_stats.items()))


def find_peak_sales_day(transactions: List[Dict[str, Any]]) -> Optional[Tuple[str, float, int]]:
    """
    Identifies the date with highest revenue.
    
    Args:
        transactions: List of transaction dictionaries.
    
    Returns:
        Tuple of (date, revenue, transaction_count) for peak day, or None if empty.
    """
    daily_sales = daily_sales_trend(transactions)
    if not daily_sales:
        return None
    date, stats = max(daily_sales.items(), key=lambda x: x[1]['revenue'])
    return (date, stats['revenue'], stats['transaction_count'])


def low_performing_products(
    transactions: List[Dict[str, Any]],
    threshold: int = DEFAULT_LOW_PRODUCT_THRESHOLD
) -> List[Tuple[str, int, float]]:
    """
    Identifies products with low sales below threshold.
    
    Args:
        transactions: List of transaction dictionaries.
        threshold: Minimum quantity threshold. Defaults to DEFAULT_LOW_PRODUCT_THRESHOLD.
    
    Returns:
        List of (product_name, total_quantity, total_revenue) tuples
        for products below threshold, sorted by quantity ascending.
    """
    product_stats = defaultdict(lambda: {'total_quantity': 0, 'total_revenue': 0.0})
    
    for transaction in transactions:
        product_name = transaction['ProductName']
        quantity = transaction['Quantity']
        revenue = transaction['Quantity'] * transaction['UnitPrice']
        product_stats[product_name]['total_quantity'] += quantity
        product_stats[product_name]['total_revenue'] += revenue
    
    product_list = [
        (name, stats['total_quantity'], stats['total_revenue'])
        for name, stats in product_stats.items()
        if stats['total_quantity'] < threshold
    ]
    product_list.sort(key=lambda x: x[1])
    return product_list


def generate_sales_report(
    transactions: List[Dict[str, Any]],
    enriched_transactions: Optional[List[Dict[str, Any]]] = None,
    output_file: str = DEFAULT_OUTPUT_FILE
) -> bool:
    """
    Generates a comprehensive formatted text sales report.
    
    Creates a detailed report including overall summary, regional performance,
    top products, top customers, daily trends, and API enrichment summary.
    
    Args:
        transactions: List of cleaned transaction dictionaries.
        enriched_transactions: Optional list of enriched transaction dictionaries.
        output_file: Output file path. Defaults to DEFAULT_OUTPUT_FILE.
    
    Returns:
        True if report generated successfully, False otherwise.
    """
    if not transactions:
        return False
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            total_revenue = calculate_total_revenue(transactions)
            total_transactions = len(transactions)
            avg_order_value = total_revenue / total_transactions if total_transactions > 0 else 0
            dates = sorted([t['Date'] for t in transactions])
            date_range = f"{dates[0]} to {dates[-1]}" if dates else "N/A"
            
            _write_report_header(f, total_transactions)
            _write_overall_summary(f, total_revenue, total_transactions, avg_order_value, date_range)
            _write_region_performance(f, transactions)
            _write_top_products_section(f, transactions)
            _write_top_customers_section(f, transactions)
            _write_daily_trend_section(f, transactions)
            _write_product_performance_section(f, transactions)
            _write_api_enrichment_summary(f, enriched_transactions)
            
            f.write("\n" + "=" * REPORT_LINE_WIDTH + "\n")
            f.write("END OF REPORT\n")
            f.write("=" * REPORT_LINE_WIDTH + "\n")
        
        logger.info(f"Successfully generated sales report: {output_file}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to generate report: {str(e)}")
        return False


def _write_report_header(f: Any, total_transactions: int) -> None:
    """Writes the report header section."""
    f.write("=" * REPORT_LINE_WIDTH + "\n")
    f.write("           SALES ANALYTICS REPORT\n")
    f.write(f"         Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write(f"         Records Processed: {total_transactions}\n")
    f.write("=" * REPORT_LINE_WIDTH + "\n\n")


def _write_overall_summary(
    f: Any,
    total_revenue: float,
    total_transactions: int,
    avg_order_value: float,
    date_range: str
) -> None:
    """Writes the overall summary section."""
    f.write("OVERALL SUMMARY\n")
    f.write("-" * REPORT_LINE_WIDTH + "\n")
    f.write(f"Total Revenue:        ${total_revenue:,.2f}\n")
    f.write(f"Total Transactions:   {total_transactions}\n")
    f.write(f"Average Order Value:  ${avg_order_value:,.2f}\n")
    f.write(f"Date Range:           {date_range}\n\n")


def _write_region_performance(f: Any, transactions: List[Dict[str, Any]]) -> None:
    """Writes the region-wise performance section."""
    regions = region_wise_sales(transactions)
    f.write("REGION-WISE PERFORMANCE\n")
    f.write("-" * REPORT_LINE_WIDTH + "\n")
    f.write(f"{'Region':<15} {'Sales':<18} {'% of Total':<15} {'Transactions':<12}\n")
    f.write("-" * REPORT_LINE_WIDTH + "\n")
    for region, stats in regions.items():
        region_display = region if region else "(Unknown)"
        f.write(f"{region_display:<15} ${stats['total_sales']:>15,.2f}  "
               f"{stats['percentage']:>6.2f}%     {stats['transaction_count']:>6}\n")
    f.write("\n")


def _write_top_products_section(f: Any, transactions: List[Dict[str, Any]]) -> None:
    """Writes the top products section."""
    top_products = top_selling_products(transactions, n=5)
    f.write("TOP 5 PRODUCTS\n")
    f.write("-" * REPORT_LINE_WIDTH + "\n")
    f.write(f"{'Rank':<6} {'Product Name':<25} {'Quantity':<12} {'Revenue':<15}\n")
    f.write("-" * REPORT_LINE_WIDTH + "\n")
    for idx, (name, qty, revenue) in enumerate(top_products, 1):
        f.write(f"{idx:<6} {name:<25} {qty:<12} ${revenue:>13,.2f}\n")
    f.write("\n")


def _write_top_customers_section(f: Any, transactions: List[Dict[str, Any]]) -> None:
    """Writes the top customers section."""
    customers = customer_analysis(transactions)
    top_5_customers = list(customers.items())[:5]
    f.write("TOP 5 CUSTOMERS\n")
    f.write("-" * REPORT_LINE_WIDTH + "\n")
    f.write(f"{'Rank':<6} {'Customer ID':<15} {'Total Spent':<18} {'Order Count':<12}\n")
    f.write("-" * REPORT_LINE_WIDTH + "\n")
    for idx, (cid, stats) in enumerate(top_5_customers, 1):
        f.write(f"{idx:<6} {cid:<15} ${stats['total_spent']:>15,.2f}  "
               f"{stats['purchase_count']:>6}\n")
    f.write("\n")


def _write_daily_trend_section(f: Any, transactions: List[Dict[str, Any]]) -> None:
    """Writes the daily sales trend section."""
    daily_trend = daily_sales_trend(transactions)
    f.write("DAILY SALES TREND\n")
    f.write("-" * REPORT_LINE_WIDTH + "\n")
    f.write(f"{'Date':<15} {'Revenue':<18} {'Transactions':<15} {'Unique Customers':<12}\n")
    f.write("-" * REPORT_LINE_WIDTH + "\n")
    for date, stats in list(daily_trend.items())[:10]:  # Show first 10 days
        f.write(f"{date:<15} ${stats['revenue']:>15,.2f}  "
               f"{stats['transaction_count']:>6}         {stats['unique_customers']:>6}\n")
    if len(daily_trend) > 10:
        f.write(f"... and {len(daily_trend) - 10} more days\n")
    f.write("\n")


def _write_product_performance_section(f: Any, transactions: List[Dict[str, Any]]) -> None:
    """Writes the product performance analysis section."""
    peak_day = find_peak_sales_day(transactions)
    low_performers = low_performing_products(transactions, threshold=DEFAULT_LOW_PRODUCT_THRESHOLD)
    regions = region_wise_sales(transactions)
    
    f.write("PRODUCT PERFORMANCE ANALYSIS\n")
    f.write("-" * REPORT_LINE_WIDTH + "\n")
    if peak_day:
        f.write(f"Best Selling Day:     {peak_day[0]} (${peak_day[1]:,.2f}, {peak_day[2]} transactions)\n")
    f.write(f"Low Performing Products (< {DEFAULT_LOW_PRODUCT_THRESHOLD} units): {len(low_performers)} products\n")
    if low_performers:
        for product_name, qty, revenue in low_performers[:5]:
            f.write(f"  - {product_name}: {qty} units (${revenue:,.2f})\n")
    
    f.write("\nAverage Transaction Value per Region:\n")
    for region, stats in regions.items():
        region_display = region if region else "(Unknown)"
        if stats['transaction_count'] > 0:
            avg = stats['total_sales'] / stats['transaction_count']
            f.write(f"  {region_display}: ${avg:,.2f}\n")
    f.write("\n")


def _write_api_enrichment_summary(
    f: Any,
    enriched_transactions: Optional[List[Dict[str, Any]]]
) -> None:
    """Writes the API enrichment summary section."""
    f.write("API ENRICHMENT SUMMARY\n")
    f.write("-" * REPORT_LINE_WIDTH + "\n")
    if enriched_transactions:
        total_enriched = len(enriched_transactions)
        matched = sum(1 for tx in enriched_transactions if tx.get('API_Match', False))
        success_rate = (matched / total_enriched * 100) if total_enriched > 0 else 0
        
        f.write(f"Total Transactions Processed: {total_enriched}\n")
        f.write(f"Successfully Enriched:       {matched}\n")
        f.write(f"Enrichment Success Rate:     {success_rate:.2f}%\n")
        
        # Find products that couldn't be enriched
        unmatched_products: Set[str] = set()
        for tx in enriched_transactions:
            if not tx.get('API_Match', False):
                unmatched_products.add(tx.get('ProductID', 'Unknown'))
        if unmatched_products:
            f.write(f"\nProducts Not Enriched ({len(unmatched_products)} products):\n")
            for product_id in sorted(list(unmatched_products))[:10]:
                f.write(f"  - {product_id}\n")
            if len(unmatched_products) > 10:
                f.write(f"  ... and {len(unmatched_products) - 10} more\n")
    else:
        f.write("No enriched data available\n")