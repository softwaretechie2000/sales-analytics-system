"""
Data Processor Module
Handles data cleaning, validation, and analysis
"""

def clean_sales_data(raw_data):
    """
    Clean and validate sales data

    """
    valid_records = []
    invalid_count = 0
    total_count = 0
    
    for line in raw_data:
        line = line.strip()
        
        # Skip empty lines and header
        if not line or line.startswith('TransactionID'):
            continue
        
        total_count += 1
        
        # Split by pipe
        fields = line.split('|')
        
        # Check if we have correct number of fields (8 fields expected)
        if len(fields) < 8:
            invalid_count += 1
            continue
        
        try:
            # Extract fields
            transaction_id = fields[0].strip()
            date = fields[1].strip()
            product_id = fields[2].strip()
            product_name = fields[3].strip()
            quantity_str = fields[4].strip()
            unit_price_str = fields[5].strip()
            customer_id = fields[6].strip()
            region = fields[7].strip()
            
            # Validation: TransactionID must start with 'T'
            if not transaction_id.startswith('T'):
                invalid_count += 1
                continue
            
            # Validation: CustomerID must not be empty
            if not customer_id:
                invalid_count += 1
                continue
            
            # Validation: Region must not be empty
            if not region:
                invalid_count += 1
                continue
            
            # Clean quantity: convert and validate
            quantity_str = quantity_str.replace(',', '')
            quantity = int(quantity_str)
            
            # Validation: Quantity must be > 0
            if quantity <= 0:
                invalid_count += 1
                continue
            
            # Clean unit price: convert and validate
            unit_price_str = unit_price_str.replace(',', '')
            unit_price = float(unit_price_str)
            
            # Validation: UnitPrice must be > 0
            if unit_price <= 0:
                invalid_count += 1
                continue
            
            # Clean product name: remove commas
            product_name = product_name.replace(',', '')
            
            # Record is valid, add to cleaned data
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


def print_cleaning_summary(total_records, invalid_records, valid_records):
    """
    Print data cleaning summary
    """
    print("\n" + "="*50)
    print("DATA CLEANING SUMMARY")
    print("="*50)
    print(f"Total records parsed: {total_records}")
    print(f"Invalid records removed: {invalid_records}")
    print(f"Valid records after cleaning: {valid_records}")
    print("="*50 + "\n")


def analyze_sales_by_region(records):
    """
    Analyze total sales by region
    """
    region_sales = {}
    
    for record in records:
        region = record['Region']
        total_sales = record['TotalSales']
        
        if region not in region_sales:
            region_sales[region] = 0
        
        region_sales[region] += total_sales
    
    return region_sales


def analyze_sales_by_product(records):
    """
    Analyze total sales by product
    """
    product_sales = {}
    
    for record in records:
        product_name = record['ProductName']
        total_sales = record['TotalSales']
        
        if product_name not in product_sales:
            product_sales[product_name] = {'total': 0, 'quantity': 0}
        
        product_sales[product_name]['total'] += total_sales
        product_sales[product_name]['quantity'] += record['Quantity']
    
    return product_sales


def analyze_sales_by_customer(records):
    """
    Analyze total sales by customer
    """
    customer_sales = {}
    
    for record in records:
        customer_id = record['CustomerID']
        total_sales = record['TotalSales']
        
        if customer_id not in customer_sales:
            customer_sales[customer_id] = 0
        
        customer_sales[customer_id] += total_sales
    
    return customer_sales


def get_top_customers(records, top_n=10):
    """
    Get top customers by total spending
    """
    customer_sales = analyze_sales_by_customer(records)
    
    # Sort by total sales descending
    sorted_customers = sorted(customer_sales.items(), key=lambda x: x[1], reverse=True)
    
    return sorted_customers[:top_n]


def get_top_products(records, top_n=10):
    """
    Get top products by total sales value
    """
    product_sales = analyze_sales_by_product(records)
    
    # Sort by total sales descending
    sorted_products = sorted(product_sales.items(), key=lambda x: x[1]['total'], reverse=True)
    
    return sorted_products[:top_n]


def calculate_statistics(records):
    """
    Calculate overall sales statistics
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


# =====================================================
# Sales Summary Calculator
# =====================================================

def calculate_total_revenue(transactions):
    """
    Calculates total revenue from all transactions

    """
    total_revenue = sum(t['Quantity'] * t['UnitPrice'] for t in transactions)
    return total_revenue


def region_wise_sales(transactions):
    """
    Analyzes sales by region
    
    """
    # Calculate total revenue first
    total_revenue = calculate_total_revenue(transactions)
    
    # Dictionary to store region statistics
    region_stats = {}
    
    # Aggregate by region
    for transaction in transactions:
        region = transaction['Region']
        sales = transaction['Quantity'] * transaction['UnitPrice']
        
        if region not in region_stats:
            region_stats[region] = {
                'total_sales': 0,
                'transaction_count': 0
            }
        
        region_stats[region]['total_sales'] += sales
        region_stats[region]['transaction_count'] += 1
    
    # Calculate percentages
    for region in region_stats:
        percentage = (region_stats[region]['total_sales'] / total_revenue * 100) if total_revenue > 0 else 0
        region_stats[region]['percentage'] = round(percentage, 2)
    
    # Sort by total_sales descending
    sorted_regions = dict(sorted(
        region_stats.items(),
        key=lambda x: x[1]['total_sales'],
        reverse=True
    ))
    
    return sorted_regions


def top_selling_products(transactions, n=5):
    """
    Finds top n products by total quantity sold

    """
    # Dictionary to store product statistics
    product_stats = {}
    
    # Aggregate by ProductName
    for transaction in transactions:
        product_name = transaction['ProductName']
        quantity = transaction['Quantity']
        revenue = transaction['Quantity'] * transaction['UnitPrice']
        
        if product_name not in product_stats:
            product_stats[product_name] = {
                'total_quantity': 0,
                'total_revenue': 0
            }
        
        product_stats[product_name]['total_quantity'] += quantity
        product_stats[product_name]['total_revenue'] += revenue
    
    # Convert to list of tuples and sort by quantity descending
    product_list = [
        (name, stats['total_quantity'], stats['total_revenue'])
        for name, stats in product_stats.items()
    ]
    
    # Sort by TotalQuantity descending
    product_list.sort(key=lambda x: x[1], reverse=True)
    
    # Return top n
    return product_list[:n]


def customer_analysis(transactions):
    """
    Analyzes customer purchase patterns
  

    """
    # Dictionary to store customer statistics
    customer_stats = {}
    
    # Aggregate by CustomerID
    for transaction in transactions:
        customer_id = transaction['CustomerID']
        amount_spent = transaction['Quantity'] * transaction['UnitPrice']
        product_name = transaction['ProductName']
        
        if customer_id not in customer_stats:
            customer_stats[customer_id] = {
                'total_spent': 0,
                'purchase_count': 0,
                'products_bought': set()
            }
        
        customer_stats[customer_id]['total_spent'] += amount_spent
        customer_stats[customer_id]['purchase_count'] += 1
        customer_stats[customer_id]['products_bought'].add(product_name)
    
    # Calculate average order value and convert sets to lists
    for customer_id in customer_stats:
        stats = customer_stats[customer_id]
        stats['avg_order_value'] = round(
            stats['total_spent'] / stats['purchase_count'],
            2
        )
        stats['products_bought'] = sorted(list(stats['products_bought']))
    
    # Sort by total_spent descending
    sorted_customers = dict(sorted(
        customer_stats.items(),
        key=lambda x: x[1]['total_spent'],
        reverse=True
    ))
    
    return sorted_customers


# =====================================================
# Q 3: Task 2.2 - Date-based Analysis
# =====================================================

def daily_sales_trend(transactions):
    """
    Analyzes sales trends by date
    
    """
    # Dictionary to store daily statistics
    daily_stats = {}
    
    # Aggregate by date
    for transaction in transactions:
        date = transaction['Date']
        revenue = transaction['Quantity'] * transaction['UnitPrice']
        customer_id = transaction['CustomerID']
        
        if date not in daily_stats:
            daily_stats[date] = {
                'revenue': 0,
                'transaction_count': 0,
                'unique_customers': set()
            }
        
        daily_stats[date]['revenue'] += revenue
        daily_stats[date]['transaction_count'] += 1
        daily_stats[date]['unique_customers'].add(customer_id)
    
    # Convert sets to counts
    for date in daily_stats:
        daily_stats[date]['unique_customers'] = len(daily_stats[date]['unique_customers'])
    
    # Sort chronologically by date
    sorted_daily = dict(sorted(daily_stats.items()))
    
    return sorted_daily


def find_peak_sales_day(transactions):
    """
    Identifies the date with highest revenue
   

    """
    daily_sales = daily_sales_trend(transactions)
    
    if not daily_sales:
        return None
    
    # Find the date with maximum revenue
    peak_date = max(daily_sales.items(), key=lambda x: x[1]['revenue'])
    
    date = peak_date[0]
    revenue = peak_date[1]['revenue']
    transaction_count = peak_date[1]['transaction_count']
    
    return (date, revenue, transaction_count)


# =====================================================
# Product Performance
# =====================================================

def low_performing_products(transactions, threshold=10):
    """
    Identifies products with low sales
   

    """
    # Dictionary to store product statistics
    product_stats = {}
    
    # Aggregate by ProductName
    for transaction in transactions:
        product_name = transaction['ProductName']
        quantity = transaction['Quantity']
        revenue = transaction['Quantity'] * transaction['UnitPrice']
        
        if product_name not in product_stats:
            product_stats[product_name] = {
                'total_quantity': 0,
                'total_revenue': 0
            }
        
        product_stats[product_name]['total_quantity'] += quantity
        product_stats[product_name]['total_revenue'] += revenue
    
    # Convert to list of tuples and filter by threshold
    product_list = [
        (name, stats['total_quantity'], stats['total_revenue'])
        for name, stats in product_stats.items()
        if stats['total_quantity'] < threshold
    ]
    
    # Sort by TotalQuantity ascending
    product_list.sort(key=lambda x: x[1])
    
    return product_list


# =====================================================
# Report Generation
# =====================================================

def generate_sales_report(transactions, enriched_transactions=None, output_file='output/sales_report.txt'):
    """
    Generates a comprehensive formatted text report
        
    """
    from datetime import datetime
    
    if not transactions:
        return False
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            # Calculate common data
            total_revenue = sum(t['Quantity'] * t['UnitPrice'] for t in transactions)
            total_transactions = len(transactions)
            avg_order_value = total_revenue / total_transactions if total_transactions > 0 else 0
            dates = sorted([t['Date'] for t in transactions])
            date_range = f"{dates[0]} to {dates[-1]}" if dates else "N/A"
            
            # ===== SECTION 1: HEADER =====
            f.write("=" * 60 + "\n")
            f.write("           SALES ANALYTICS REPORT\n")
            f.write(f"         Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"         Records Processed: {total_transactions}\n")
            f.write("=" * 60 + "\n\n")
            
            # ===== SECTION 2: OVERALL SUMMARY =====
            f.write("OVERALL SUMMARY\n")
            f.write("-" * 60 + "\n")
            f.write(f"Total Revenue:        ${total_revenue:,.2f}\n")
            f.write(f"Total Transactions:   {total_transactions}\n")
            f.write(f"Average Order Value:  ${avg_order_value:,.2f}\n")
            f.write(f"Date Range:           {date_range}\n\n")
            
            # ===== SECTION 3: REGION-WISE PERFORMANCE =====
            regions = region_wise_sales(transactions)
            f.write("REGION-WISE PERFORMANCE\n")
            f.write("-" * 60 + "\n")
            f.write(f"{'Region':<15} {'Sales':<18} {'% of Total':<15} {'Transactions':<12}\n")
            f.write("-" * 60 + "\n")
            for region, stats in regions.items():
                region_display = region if region else "(Unknown)"
                f.write(f"{region_display:<15} ${stats['total_sales']:>15,.2f}  "
                       f"{stats['percentage']:>6.2f}%     {stats['transaction_count']:>6}\n")
            f.write("\n")
            
            # ===== SECTION 4: TOP 5 PRODUCTS =====
            top_products = top_selling_products(transactions, n=5)
            f.write("TOP 5 PRODUCTS\n")
            f.write("-" * 60 + "\n")
            f.write(f"{'Rank':<6} {'Product Name':<25} {'Quantity':<12} {'Revenue':<15}\n")
            f.write("-" * 60 + "\n")
            for idx, (name, qty, revenue) in enumerate(top_products, 1):
                f.write(f"{idx:<6} {name:<25} {qty:<12} ${revenue:>13,.2f}\n")
            f.write("\n")
            
            # ===== SECTION 5: TOP 5 CUSTOMERS =====
            customers = customer_analysis(transactions)
            top_5_customers = list(customers.items())[:5]
            f.write("TOP 5 CUSTOMERS\n")
            f.write("-" * 60 + "\n")
            f.write(f"{'Rank':<6} {'Customer ID':<15} {'Total Spent':<18} {'Order Count':<12}\n")
            f.write("-" * 60 + "\n")
            for idx, (cid, stats) in enumerate(top_5_customers, 1):
                f.write(f"{idx:<6} {cid:<15} ${stats['total_spent']:>15,.2f}  "
                       f"{stats['purchase_count']:>6}\n")
            f.write("\n")
            
            # ===== SECTION 6: DAILY SALES TREND =====
            daily_trend = daily_sales_trend(transactions)
            f.write("DAILY SALES TREND\n")
            f.write("-" * 60 + "\n")
            f.write(f"{'Date':<15} {'Revenue':<18} {'Transactions':<15} {'Unique Customers':<12}\n")
            f.write("-" * 60 + "\n")
            for date, stats in list(daily_trend.items())[:10]:  # Show first 10 days
                f.write(f"{date:<15} ${stats['revenue']:>15,.2f}  "
                       f"{stats['transaction_count']:>6}         {stats['unique_customers']:>6}\n")
            if len(daily_trend) > 10:
                f.write(f"... and {len(daily_trend) - 10} more days\n")
            f.write("\n")
            
            # ===== SECTION 7: PRODUCT PERFORMANCE ANALYSIS =====
            peak_day = find_peak_sales_day(transactions)
            low_performers = low_performing_products(transactions, threshold=10)
            f.write("PRODUCT PERFORMANCE ANALYSIS\n")
            f.write("-" * 60 + "\n")
            f.write(f"Best Selling Day:     {peak_day[0]} (${peak_day[1]:,.2f}, {peak_day[2]} transactions)\n")
            f.write(f"Low Performing Products (< 10 units): {len(low_performers)} products\n")
            if low_performers:
                for product_name, qty, revenue in low_performers[:5]:
                    f.write(f"  - {product_name}: {qty} units (${revenue:,.2f})\n")
            
            # Calculate average transaction value per region
            f.write("\nAverage Transaction Value per Region:\n")
            for region, stats in regions.items():
                region_display = region if region else "(Unknown)"
                if stats['transaction_count'] > 0:
                    avg = stats['total_sales'] / stats['transaction_count']
                    f.write(f"  {region_display}: ${avg:,.2f}\n")
            f.write("\n")
            
            # ===== SECTION 8: API ENRICHMENT SUMMARY =====
            f.write("API ENRICHMENT SUMMARY\n")
            f.write("-" * 60 + "\n")
            if enriched_transactions:
                total_enriched = len(enriched_transactions)
                matched = sum(1 for tx in enriched_transactions if tx.get('API_Match', False))
                success_rate = (matched / total_enriched * 100) if total_enriched > 0 else 0
                
                f.write(f"Total Transactions Processed: {total_enriched}\n")
                f.write(f"Successfully Enriched:       {matched}\n")
                f.write(f"Enrichment Success Rate:     {success_rate:.2f}%\n")
                
                # Find products that couldn't be enriched
                unmatched_products = set()
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
            
            f.write("\n" + "=" * 60 + "\n")
            f.write("END OF REPORT\n")
            f.write("=" * 60 + "\n")
        
        return True
    
    except Exception as e:
        print(f"[ERROR] Failed to generate report: {str(e)}")
        return False