"""
Sales Analytics System - Main Application
Interactive workflow with user input and comprehensive reporting
"""

import os
import sys
import json
from datetime import datetime

utils_path = os.path.join(os.path.dirname(__file__), 'utils')
sys.path.insert(0, utils_path)

from file_handler import (
    read_sales_data,
    parse_transactions,
    validate_and_filter,
    write_json_file
)
from data_processor import (
    calculate_statistics,
    region_wise_sales,
    top_selling_products,
    customer_analysis,
    daily_sales_trend,
    find_peak_sales_day,
    low_performing_products,
    generate_sales_report,
    analyze_sales_by_region,
    get_top_customers,
    get_top_products
)
from api_handler import (
    fetch_all_products,
    create_product_mapping,
    enrich_sales_data_with_api,
    save_enriched_data
)


def print_step(step_num, total_steps, message):
    print(f"[{step_num}/{total_steps}] {message}")

def print_success(message):
    print(f"[OK] {message}\n")

def print_error(message):
    print(f"[ERROR] {message}\n")

def get_user_filter_choice():
    while True:
        choice = input("\nDo you want to filter data? (y/n): ").lower().strip()
        if choice in ['y', 'yes', 'n', 'no']:
            return choice in ['y', 'yes']
        print("Please enter 'y' or 'n'")


def get_region_filter():
    while True:
        region = input("Enter region to filter by (or press Enter to skip): ").strip()
        if region == "":
            return None
        if region in ['North', 'South', 'East', 'West']:
            return region
        print("Invalid region. Valid options: North, South, East, West")


def get_amount_filter():
    try:
        print("\nEnter transaction amount range:")
        min_amount_str = input("Minimum amount (or press Enter to skip): ").strip()
        max_amount_str = input("Maximum amount (or press Enter to skip): ").strip()
        
        min_amount = float(min_amount_str) if min_amount_str else None
        max_amount = float(max_amount_str) if max_amount_str else None
        
        if min_amount is not None and max_amount is not None and min_amount > max_amount:
            print("Error: Minimum amount cannot be greater than maximum amount")
            return None, None
        
        return min_amount, max_amount
    except ValueError:
        print("Invalid amount entered")
        return None, None


def main():

    try:
        # Welcome message
        print("\n" + "="*60)
        print("             SALES ANALYTICS SYSTEM")
        print("="*60 + "\n")
        
        # Define file paths
        script_dir = os.path.dirname(os.path.abspath(__file__))
        data_file = os.path.join(script_dir, 'data', 'sales_data.txt')
        output_dir = os.path.join(script_dir, 'output')
        
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        print_step(1, 10, "Reading sales data...")
        raw_data = read_sales_data(data_file)
        if not raw_data:
            print_error("No data read from file")
            return
        print_success(f"Successfully read {len(raw_data)} transactions")
        
        print_step(2, 10, "Parsing and cleaning data...")
        parsed_transactions = parse_transactions(raw_data)
        if not parsed_transactions:
            print_error("No transactions parsed")
            return
        print_success(f"Parsed {len(parsed_transactions)} records")
        
        print_step(3, 10, "Filter Options Available:")
        
        regions_set = set()
        amounts = []
        for tx in parsed_transactions:
            regions_set.add(tx.get('Region', ''))
            amounts.append(tx.get('TotalSales', 0))
        
        regions_list = sorted(list(regions_set))
        min_amount = min(amounts) if amounts else 0
        max_amount = max(amounts) if amounts else 0
        print(f"Regions: {', '.join(regions_list)}")
        print(f"Amount Range: ${min_amount:,.2f} - ${max_amount:,.2f}\n")
        
        region_filter = None
        min_filter = None
        max_filter = None
        
        if get_user_filter_choice():
            region_filter = get_region_filter()
            min_filter, max_filter = get_amount_filter()
        
        print()
        print_step(5, 10, "Validating transactions...")
        valid_transactions, invalid_count, filter_summary = validate_and_filter(
            parsed_transactions,
            region=region_filter,
            min_amount=min_filter,
            max_amount=max_filter
        )
        if not valid_transactions:
            print_error("No valid transactions after validation")
            return
        print(f"[OK] Valid: {filter_summary['valid']} | Invalid: {filter_summary['invalid']}\n")
        
        print_step(6, 10, "Analyzing sales data...")
        statistics = calculate_statistics(valid_transactions)
        regions = region_wise_sales(valid_transactions)
        top_products = top_selling_products(valid_transactions, n=5)
        customers = customer_analysis(valid_transactions)
        daily_trend = daily_sales_trend(valid_transactions)
        peak_day = find_peak_sales_day(valid_transactions)
        low_performers = low_performing_products(valid_transactions, threshold=10)
        
        print_success("Analysis complete")
        
        print_step(8, 10, "Fetching product data from API...")
        api_products = fetch_all_products()
        if api_products:
            product_mapping = create_product_mapping(api_products)
            print_success(f"Fetched {len(api_products)} products")
        else:
            print("[WARNING] API products not available, continuing with local data...\n")
            product_mapping = {}
        
        print_step(9, 10, "Enriching sales data with API information...")
        api_enriched_transactions = enrich_sales_data_with_api(valid_transactions, product_mapping)
        
        matched = sum(1 for tx in api_enriched_transactions if tx.get('API_Match', False))
        total = len(api_enriched_transactions)
        success_rate = (matched / total * 100) if total > 0 else 0
        
        print_success(f"Enriched {matched}/{total} transactions ({success_rate:.1f}%)")
        enriched_file = os.path.join(script_dir, 'data', 'enriched_sales_data.txt')
        save_enriched_data(api_enriched_transactions, enriched_file)
        
        print_step(10, 10, "Saving reports and generating output...")
        parsed_output_file = os.path.join(output_dir, 'parsed_transactions.json')
        write_json_file(parsed_output_file, valid_transactions)
        enriched_output_file = os.path.join(output_dir, 'enriched_sales_data.json')
        write_json_file(enriched_output_file, api_enriched_transactions)
        report_file = os.path.join(output_dir, 'sales_report.txt')
        report_success = generate_sales_report(valid_transactions, api_enriched_transactions, report_file)
        analytics_report = {
            'timestamp': datetime.now().isoformat(),
            'data_validation': {
                'total_input': filter_summary['total_input'],
                'invalid_records': filter_summary['invalid'],
                'valid_records': filter_summary['valid'],
                'final_count': filter_summary['final_count']
            },
            'statistics': statistics,
            'sales_by_region': regions,
            'top_5_products': [{'name': p[0], 'quantity': p[1], 'revenue': p[2]} for p in top_products],
            'top_5_customers': [{'id': c[0], 'total_spent': c[1]['total_spent'], 'orders': c[1]['purchase_count']} for c in list(customers.items())[:5]],
            'peak_sales_day': {'date': peak_day[0], 'revenue': peak_day[1], 'transactions': peak_day[2]},
            'low_performers_count': len(low_performers),
            'api_enrichment': {
                'total_enriched': len(api_enriched_transactions),
                'matched': matched,
                'success_rate': success_rate
            }
        }
        
        analytics_output_file = os.path.join(output_dir, 'analytics_report.json')
        write_json_file(analytics_output_file, analytics_report)
        
        print_success("All reports saved")
        
        # ===== COMPLETION MESSAGE =====
        print("="*60)
        print("[SUCCESS] PROCESS COMPLETE!")
        print("="*60)
        print("\nOutput Files Generated:")
        print(f"  [OK] {parsed_output_file}")
        print(f"  [OK] {enriched_output_file}")
        print(f"  [OK] {analytics_output_file}")
        print(f"  [OK] {report_file}")
        print(f"  [OK] {enriched_file}")
        print("\n" + "="*60 + "\n")
        
    except FileNotFoundError as e:
        print_error(f"File not found: {str(e)}")
    except ValueError as e:
        print_error(f"Value error: {str(e)}")
    except KeyError as e:
        print_error(f"Missing required field: {str(e)}")
    except Exception as e:
        print_error(f"Unexpected error: {str(e)}")


if __name__ == "__main__":
    main()
