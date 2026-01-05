"""
Calculate transaction counts for FMCG historical data
"""

import pandas as pd
from datetime import date, timedelta

def calculate_transaction_counts():
    """Calculate realistic transaction counts and values for FMCG operation"""
    
    print("=" * 80)
    print("FMCG TRANSACTION COUNT & VALUE CALCULATION - 2015 TO PRESENT (11 YEARS)")
    print("=" * 80)
    
    # Base parameters for billion-peso FMCG enterprise
    years = 11  # 2015 to 2026 (present)
    days_per_year = 365
    total_days = years * days_per_year
    
    # Daily transaction estimates for enterprise scale (adjusted for <1M total transactions)
    daily_transactions = {
        'retail_stores': {
            'stores': 200,  # 200 retail stores nationwide
            'daily_transactions_per_store': 2,  # Average 2 transactions per store = 400 daily total
            'avg_transaction_value': 5000,  # PHP 5,000 average per transaction (bulk pack purchases)
            'description': 'Sari-sari stores, supermarkets, convenience stores'
        },
        'online_sales': {
            'daily_orders': 10,  # Online platform orders
            'avg_transaction_value': 6000,  # PHP 6,000 average (larger bulk orders online)
            'description': 'E-commerce, mobile app, website orders'
        },
        'distributor_sales': {
            'distributors': 10,  # 10 distributors
            'daily_transactions_per_distributor': 2,  # Average 2 transactions per distributor
            'avg_transaction_value': 8000,  # PHP 8,000 average (large case packs)
            'description': 'Wholesale to smaller retailers'
        },
        'direct_sales': {
            'sales_reps': 15,  # 15 direct sales representatives
            'daily_calls_per_rep': 1,  # Average 1 sales call/visit
            'avg_transaction_value': 15000,  # PHP 15,000 average (major bulk orders)
            'description': 'Direct sales to major accounts'
        }
    }
    
    print(f"\nTime Period: {years} years ({total_days:,} days)")
    print(f"Date Range: 2015-01-01 to 2026-01-05 (present)")
    
    print(f"\nDaily Transaction Estimates:")
    
    total_daily_transactions = 0
    total_daily_revenue = 0
    
    for channel, data in daily_transactions.items():
        if channel == 'retail_stores':
            daily_count = data['stores'] * data['daily_transactions_per_store']
            daily_revenue = daily_count * data['avg_transaction_value']
        elif channel == 'online_sales':
            daily_count = data['daily_orders']
            daily_revenue = daily_count * data['avg_transaction_value']
        elif channel == 'distributor_sales':
            daily_count = data['distributors'] * data['daily_transactions_per_distributor']
            daily_revenue = daily_count * data['avg_transaction_value']
        elif channel == 'direct_sales':
            daily_count = data['sales_reps'] * data['daily_calls_per_rep']
            daily_revenue = daily_count * data['avg_transaction_value']
        
        total_daily_transactions += daily_count
        total_daily_revenue += daily_revenue
        print(f"   {channel.title()}: {daily_count:,} daily transactions")
        print(f"      - Daily Revenue: PHP{daily_revenue:,.0f}")
        print(f"      - Avg Transaction: PHP{data['avg_transaction_value']}")
        print(f"      - {data['description']}")
    
    print(f"\nTotal Daily Transactions: {total_daily_transactions:,}")
    print(f"Total Daily Revenue: PHP{total_daily_revenue:,.0f}")
    print(f"Total Annual Revenue: PHP{total_daily_revenue * 365:,.0f}")
    print(f"Total 11-Year Revenue: PHP{total_daily_revenue * total_days:,.0f}")
    
    # Calculate by year with growth (2015-2026)
    print(f"\nYear-by-Year Revenue Growth (2015-2026):")
    
    base_daily_revenue = total_daily_revenue
    total_11_year_revenue = 0
    total_11_year_transactions = 0
    
    for year in range(1, years + 1):
        # Growth factor: 5% to 15% annual growth
        growth_factor = 1 + (0.05 + (year - 1) * 0.01)  # Progressive growth
        year_daily_revenue = int(base_daily_revenue * growth_factor)
        year_revenue = year_daily_revenue * 365
        year_transactions = int(total_daily_transactions * growth_factor * 365)
        total_11_year_revenue += year_revenue
        total_11_year_transactions += year_transactions
        
        print(f"   Year {2014 + year}: PHP{year_revenue:,.0f} ({growth_factor:.1%} growth)")
    
    print(f"\nSummary Statistics:")
    print(f"   Total 11-Year Revenue: PHP{total_11_year_revenue:,.0f}")
    print(f"   Average Daily Revenue: PHP{total_11_year_revenue / total_days:,.0f}")
    print(f"   Average Annual Revenue: PHP{total_11_year_revenue / years:,.0f}")
    print(f"   Average Monthly Revenue: PHP{total_11_year_revenue / (years * 12):,.0f}")
    print(f"   Average Transaction Value: PHP{total_11_year_revenue / total_11_year_transactions:.2f}")
    
    # Additional breakdowns
    print(f"\nRevenue Breakdown by Channel:")
    
    for channel, data in daily_transactions.items():
        if channel == 'retail_stores':
            channel_daily_revenue = data['stores'] * data['daily_transactions_per_store'] * data['avg_transaction_value']
            channel_total_revenue = channel_daily_revenue * total_days
        elif channel == 'online_sales':
            channel_daily_revenue = data['daily_orders'] * data['avg_transaction_value']
            channel_total_revenue = channel_daily_revenue * total_days
        elif channel == 'distributor_sales':
            channel_daily_revenue = data['distributors'] * data['daily_transactions_per_distributor'] * data['avg_transaction_value']
            channel_total_revenue = channel_daily_revenue * total_days
        elif channel == 'direct_sales':
            channel_daily_revenue = data['sales_reps'] * data['daily_calls_per_rep'] * data['avg_transaction_value']
            channel_total_revenue = channel_daily_revenue * total_days
        
        percentage = (channel_total_revenue / total_11_year_revenue) * 100
        print(f"      {channel.title()}: PHP{channel_total_revenue:,.0f} ({percentage:.1f}%)")
        print(f"         Avg Transaction: PHP{data['avg_transaction_value']}")
    
    # Peak season calculations
    print(f"\nPeak Season Analysis:")
    peak_days = total_days * 0.25  # 25% of days are peak season
    normal_days = total_days * 0.75  # 75% are normal season
    
    peak_revenue = int(total_11_year_revenue * 0.4)  # 40% of revenue during peak season
    normal_revenue = total_11_year_revenue - peak_revenue
    
    print(f"   Peak Season Revenue: PHP{peak_revenue:,.0f} ({peak_revenue/total_11_year_revenue*100:.1f}%)")
    print(f"   Normal Season Revenue: PHP{normal_revenue:,.0f} ({normal_revenue/total_11_year_revenue*100:.1f}%)")
    
    # Database storage considerations
    print(f"\nDatabase Storage Considerations:")
    avg_record_size = 500  # bytes per record (estimated)
    total_size_gb = (total_11_year_transactions * avg_record_size) / (1024**3)
    
    print(f"   Estimated Record Count: {total_11_year_transactions:,}")
    print(f"   Estimated Storage: {total_size_gb:.1f} GB")
    print(f"   Recommended Partitioning: Yearly partitions for performance")
    
    print(f"\nTransaction Count & Value Calculation Completed!")
    print(f"   Total Transactions: {total_11_year_transactions:,}")
    print(f"   Total Revenue: PHP{total_11_year_revenue:,.0f}")
    print(f"   Time Period: {years} years (2015-2026)")
    print(f"   Enterprise Scale: 500+ stores, multi-regional operations")
    print(f"   Company Scale: Billion-peso FMCG enterprise")
    print(f"   Average Transaction: PHP{total_11_year_revenue / total_11_year_transactions:.2f}")

if __name__ == "__main__":
    calculate_transaction_counts()
