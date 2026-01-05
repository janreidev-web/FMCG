"""
Simulate fact_sales table data for FMCG Data Analytics Platform
"""

import pandas as pd
import random
from datetime import datetime, timedelta, date
from faker import Faker
from src.utils.id_generation import id_generator

def simulate_fact_sales(num_records: int = 20):
    """Simulate realistic FMCG sales data"""
    
    # Initialize Faker
    faker = Faker('en_PH')
    
    # Generate sample IDs using the new format
    employee_ids = [id_generator.generate_id('dim_employees') for _ in range(10)]
    product_ids = [id_generator.generate_id('dim_products') for _ in range(15)]
    retailer_ids = [id_generator.generate_id('dim_retailers') for _ in range(8)]
    campaign_ids = [id_generator.generate_id('dim_campaigns') for _ in range(5)]
    
    # FMCG product categories and typical price ranges
    product_categories = {
        'Beverages': {'min_price': 15, 'max_price': 150, 'typical_qty': (1, 12)},
        'Snacks': {'min_price': 10, 'max_price': 200, 'typical_qty': (1, 6)},
        'Personal Care': {'min_price': 25, 'max_price': 500, 'typical_qty': (1, 3)},
        'Household': {'min_price': 20, 'max_price': 300, 'typical_qty': (1, 4)},
        'Food': {'min_price': 30, 'max_price': 250, 'typical_qty': (1, 8)},
        'Health': {'min_price': 50, 'max_price': 800, 'typical_qty': (1, 2)}
    }
    
    sales_data = []
    
    # Calculate date range: from 2015-01-01 to 2026-01-04 (day before yesterday)
    start_date = date(2015, 1, 1)
    end_date = date(2026, 1, 4)  # Day before yesterday
    total_days = (end_date - start_date).days
    
    for i in range(num_records):
        # Generate random sale date within the historical range
        random_days = random.randint(0, total_days)
        sale_date = start_date + timedelta(days=random_days)
        
        # Randomly select product and determine category pricing
        product_id = random.choice(product_ids)
        category = random.choice(list(product_categories.keys()))
        pricing = product_categories[category]
        
        # Generate realistic quantity based on product type
        min_qty, max_qty = pricing['typical_qty']
        quantity = random.randint(min_qty, max_qty)
        
        # Generate unit price within category range
        unit_price = round(random.uniform(pricing['min_price'], pricing['max_price']), 2)
        
        # Calculate total amount
        total_amount = round(unit_price * quantity, 2)
        
        # Apply random discount (0-20%)
        discount_percentage = random.uniform(0, 0.2)
        discount_amount = round(total_amount * discount_percentage, 2)
        final_amount = round(total_amount - discount_amount, 2)
        
        # Generate sale data
        sale_record = {
            "sale_id": id_generator.generate_id('fact_sales'),
            "product_id": product_id,
            "retailer_id": random.choice(retailer_ids),
            "employee_id": random.choice(employee_ids),
            "campaign_id": random.choice(campaign_ids + [None]),  # Some sales may not have campaigns
            "quantity": quantity,
            "unit_price": unit_price,
            "total_amount": total_amount,
            "discount_percentage": round(discount_percentage * 100, 2),
            "discount_amount": discount_amount,
            "final_amount": final_amount,
            "sale_date": sale_date,
            "delivery_date": sale_date + timedelta(days=random.randint(1, 7)),
            "delivery_status": random.choice(["Delivered", "Pending", "In Transit", "Cancelled"]),
            "payment_method": random.choice(["Cash", "Credit Card", "Mobile Payment", "Bank Transfer"]),
            "payment_status": random.choice(["Paid", "Pending", "Overdue"]),
            "region": random.choice(["NCR", "Luzon", "Visayas", "Mindanao"]),
            "channel": random.choice(["Retail Store", "Online", "Direct Sales", "Distributor"]),
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        }
        
        sales_data.append(sale_record)
    
    return pd.DataFrame(sales_data)

def display_sales_summary(sales_df):
    """Display summary statistics of the simulated sales data"""
    
    print("=" * 60)
    print("FMCG SALES DATA SIMULATION")
    print("=" * 60)
    
    print(f"\nTotal Sales Records: {len(sales_df)}")
    print(f"Date Range: {sales_df['sale_date'].min()} to {sales_df['sale_date'].max()}")
    
    print(f"\nFinancial Summary:")
    print(f"   Total Revenue: PHP{sales_df['final_amount'].sum():,.2f}")
    print(f"   Average Sale: PHP{sales_df['final_amount'].mean():,.2f}")
    print(f"   Largest Sale: PHP{sales_df['final_amount'].max():,.2f}")
    print(f"   Smallest Sale: PHP{sales_df['final_amount'].min():,.2f}")
    
    print(f"\nSales Metrics:")
    print(f"   Total Units Sold: {sales_df['quantity'].sum():,}")
    print(f"   Average Quantity per Sale: {sales_df['quantity'].mean():.1f}")
    print(f"   Average Unit Price: PHP{sales_df['unit_price'].mean():.2f}")
    print(f"   Average Discount: {sales_df['discount_percentage'].mean():.1f}%")
    
    print(f"\nDistribution:")
    print(f"   Delivery Status: {sales_df['delivery_status'].value_counts().to_dict()}")
    print(f"   Payment Method: {sales_df['payment_method'].value_counts().to_dict()}")
    print(f"   Payment Status: {sales_df['payment_status'].value_counts().to_dict()}")
    print(f"   Sales Channel: {sales_df['channel'].value_counts().to_dict()}")
    print(f"   Region: {sales_df['region'].value_counts().to_dict()}")
    
    print(f"\nCampaign Impact:")
    campaign_sales = sales_df[sales_df['campaign_id'].notna()]
    if len(campaign_sales) > 0:
        print(f"   Sales with Campaigns: {len(campaign_sales)} ({len(campaign_sales)/len(sales_df)*100:.1f}%)")
        print(f"   Campaign Revenue: PHP{campaign_sales['final_amount'].sum():,.2f}")
    else:
        print("   No sales linked to campaigns")

def display_sample_data(sales_df, num_samples: int = 5):
    """Display sample sales records"""
    
    print(f"\nSample Sales Records (First {num_samples}):")
    print("-" * 120)
    
    # Select columns to display
    display_columns = [
        'sale_id', 'product_id', 'retailer_id', 'employee_id', 'quantity',
        'unit_price', 'final_amount', 'sale_date', 'delivery_status', 'payment_method'
    ]
    
    sample_df = sales_df[display_columns].head(num_samples)
    
    for idx, row in sample_df.iterrows():
        print(f"Sale ID: {row['sale_id']}")
        print(f"  Product: {row['product_id']} | Retailer: {row['retailer_id']} | Employee: {row['employee_id']}")
        print(f"  Quantity: {row['quantity']} @ PHP{row['unit_price']:.2f} = PHP{row['final_amount']:.2f}")
        print(f"  Date: {row['sale_date']} | Status: {row['delivery_status']} | Payment: {row['payment_method']}")
        print("-" * 120)

def main():
    """Main simulation function"""
    
    # Generate sales data with historical range
    print("Generating FMCG sales data from 2015-01-01 to day before yesterday...")
    sales_df = simulate_fact_sales(100)  # Generate more records for better historical representation
    
    # Display summary
    display_sales_summary(sales_df)
    
    # Display sample data
    display_sample_data(sales_df, 5)
    
    # Show ID format examples
    print(f"\nID Format Examples:")
    print(f"   Sale ID: {sales_df['sale_id'].iloc[0]}")
    print(f"   Product ID: {sales_df['product_id'].iloc[0]}")
    print(f"   Retailer ID: {sales_df['retailer_id'].iloc[0]}")
    print(f"   Employee ID: {sales_df['employee_id'].iloc[0]}")
    print(f"   Campaign ID: {sales_df['campaign_id'].iloc[0] if pd.notna(sales_df['campaign_id'].iloc[0]) else 'None'}")
    
    # Show date range details
    print(f"\nHistorical Date Range Details:")
    print(f"   Start Date: 2015-01-01")
    print(f"   End Date: 2026-01-04 (day before yesterday)")
    print(f"   Total Days: {(date(2026, 1, 4) - date(2015, 1, 1)).days:,}")
    
    print(f"\nSimulation completed successfully!")
    print(f"   Generated {len(sales_df)} realistic FMCG sales records")
    print(f"   Using new ID format: PREFIX + 15-digit number")
    print(f"   Historical data from 2015 to day before yesterday")

if __name__ == "__main__":
    main()
