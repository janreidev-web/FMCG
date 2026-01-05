"""
Financial simulation for FMCG Data Analytics Platform
Ensures revenue > total costs (wages + operating + other costs)
"""

import pandas as pd
import random
from datetime import datetime, timedelta, date
from faker import Faker
from src.utils.id_generation import id_generator

def simulate_financial_data(num_months: int = 12):
    """Simulate financial data ensuring revenue > costs"""
    
    faker = Faker('en_PH')
    
    # Financial data storage
    financial_data = []
    
    # Base financial parameters (monthly) - BILLION PESO COMPANY SCALE
    base_monthly_revenue = 80000000   # 80M PHP monthly revenue (~960M PHP annually)
    base_monthly_wages = 20000000     # 20M PHP monthly wages for 400+ employees
    base_monthly_operating = 15000000  # 15M PHP monthly operating costs (multi-regional)
    base_monthly_marketing = 5000000    # 5M PHP monthly marketing costs
    
    for month_offset in range(num_months):
        # Calculate date for this month
        current_date = date(2025, 1, 1) + timedelta(days=30 * month_offset)
        month_year = current_date.strftime("%Y-%m")
        
        # Generate realistic variations for large enterprise
        revenue_factor = random.uniform(0.85, 1.25)  # ±25% revenue variation (more stable for large companies)
        wage_factor = random.uniform(0.95, 1.05)     # ±5% wage variation (stable workforce)
        operating_factor = random.uniform(0.9, 1.1)    # ±10% operating cost variation
        marketing_factor = random.uniform(0.8, 1.2)      # ±20% marketing variation
        
        # Calculate base amounts
        monthly_revenue = base_monthly_revenue * revenue_factor
        monthly_wages = base_monthly_wages * wage_factor
        monthly_operating = base_monthly_operating * operating_factor
        monthly_marketing = base_monthly_marketing * marketing_factor
        
        # Calculate total costs
        total_costs = monthly_wages + monthly_operating + monthly_marketing
        
        # Ensure revenue is always greater than total costs with a healthy margin
        min_profit_margin = 0.20  # 20% minimum profit margin (typical for large enterprises)
        required_revenue = total_costs * (1 + min_profit_margin)
        
        # Adjust revenue if needed to ensure profitability
        if monthly_revenue < required_revenue:
            monthly_revenue = required_revenue * random.uniform(1.0, 1.2)  # Add some buffer
        
        # Calculate final financial metrics
        gross_profit = monthly_revenue - (monthly_wages + monthly_operating)
        net_profit = monthly_revenue - total_costs
        profit_margin = (net_profit / monthly_revenue) * 100
        
        # Generate detailed cost breakdowns for enterprise operations
        cost_breakdown = {
            # Wage costs (40% of total costs) - 400+ employees across regions
            'salaries': monthly_wages * 0.65,      # Base salaries for 400+ staff
            'benefits': monthly_wages * 0.20,       # Health insurance, retirement, etc.
            'payroll_taxes': monthly_wages * 0.10,    # SSS, PhilHealth, Pag-IBIG
            'bonuses': monthly_wages * 0.05,         # Performance bonuses
            
            # Operating costs (37.5% of total costs) - Multi-regional operations
            'rent': monthly_operating * 0.25,       # Multiple warehouses, offices across regions
            'utilities': monthly_operating * 0.15,   # Electricity, water for large facilities
            'maintenance': monthly_operating * 0.10, # Equipment, vehicle maintenance
            'logistics': monthly_operating * 0.15,    # Transportation, distribution network
            'supplies': monthly_operating * 0.10,     # Raw materials, packaging
            'insurance': monthly_operating * 0.10,   # Business, property, liability insurance
            'compliance': monthly_operating * 0.10,   # Regulatory compliance costs
            'technology': monthly_operating * 0.05,   # IT infrastructure, systems
            
            # Marketing costs (22.5% of total costs) - National campaigns
            'advertising': monthly_marketing * 0.35,  # TV, radio, digital advertising
            'promotions': monthly_marketing * 0.25,   # In-store promotions, discounts
            'events': monthly_marketing * 0.20,      # Trade shows, community events
            'brand_ambassadors': monthly_marketing * 0.10, # Celebrity endorsements
            'digital_marketing': monthly_marketing * 0.10 # Social media, online campaigns
        }
        
        # Generate revenue breakdown by product category
        revenue_breakdown = {
            'beverages': monthly_revenue * random.uniform(0.2, 0.3),
            'food_products': monthly_revenue * random.uniform(0.25, 0.35),
            'personal_care': monthly_revenue * random.uniform(0.15, 0.25),
            'household': monthly_revenue * random.uniform(0.1, 0.2),
            'health_products': monthly_revenue * random.uniform(0.05, 0.15)
        }
        
        # Create financial record
        financial_record = {
            "financial_id": id_generator.generate_id('fact_financials'),
            "month_year": month_year,
            "total_revenue": round(monthly_revenue, 2),
            "total_costs": round(total_costs, 2),
            "gross_profit": round(gross_profit, 2),
            "net_profit": round(net_profit, 2),
            "profit_margin": round(profit_margin, 2),
            
            # Cost breakdowns
            "wage_costs": round(monthly_wages, 2),
            "operating_costs": round(monthly_operating, 2),
            "marketing_costs": round(monthly_marketing, 2),
            
            # Detailed cost components
            "salaries": round(cost_breakdown['salaries'], 2),
            "benefits": round(cost_breakdown['benefits'], 2),
            "payroll_taxes": round(cost_breakdown['payroll_taxes'], 2),
            "bonuses": round(cost_breakdown['bonuses'], 2),
            "rent": round(cost_breakdown['rent'], 2),
            "utilities": round(cost_breakdown['utilities'], 2),
            "maintenance": round(cost_breakdown['maintenance'], 2),
            "logistics": round(cost_breakdown['logistics'], 2),
            "supplies": round(cost_breakdown['supplies'], 2),
            "insurance": round(cost_breakdown['insurance'], 2),
            "compliance": round(cost_breakdown['compliance'], 2),
            "technology": round(cost_breakdown['technology'], 2),
            "advertising": round(cost_breakdown['advertising'], 2),
            "promotions": round(cost_breakdown['promotions'], 2),
            "events": round(cost_breakdown['events'], 2),
            "brand_ambassadors": round(cost_breakdown['brand_ambassadors'], 2),
            "digital_marketing": round(cost_breakdown['digital_marketing'], 2),
            
            # Revenue breakdowns
            "beverage_revenue": round(revenue_breakdown['beverages'], 2),
            "food_revenue": round(revenue_breakdown['food_products'], 2),
            "personal_care_revenue": round(revenue_breakdown['personal_care'], 2),
            "household_revenue": round(revenue_breakdown['household'], 2),
            "health_revenue": round(revenue_breakdown['health_products'], 2),
            
            # Performance metrics for large enterprise
            "revenue_growth_rate": round(random.uniform(8, 25), 2),  # Higher growth for successful companies
            "cost_efficiency": round(random.uniform(0.75, 0.85), 2),  # Better cost efficiency at scale
            "employee_productivity": round(random.uniform(200000, 400000), 2),  # Higher revenue per employee
            "operating_margin": round((gross_profit / monthly_revenue) * 100, 2),
            "market_share": round(random.uniform(15, 35), 2),  # Market share percentage
            "regional_presence": random.choice(["National", "Luzon-Visayas-Mindanao", "Major Islands"]),
            
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        }
        
        financial_data.append(financial_record)
    
    return pd.DataFrame(financial_data)

def display_financial_summary(financial_df):
    """Display comprehensive financial summary"""
    
    print("=" * 80)
    print("FMCG FINANCIAL SIMULATION - REVENUE > COSTS GUARANTEED")
    print("=" * 80)
    
    print(f"\nFinancial Overview ({len(financial_df)} months):")
    print(f"   Total Revenue: PHP{financial_df['total_revenue'].sum():,.2f}")
    print(f"   Total Costs: PHP{financial_df['total_costs'].sum():,.2f}")
    print(f"   Total Net Profit: PHP{financial_df['net_profit'].sum():,.2f}")
    print(f"   Overall Profit Margin: {financial_df['net_profit'].sum() / financial_df['total_revenue'].sum() * 100:.1f}%")
    
    print(f"\nMonthly Averages:")
    print(f"   Revenue: PHP{financial_df['total_revenue'].mean():,.2f}")
    print(f"   Costs: PHP{financial_df['total_costs'].mean():,.2f}")
    print(f"   Net Profit: PHP{financial_df['net_profit'].mean():,.2f}")
    print(f"   Profit Margin: {financial_df['profit_margin'].mean():.1f}%")
    
    print(f"\nProfitability Analysis:")
    profitable_months = len(financial_df[financial_df['net_profit'] > 0])
    print(f"   Profitable Months: {profitable_months}/{len(financial_df)} ({profitable_months/len(financial_df)*100:.1f}%)")
    
    if profitable_months == len(financial_df):
        print("   ALL MONTHS PROFITABLE - Revenue > Costs Guaranteed!")
    
    print(f"   Highest Monthly Profit: PHP{financial_df['net_profit'].max():,.2f}")
    print(f"   Lowest Monthly Profit: PHP{financial_df['net_profit'].min():,.2f}")
    print(f"   Average Operating Margin: {financial_df['operating_margin'].mean():.1f}%")
    
    print(f"\nCost Breakdown (Averages):")
    print(f"   Wage Costs: PHP{financial_df['wage_costs'].mean():,.2f} ({financial_df['wage_costs'].mean()/financial_df['total_costs'].mean()*100:.1f}%)")
    print(f"   Operating Costs: PHP{financial_df['operating_costs'].mean():,.2f} ({financial_df['operating_costs'].mean()/financial_df['total_costs'].mean()*100:.1f}%)")
    print(f"   Marketing Costs: PHP{financial_df['marketing_costs'].mean():,.2f} ({financial_df['marketing_costs'].mean()/financial_df['total_costs'].mean()*100:.1f}%)")
    
    print(f"\nRevenue Breakdown (Averages):")
    print(f"   Beverages: PHP{financial_df['beverage_revenue'].mean():,.2f}")
    print(f"   Food Products: PHP{financial_df['food_revenue'].mean():,.2f}")
    print(f"   Personal Care: PHP{financial_df['personal_care_revenue'].mean():,.2f}")
    print(f"   Household: PHP{financial_df['household_revenue'].mean():,.2f}")
    print(f"   Health Products: PHP{financial_df['health_revenue'].mean():,.2f}")

def display_sample_financials(financial_df, num_samples: int = 3):
    """Display sample financial records"""
    
    print(f"\nSample Financial Records (First {num_samples}):")
    print("-" * 100)
    
    for idx, row in financial_df.head(num_samples).iterrows():
        print(f"Period: {row['month_year']}")
        print(f"  Revenue: PHP{row['total_revenue']:,.2f} | Costs: PHP{row['total_costs']:,.2f} | Net Profit: PHP{row['net_profit']:,.2f}")
        print(f"  Profit Margin: {row['profit_margin']:.1f}% | Operating Margin: {row['operating_margin']:.1f}%")
        print(f"  Revenue > Costs: {'YES' if row['total_revenue'] > row['total_costs'] else 'NO'}")
        print("-" * 100)

def validate_profitability(financial_df):
    """Validate that revenue > costs for all records"""
    
    print(f"\nProfitability Validation:")
    
    # Check each month
    all_profitable = True
    unprofitable_months = []
    
    for idx, row in financial_df.iterrows():
        if row['total_revenue'] <= row['total_costs']:
            all_profitable = False
            unprofitable_months.append({
                'month': row['month_year'],
                'revenue': row['total_revenue'],
                'costs': row['total_costs'],
                'profit': row['net_profit']
            })
    
    if all_profitable:
        print("   ALL MONTHS PROFITABLE - Revenue > Costs Guaranteed!")
        print(f"   Minimum Profit Margin: {financial_df['profit_margin'].min():.1f}%")
        print(f"   Average Profit Margin: {financial_df['profit_margin'].mean():.1f}%")
        print(f"   Maximum Profit Margin: {financial_df['profit_margin'].max():.1f}%")
    else:
        print(f"   {len(unprofitable_months)} months unprofitable:")
        for month in unprofitable_months:
            print(f"      {month['month']}: Revenue PHP{month['revenue']:,.2f} <= Costs PHP{month['costs']:,.2f}")
    
    return all_profitable

def main():
    """Main financial simulation function"""
    
    # Generate financial data
    print("Generating FMCG financial data with guaranteed profitability...")
    financial_df = simulate_financial_data(12)
    
    # Display summary
    display_financial_summary(financial_df)
    
    # Display sample data
    display_sample_financials(financial_df, 3)
    
    # Validate profitability
    is_profitable = validate_profitability(financial_df)
    
    # Show ID format
    print(f"\nID Format Example:")
    print(f"   Financial ID: {financial_df['financial_id'].iloc[0]}")
    
    print(f"\nFinancial simulation completed!")
    print(f"   Generated {len(financial_df)} months of financial data")
    print(f"   Revenue > Costs: {'GUARANTEED' if is_profitable else 'FAILED'}")
    print(f"   Using new ID format: FIN + 15-digit number")

if __name__ == "__main__":
    main()
