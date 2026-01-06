"""
Calculate if the company still reaches billion-peso status
"""

def calculate_annual_revenue():
    """Calculate annual revenue based on transaction patterns"""
    
    print("CALCULATING ANNUAL REVENUE POTENTIAL")
    print("=" * 60)
    
    # Retailer transaction ranges (average amounts)
    retailer_avg_amounts = {
        "Sari-Sari Store": (50 + 3000) / 2,  # ₱1,525 average
        "Convenience Store": (3000 + 15000) / 2,  # ₱9,000 average
        "Pharmacy": (15000 + 25000) / 2,  # ₱20,000 average
        "Wholesale": (25000 + 100000) / 2,  # ₱62,500 average
        "Supermarket": (50000 + 75000) / 2,  # ₱62,500 average
        "Department Store": (75000 + 100000) / 2  # ₱87,500 average
    }
    
    # Daily transaction ranges
    retailer_daily_tx = {
        "Sari-Sari Store": (10, 30),
        "Convenience Store": (5, 15),
        "Pharmacy": (3, 8),
        "Wholesale": (1, 3),
        "Supermarket": (2, 6),
        "Department Store": (1, 4)
    }
    
    # Retailer distribution (500 stores total)
    retailer_distribution = {
        "Sari-Sari Store": 200,  # 40%
        "Convenience Store": 150, # 30%
        "Pharmacy": 50,          # 10%
        "Wholesale": 30,         # 6%
        "Supermarket": 40,       # 8%
        "Department Store": 30   # 6%
    }
    
    print("\n📊 DAILY REVENUE CALCULATION:")
    print("-" * 60)
    
    total_daily_revenue = 0
    import random
    
    for retailer_type, count in retailer_distribution.items():
        avg_amount = retailer_avg_amounts[retailer_type]
        tx_range = retailer_daily_tx[retailer_type]
        
        # Calculate daily transactions for this retailer type
        daily_tx = sum(random.randint(tx_range[0], tx_range[1]) for _ in range(count))
        daily_revenue = daily_tx * avg_amount
        total_daily_revenue += daily_revenue
        
        print(f"{retailer_type:<20} | {count:3d} stores | {daily_tx:4d} tx/day | ₱{daily_revenue:10,.0f}/day")
    
    print(f"\n{'TOTAL DAILY REVENUE':<20} | {'500 stores':>7} | {'~800 tx':>7} | ₱{total_daily_revenue:10,.0f}/day")
    
    # Calculate annual revenue
    annual_revenue = total_daily_revenue * 365
    
    print(f"\n📈 ANNUAL REVENUE PROJECTION:")
    print("-" * 60)
    print(f"Daily Revenue: ₱{total_daily_revenue:,.0f}")
    print(f"Annual Revenue: ₱{annual_revenue:,.0f}")
    print(f"In Billions: ₱{annual_revenue / 1_000_000_000:.2f} billion")
    
    print(f"\n🎯 BILLION-PESO STATUS:")
    print("-" * 60)
    if annual_revenue >= 1_000_000_000:
        print("✅ YES - This is a BILLION-PESO company!")
        print(f"   Revenue is {annual_revenue / 1_000_000_000:.1f}x the billion-peso mark")
    else:
        print("❌ NO - Not yet a billion-peso company")
        print(f"   Need ₱{(1_000_000_000 - annual_revenue):,.0f} more to reach billion-peso status")
        print(f"   Currently at {(annual_revenue / 1_000_000_000) * 100:.1f}% of target")
    
    print(f"\n💡 REVENUE BREAKDOWN:")
    print("-" * 60)
    print("High-Value Contributors:")
    print(f"  Wholesale: ₱{retailer_avg_amounts['Wholesale'] * retailer_distribution['Wholesale'] * 2 * 365:,.0f}/year")
    print(f"  Department Store: ₱{retailer_avg_amounts['Department Store'] * retailer_distribution['Department Store'] * 2.5 * 365:,.0f}/year")
    print(f"  Supermarket: ₱{retailer_avg_amounts['Supermarket'] * retailer_distribution['Supermarket'] * 4 * 365:,.0f}/year")
    
    print("\nVolume Contributors:")
    print(f"  Sari-Sari Store: ₱{retailer_avg_amounts['Sari-Sari Store'] * retailer_distribution['Sari-Sari Store'] * 20 * 365:,.0f}/year")
    print(f"  Convenience Store: ₱{retailer_avg_amounts['Convenience Store'] * retailer_distribution['Convenience Store'] * 10 * 365:,.0f}/year")

if __name__ == "__main__":
    calculate_annual_revenue()
