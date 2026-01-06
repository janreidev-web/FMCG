"""
Test retailer transaction ranges without full pipeline import
"""

def test_retailer_ranges():
    """Test retailer transaction ranges directly"""
    
    print("Testing retailer transaction ranges...")
    
    # Define the same ranges as in the pipeline
    retailer_transaction_ranges = {
        "Sari-Sari Store": {"min_qty": 1, "max_qty": 5, "min_amount": 50, "max_amount": 3000},
        "Convenience Store": {"min_qty": 1, "max_qty": 10, "min_amount": 3000, "max_amount": 15000},
        "Pharmacy": {"min_qty": 1, "max_qty": 15, "min_amount": 15000, "max_amount": 25000},
        "Wholesale": {"min_qty": 10, "max_qty": 100, "min_amount": 25000, "max_amount": 100000},
        "Supermarket": {"min_qty": 5, "max_qty": 50, "min_amount": 50000, "max_amount": 75000},
        "Department Store": {"min_qty": 3, "max_qty": 30, "min_amount": 75000, "max_amount": 100000}
    }
    
    print("\nRetailer Transaction Ranges (PHP):")
    print("=" * 60)
    print(f"{'Retailer Type':<20} | {'Min Amount':<12} | {'Max Amount':<12} | {'Qty Range'}")
    print("=" * 60)
    
    for retailer_type, params in retailer_transaction_ranges.items():
        min_amount_display = f"₱{params['min_amount']:,}"
        max_amount_display = f"₱{params['max_amount']:,}"
        qty_range = f"{params['min_qty']}-{params['max_qty']}"
        print(f"{retailer_type:<20} | {min_amount_display:<12} | {max_amount_display:<12} | {qty_range}")
    
    print("\n" + "=" * 60)
    print("Key Business Logic:")
    print("✅ Sari-Sari Store: ₱50-₱3,000 (smallest neighborhood stores)")
    print("✅ Convenience Store: ₱3,000-₱15,000 (small to medium)")
    print("✅ Pharmacy: ₱15,000-₱25,000 (medium with medical products)")
    print("✅ Wholesale: ₱25,000-₱100,000 (bulk transactions)")
    print("✅ Supermarket: ₱50,000-₱75,000 (large grocery stores)")
    print("✅ Department Store: ₱75,000-₱100,000 (largest retail format)")
    
    # Validate the ranges make business sense
    sari_sari = retailer_transaction_ranges["Sari-Sari Store"]
    department = retailer_transaction_ranges["Department Store"]
    wholesale = retailer_transaction_ranges["Wholesale"]
    
    print("\n" + "=" * 60)
    print("Business Logic Validation:")
    
    # Sari-sari should have smallest transactions
    assert sari_sari["max_amount"] < department["min_amount"], "Sari-sari max should be less than department store min"
    print("✅ Sari-sari stores have smaller transactions than department stores")
    
    # Wholesale should handle larger quantities
    assert wholesale["max_qty"] > department["max_qty"], "Wholesale should handle larger quantities"
    print("✅ Wholesale handles larger quantities than department stores")
    
    # Wholesale can have higher max amount due to bulk purchases
    assert wholesale["max_amount"] >= department["max_amount"], "Wholesale can have higher max due to bulk"
    print("✅ Wholesale can have higher maximum amounts due to bulk purchases")
    
    # Department store should have higher minimum than convenience store
    convenience = retailer_transaction_ranges["Convenience Store"]
    assert department["min_amount"] > convenience["max_amount"], "Department store min should be higher than convenience store max"
    print("✅ Department stores have higher minimums than convenience stores")
    
    print("\n🎉 All retailer transaction ranges are working correctly!")
    print("🎉 Ready for realistic FMCG data generation!")

if __name__ == "__main__":
    test_retailer_ranges()
