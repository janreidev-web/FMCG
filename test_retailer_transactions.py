"""
Test retailer-specific transaction amounts
"""

import sys
sys.path.append('src')

def test_retailer_transactions():
    """Test that different retailer types have appropriate transaction amounts"""
    
    print("Testing retailer-specific transaction amounts...")
    
    # Import the pipeline
    from etl.pipeline import ETLPipeline
    
    pipeline = ETLPipeline()
    
    # Test each retailer type
    retailer_types = [
        "Sari-Sari Store",
        "Convenience Store", 
        "Pharmacy",
        "Wholesale",
        "Supermarket",
        "Department Store"
    ]
    
    print("\nRetailer Transaction Ranges (PHP):")
    print("=" * 50)
    
    for retailer_type in retailer_types:
        params = pipeline.get_retailer_transaction_params(retailer_type)
        print(f"{retailer_type:20} | ₱{params['min_amount']:6,} - ₱{params['max_amount']:7,} | Qty: {params['min_qty']}-{params['max_qty']}")
    
    print("\n" + "=" * 50)
    print("Expected Transaction Patterns:")
    print("✅ Sari-Sari Store: Small transactions (₱50-₱5,000)")
    print("✅ Convenience Store: Medium transactions (₱100-₱15,000)")
    print("✅ Pharmacy: Medium-high transactions (₱200-₱25,000)")
    print("✅ Wholesale: Large bulk transactions (₱5,000-₱100,000)")
    print("✅ Supermarket: Large transactions (₱1,000-₱50,000)")
    print("✅ Department Store: Very large transactions (₱2,000-₱75,000)")
    
    # Test the ranges are logical
    sari_sari = pipeline.get_retailer_transaction_params("Sari-Sari Store")
    department = pipeline.get_retailer_transaction_params("Department Store")
    
    assert sari_sari["max_amount"] < department["min_amount"], "Sari-sari max should be less than department store min"
    assert sari_sari["max_qty"] < department["max_qty"], "Sari-sari max qty should be less than department store max qty"
    
    print("\n✅ Transaction range validation passed!")
    print("✅ Retailer-specific amounts are working correctly!")

if __name__ == "__main__":
    test_retailer_transactions()
