#!/usr/bin/env python3
"""
Test script to verify the performance optimization for retailer eligibility checking
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import time
import random

def create_test_retailers():
    """Create test retailer data similar to the real dataset"""
    retailers = []
    statuses = ['Active', 'Terminated']
    
    for i in range(500):  # Same as real dataset
        status = random.choice(statuses)
        deactivation_date = None
        
        if status == 'Terminated':
            # Random deactivation date between 2015-2025
            start_year = random.randint(2015, 2025)
            start_month = random.randint(1, 12)
            start_day = random.randint(1, 28)
            deactivation_date = date(start_year, start_month, start_day)
        
        retailers.append({
            'retailer_id': f'RET_{i:04d}',
            'status': status,
            'deactivation_date': deactivation_date,
            'retailer_type': random.choice(['Supermarket', 'Convenience', 'Wholesale', 'Specialty'])
        })
    
    return pd.DataFrame(retailers)

def test_old_method(retailers, test_dates):
    """Test the old inefficient method"""
    print("Testing OLD method (nested loops)...")
    start_time = time.time()
    
    for order_date in test_dates:
        eligible_retailers = []
        for _, retailer in retailers.iterrows():
            if retailer['status'] == 'Active':
                eligible_retailers.append(retailer)
            elif retailer['status'] == 'Terminated' and retailer['deactivation_date'] is not None:
                if order_date < retailer['deactivation_date']:
                    eligible_retailers.append(retailer)
    
    end_time = time.time()
    return end_time - start_time

def test_new_method(retailers, test_dates):
    """Test the new optimized method with caching"""
    print("Testing NEW method (pre-computed cache)...")
    start_time = time.time()
    
    # Pre-compute cache
    retailer_eligibility_cache = {}
    for current_date in test_dates:
        eligible_mask = (
            (retailers['status'] == 'Active') |
            ((retailers['status'] == 'Terminated') & 
             (retailers['deactivation_date'].notna()) & 
             (current_date < retailers['deactivation_date']))
        )
        eligible_retailers_df = retailers[eligible_mask]
        retailer_eligibility_cache[current_date] = eligible_retailers_df
    
    # Test cache lookups
    for order_date in test_dates:
        eligible_retailers_df = retailer_eligibility_cache[order_date]
    
    end_time = time.time()
    return end_time - start_time

def main():
    print("=== Performance Optimization Test ===")
    
    # Create test data
    retailers = create_test_retailers()
    print(f"Created {len(retailers)} test retailers")
    
    # Create test dates (sample of dates from the real range)
    start_date = date(2015, 1, 1)
    end_date = date(2025, 12, 31)
    
    # Test with 100 dates (representative sample)
    total_days = (end_date - start_date).days + 1
    sample_indices = np.linspace(0, total_days - 1, 100, dtype=int)
    test_dates = [start_date + timedelta(days=int(i)) for i in sample_indices]
    
    print(f"Testing with {len(test_dates)} sample dates")
    
    # Test old method
    old_time = test_old_method(retailers, test_dates)
    print(f"OLD method time: {old_time:.4f} seconds")
    
    # Test new method
    new_time = test_new_method(retailers, test_dates)
    print(f"NEW method time: {new_time:.4f} seconds")
    
    # Calculate improvement
    if old_time > 0:
        improvement = (old_time - new_time) / old_time * 100
        speedup = old_time / new_time if new_time > 0 else float('inf')
        print(f"\n=== RESULTS ===")
        print(f"Performance improvement: {improvement:.1f}%")
        print(f"Speedup factor: {speedup:.1f}x")
        
        # Extrapolate to full dataset
        estimated_old_time_full = old_time * (4042 / 100) * 124  # Full days × avg daily transactions
        estimated_new_time_full = new_time * (4042 / 100)
        print(f"\nEstimated time for full 500K transactions:")
        print(f"OLD method: {estimated_old_time_full:.1f} seconds ({estimated_old_time_full/60:.1f} minutes)")
        print(f"NEW method: {estimated_new_time_full:.1f} seconds ({estimated_new_time_full/60:.1f} minutes)")

if __name__ == "__main__":
    main()
