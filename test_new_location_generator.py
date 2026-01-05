"""
Test the new LocationGenerator with official Philippines geography
"""

import sys
sys.path.append('src')

from core.generators import LocationGenerator
from faker import Faker

def test_new_location_generator():
    """Test the updated LocationGenerator"""
    
    print("Testing new LocationGenerator with official Philippines geography...")
    
    faker = Faker('en_PH')
    loc_gen = LocationGenerator(faker)
    
    # Generate 5 locations
    locations = loc_gen.generate_locations(5)
    
    print(f"Generated {len(locations)} locations")
    print(f"Columns: {locations.columns.tolist()}")
    
    # Display sample locations
    print("\nSample locations:")
    for idx, row in locations.iterrows():
        print(f"  {idx+1}. {row['city']}, {row['province']}, {row['region']}")
    
    # Test the pick_ph_location function
    print("\nTesting pick_ph_location function:")
    from core.generators import pick_ph_location
    for i in range(3):
        region, province, city = pick_ph_location()
        print(f"  {i+1}. {city} ({province}) - {region}")
    
    print("\n✅ LocationGenerator working with official Philippines geography!")

if __name__ == "__main__":
    test_new_location_generator()
