"""
Debug the data generation error
"""

import sys
sys.path.append('src')

from core.generators import EmployeeGenerator, ProductGenerator, RetailerGenerator, LocationGenerator, DepartmentGenerator, JobGenerator
from faker import Faker

def debug_generation():
    """Debug the data generation error"""
    
    print("Debugging data generation error...")
    
    faker = Faker('en_PH')
    
    # Test department and job generation
    from core.generators import DepartmentGenerator, JobGenerator
    dept_gen = DepartmentGenerator(faker)
    jobs_gen = JobGenerator(faker, dept_gen.generate_departments())
    
    loc_gen = LocationGenerator(faker)
    locations = loc_gen.generate_locations(3)
    
    print(f"Generated {len(locations)} locations")
    print(f"Location columns: {locations.columns.tolist()}")
    
    # Check if locations is empty
    if len(locations) == 0:
        print("ERROR: No locations generated!")
        return
    
    # Test employee generation with error handling
    try:
        emp_gen = EmployeeGenerator(faker, dept_gen.generate_departments(), jobs_gen.generate_jobs(), locations)
        employees = emp_gen.generate_employees(2)
        print(f"Generated {len(employees)} employees")
        print("✅ Employee generation working")
    except Exception as e:
        print(f"❌ Employee generation error: {e}")
        import traceback
        traceback.print_exc()
        
        # Try with more locations
        try:
            more_locations = loc_gen.generate_locations(10)
            print(f"Trying with {len(more_locations)} locations...")
            emp_gen = EmployeeGenerator(faker, dept_gen.generate_departments(), jobs_gen.generate_jobs(), more_locations)
            employees = emp_gen.generate_employees(2)
            print(f"✅ Employee generation working with more locations")
        except Exception as e2:
            print(f"❌ Still failing: {e2}")

if __name__ == "__main__":
    debug_generation()
