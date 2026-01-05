"""
Check and fix remaining errors in the codebase
"""

def check_critical_errors():
    """Check for critical errors that would prevent deployment"""
    
    print("=" * 60)
    print("CRITICAL ERROR CHECK")
    print("=" * 60)
    
    errors = []
    
    # Test 1: Core module imports
    try:
        import sys
        sys.path.append('src')
        
        # Test generators
        from core.generators import LocationGenerator, DepartmentGenerator, ProductGenerator
        from faker import Faker
        
        # Test basic functionality
        faker = Faker()
        loc_gen = LocationGenerator(faker)
        locations = loc_gen.generate_locations(2)
        assert len(locations) == 2
        
        print("✅ Core generators working")
        
        # Test schemas
        from data.schemas import ALL_SCHEMAS
        assert len(ALL_SCHEMAS) == 11
        print("✅ Schemas working")
        
        # Test ID generation
        from utils.id_generation import id_generator
        test_id = id_generator.generate_id('dim_employees')
        assert len(test_id) == 18  # 3 letters + 15 digits
        print("✅ ID generation working")
        
    except Exception as e:
        errors.append(f"Core module error: {e}")
        print(f"❌ Core module error: {e}")
    
    # Test 2: Data generation functionality
    try:
        from core.generators import EmployeeGenerator, ProductGenerator, RetailerGenerator
        from faker import Faker
        
        faker = Faker('en_PH')
        
        # Test department and job generation
        from core.generators import DepartmentGenerator, JobGenerator
        dept_gen = DepartmentGenerator(faker)
        jobs_gen = JobGenerator(faker, dept_gen.generate_departments())
        
        loc_gen = LocationGenerator(faker)
        locations = loc_gen.generate_locations(3)
        
        # Test employee generation
        emp_gen = EmployeeGenerator(faker, dept_gen.generate_departments(), jobs_gen.generate_jobs(), locations)
        employees = emp_gen.generate_employees(2)
        assert len(employees) == 2
        assert 'gender' in employees.columns
        print("✅ Employee generation with gender working")
        
        # Test product generation
        prod_gen = ProductGenerator(faker)
        products, categories, subcategories, brands = prod_gen.generate_products(2)
        assert len(products) == 2
        assert len(categories) > 0
        assert len(subcategories) > 0
        assert len(brands) > 0
        print("✅ Product hierarchy generation working")
        
        # Test retailer generation
        retail_gen = RetailerGenerator(faker)
        retailers = retail_gen.generate_retailers(2, locations)
        assert len(retailers) == 2
        print("✅ Retailer generation working")
        
    except Exception as e:
        errors.append(f"Data generation error: {e}")
        print(f"❌ Data generation error: {e}")
    
    # Test 3: ID format consistency
    try:
        from utils.id_generation import id_generator
        
        # Test different table prefixes
        emp_id = id_generator.generate_id('dim_employees')
        prod_id = id_generator.generate_id('dim_products')
        ret_id = id_generator.generate_id('dim_retailers')
        
        # Verify format: 3 letters + 15 digits
        assert emp_id[:3] == 'EMP'
        assert prod_id[:3] == 'PRO'
        assert ret_id[:3] == 'RET'
        assert len(emp_id) == 18
        assert len(prod_id) == 18
        assert len(ret_id) == 18
        assert emp_id[3:].isdigit()
        assert prod_id[3:].isdigit()
        assert ret_id[3:].isdigit()
        
        print("✅ ID format consistency verified")
        
    except Exception as e:
        errors.append(f"ID generation error: {e}")
        print(f"❌ ID generation error: {e}")
    
    # Test 4: Schema consistency
    try:
        from data.schemas import ALL_SCHEMAS
        
        # Check that all dimension tables use STRING type for IDs
        dimension_tables = ['dim_employees', 'dim_products', 'dim_retailers', 'dim_campaigns', 'dim_locations', 'dim_departments', 'dim_jobs']
        
        for table_name in dimension_tables:
            if table_name in ALL_SCHEMAS:
                schema = ALL_SCHEMAS[table_name]
                id_field = next(field for field in schema.fields if field['name'].endswith('_id'))
                if id_field['type'] != 'STRING':
                    errors.append(f"{table_name} ID field should be STRING, got {id_field['type']}")
                    print(f"❌ {table_name} ID field type error")
        
        if not any('ID field type error' in str(e) for e in errors):
            print("✅ Schema ID types consistent")
        
    except Exception as e:
        errors.append(f"Schema consistency error: {e}")
        print(f"❌ Schema consistency error: {e}")
    
    # Test 5: Faker locale compatibility
    try:
        from faker import Faker
        
        # Test Philippine locale
        faker = Faker('en_PH')
        
        # Test basic methods
        faker.name()
        faker.email()
        faker.date()
        faker.company()
        
        print("✅ Faker Philippine locale working")
        
    except Exception as e:
        errors.append(f"Faker locale error: {e}")
        print(f"❌ Faker locale error: {e}")
    
    print("\n" + "=" * 60)
    print("ERROR SUMMARY")
    print("=" * 60)
    
    if not errors:
        print("✅ NO CRITICAL ERRORS FOUND!")
        print("✅ All core functionality is working properly")
        print("✅ Ready for deployment")
        return True
    else:
        print(f"❌ {len(errors)} critical errors found:")
        for error in errors:
            print(f"   - {error}")
        return False

if __name__ == "__main__":
    success = check_critical_errors()
    print(f"\nDeployment Ready: {'YES' if success else 'NO'}")
