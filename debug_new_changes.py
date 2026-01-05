"""
Debug issues with the new changes
"""

import sys
sys.path.append('src')

def debug_new_changes():
    """Debug issues with new table implementations"""
    
    print("Debugging new changes...")
    
    try:
        # Test schemas import
        from data.schemas import ALL_SCHEMAS
        print(f"✅ Schemas imported: {len(ALL_SCHEMAS)} tables")
        
        # Check all 16 tables
        expected_tables = [
            'dim_employees', 'dim_products', 'dim_retailers', 'dim_campaigns',
            'dim_locations', 'dim_departments', 'dim_jobs', 'dim_banks',
            'dim_insurance', 'dim_categories', 'dim_brands', 'dim_subcategories',
            'fact_sales', 'fact_inventory', 'fact_operating_costs', 'fact_marketing_costs'
        ]
        
        missing_tables = []
        for table in expected_tables:
            if table not in ALL_SCHEMAS:
                missing_tables.append(table)
        
        if missing_tables:
            print(f"❌ Missing schemas: {missing_tables}")
        else:
            print("✅ All 16 schemas present")
        
        # Test generators
        from core.generators import LocationGenerator, DepartmentGenerator, JobGenerator, EmployeeGenerator, ProductGenerator, RetailerGenerator, CampaignGenerator
        from faker import Faker
        
        faker = Faker('en_PH')
        
        # Test location generation
        loc_gen = LocationGenerator(faker)
        locations = loc_gen.generate_locations(2)
        print(f"✅ Generated {len(locations)} locations")
        
        # Test product generation
        prod_gen = ProductGenerator(faker)
        products, categories, subcategories, brands = prod_gen.generate_products(2)
        print(f"✅ Generated {len(products)} products, {len(categories)} categories, {len(subcategories)} subcategories, {len(brands)} brands")
        
        # Test schema consistency for new tables
        new_tables = ['dim_categories', 'dim_subcategories', 'dim_brands', 'dim_banks', 'dim_insurance']
        
        for table_name in new_tables:
            if table_name in ALL_SCHEMAS:
                schema = ALL_SCHEMAS[table_name]
                id_field = next(field for field in schema.fields if field['name'].endswith('_id'))
                print(f"✅ {table_name} ID field: {id_field['name']} ({id_field['type']})")
            else:
                print(f"❌ {table_name} schema missing")
        
        print("✅ All new changes working properly!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_new_changes()
