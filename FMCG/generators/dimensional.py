"""
Dimensional Model Data Generator for FMCG Analytics
Creates optimized dim/fact tables for BigQuery
"""

import random
from datetime import datetime, timedelta, date
from faker import Faker
from helpers import random_date_range
from geography import PH_GEOGRAPHY, pick_ph_location

fake = Faker()

def generate_dim_products(num_products=150, start_id=1):
    """Generate products dimension table with realistic FMCG products"""
    products = []
    
    # Realistic FMCG categories and subcategories
    categories = [
        "Beverages", "Food & Snacks", "Personal Care", "Household Care", 
        "Baby Care", "Health & Wellness", "Pet Care"
    ]
    
    subcategories = {
        "Beverages": [
            "Carbonated Soft Drinks", "Bottled Water", "Fruit Juices", 
            "Energy Drinks", "Ready-to-Drink Tea", "Sports Drinks",
            "Coffee Mixes", "Milk Products"
        ],
        "Food & Snacks": [
            "Chips & Crisps", "Biscuits & Cookies", "Instant Noodles",
            "Canned Goods", "Cereal & Breakfast", "Confectionery",
            "Cooking Oil", "Condiments & Spices", "Frozen Foods"
        ],
        "Personal Care": [
            "Soap & Bath", "Shampoo & Conditioner", "Toothpaste & Oral Care",
            "Deodorant", "Skin Care", "Hair Care", "Feminine Hygiene"
        ],
        "Household Care": [
            "Laundry Detergent", "Dishwashing Soap", "All-Purpose Cleaners",
            "Disinfectants", "Paper Products", "Air Fresheners",
            "Insecticides", "Kitchen Cleaners"
        ],
        "Baby Care": [
            "Baby Diapers", "Baby Wipes", "Baby Formula",
            "Baby Food", "Baby Toiletries"
        ],
        "Health & Wellness": [
            "Vitamins & Supplements", "Medicated Ointments", "First Aid",
            "Health Drinks", "Herbal Products"
        ],
        "Pet Care": [
            "Pet Food", "Pet Accessories", "Pet Grooming"
        ]
    }
    
    # Realistic Filipino and international FMCG brands
    brands = [
        # Local brands
        "Nescafé", "Lucky Me", "Maggi", "Bear Brand", "Kopiko", "Coffee Mate",
        "Great Taste", "Royal", "Coke", "Sprite", "Sarsi", "Pop Cola",
        "Head & Shoulders", "Pantene", "Safeguard", "Dove", "Colgate",
        "Close Up", "Tide", "Downy", "Ariel", "Breeze", "Surf",
        "Joy", "Domex", "Raid", "Baygon", "Mortein", "Green Cross",
        "Biogesic", "Medicol", "Alaxan", "Decolgen", "Strepsils",
        # International brands
        "Unilever", "P&G", "Nestlé", "Coca-Cola", "Pepsi", "Johnson & Johnson",
        "Kimberly-Clark", "Mondelez", "Procter & Gamble"
    ]
    
    # Product sizes and units
    sizes = ["50ml", "100ml", "200ml", "250ml", "500ml", "1L", "1.5L", "2L", 
             "50g", "100g", "200g", "500g", "1kg", "5kg",
             "10pcs", "20pcs", "50pcs", "100pcs", "200pcs",
             "Sachet", "Pouch", "Bottle", "Can", "Box", "Pack"]
    
    for i in range(num_products):
        category = random.choice(categories)
        subcategory = random.choice(subcategories[category])
        brand = random.choice(brands)
        size = random.choice(sizes)
        
        # Realistic pricing based on category and size
        if category == "Beverages":
            wholesale_price = round(random.uniform(15, 120), 2)
        elif category == "Food & Snacks":
            wholesale_price = round(random.uniform(20, 250), 2)
        elif category == "Personal Care":
            wholesale_price = round(random.uniform(25, 300), 2)
        elif category == "Household Care":
            wholesale_price = round(random.uniform(30, 400), 2)
        elif category == "Baby Care":
            wholesale_price = round(random.uniform(40, 500), 2)
        elif category == "Health & Wellness":
            wholesale_price = round(random.uniform(35, 600), 2)
        else:  # Pet Care
            wholesale_price = round(random.uniform(50, 800), 2)
        
        # Retail price with realistic markup (30-100% markup)
        markup = random.uniform(1.3, 2.0)
        retail_price = round(wholesale_price * markup, 2)
        
        # Generate realistic product name
        product_variants = ["Premium", "Classic", "Extra", "Plus", "Max", "Fresh", "Clean", "Pure"]
        variant = random.choice(product_variants) if random.random() < 0.3 else ""
        
        product_name = f"{brand} {variant} {subcategory.split()[0]} {size}".strip()
        
        # Product lifecycle status
        status_weights = [0.85, 0.10, 0.05]  # 85% Active, 10% Discontinued, 5% New
        status = random.choices(["Active", "Discontinued", "New"], weights=status_weights)[0]
        
        products.append({
            "product_key": start_id + i,
            "product_id": f"P{start_id + i:05}",
            "product_name": product_name,
            "category": category,
            "subcategory": subcategory,
            "brand": brand,
            "wholesale_price": wholesale_price,
            "retail_price": retail_price,
            "status": status,
            "created_date": fake.date_between_dates(date_start=date(2015, 1, 1), date_end=date.today())
        })
    
    return products

def generate_dim_employees(num_employees=200, start_id=1):
    """Generate employees dimension table with realistic FMCG company structure"""
    employees = []
    
    # Realistic FMCG company departments
    departments = [
        "Sales", "Marketing", "Operations", "Finance", "Human Resources", 
        "Supply Chain", "Quality Assurance", "IT", "Customer Service", "Administration"
    ]
    
    # Realistic positions by department with hierarchy
    positions = {
        "Sales": [
            "Sales Representative", "Senior Sales Rep", "Sales Supervisor", 
            "Area Sales Manager", "Regional Sales Manager", "Sales Director"
        ],
        "Marketing": [
            "Marketing Assistant", "Brand Specialist", "Digital Marketing Specialist",
            "Brand Manager", "Marketing Manager", "Marketing Director"
        ],
        "Operations": [
            "Operations Staff", "Warehouse Supervisor", "Operations Supervisor",
            "Operations Manager", "Plant Manager", "Operations Director"
        ],
        "Finance": [
            "Accounting Staff", "Financial Analyst", "Senior Accountant",
            "Finance Manager", "Controller", "CFO"
        ],
        "Human Resources": [
            "HR Assistant", "HR Specialist", "HR Supervisor",
            "HR Manager", "HR Director"
        ],
        "Supply Chain": [
            "Logistics Coordinator", "Supply Chain Analyst", "Warehouse Manager",
            "Supply Chain Manager", "Logistics Director"
        ],
        "Quality Assurance": [
            "QA Inspector", "QA Analyst", "QA Supervisor",
            "QA Manager", "Quality Director"
        ],
        "IT": [
            "IT Support", "Systems Administrator", "IT Analyst",
            "IT Manager", "IT Director"
        ],
        "Customer Service": [
            "Customer Service Rep", "Senior CSR", "Customer Service Supervisor",
            "Customer Service Manager"
        ],
        "Administration": [
            "Administrative Assistant", "Executive Assistant", "Office Manager",
            "Admin Manager"
        ]
    }
    
    # Department size distribution (realistic for 200-employee FMCG company)
    dept_distribution = {
        "Sales": 0.30,      # 30% of workforce
        "Operations": 0.25,   # 25% of workforce
        "Marketing": 0.10,    # 10% of workforce
        "Supply Chain": 0.08, # 8% of workforce
        "Finance": 0.08,      # 8% of workforce
        "Customer Service": 0.06, # 6% of workforce
        "Quality Assurance": 0.05, # 5% of workforce
        "Human Resources": 0.04, # 4% of workforce
        "IT": 0.02,           # 2% of workforce
        "Administration": 0.02  # 2% of workforce
    }
    
    # Generate employees by department
    employee_id = start_id
    for dept, percentage in dept_distribution.items():
        dept_count = int(num_employees * percentage)
        dept_positions = positions[dept]
        
        for i in range(dept_count):
            # Position assignment with realistic hierarchy
            if i == 0 and dept_count > 10:  # Department head
                position = dept_positions[-1]  # Highest position
            elif i < dept_count * 0.1 and dept_count > 5:  # Senior management
                position = random.choice(dept_positions[-3:])  # Senior positions
            elif i < dept_count * 0.3:  # Middle management
                position = random.choice(dept_positions[-5:-2])  # Middle positions
            else:  # Staff level
                position = random.choice(dept_positions[:3])  # Staff positions
            
            hire_date = fake.date_between_dates(date_start=date(2015, 1, 1), date_end=date.today())
            
            # Realistic turnover rate (15% annual)
            termination_date = None
            years_employed = (date.today() - hire_date).days / 365.25
            turnover_probability = 1 - (0.85 ** years_employed)  # Cumulative turnover
            
            if random.random() < turnover_probability:
                termination_date = fake.date_between_dates(date_start=hire_date, date_end=date.today())
                employment_status = "Terminated"
            else:
                employment_status = "Active"
            
            employees.append({
                "employee_key": employee_id,
                "employee_id": f"E{employee_id:05}",
                "full_name": fake.name(),
                "department": dept,
                "position": position,
                "employment_status": employment_status,
                "hire_date": hire_date,
                "termination_date": termination_date
            })
            
            employee_id += 1
    
    return employees

def generate_dim_retailers(num_retailers=500, start_id=1):
    """Generate retailers dimension table with realistic Philippine retail landscape"""
    retailers = []
    
    # Realistic Philippine retail chain types and distribution
    retailer_types = [
        "Supermarket", "Hypermarket", "Convenience Store", "Sari-Sari Store",
        "Drugstore", "Wholesale Club", "Department Store", "Specialty Store",
        "Online Retailer", "Flea Market Stall"
    ]
    
    # Major Philippine retail chains
    major_chains = [
        "SM Supermarket", "Robinsons Supermarket", "Puregold", "Waltermart",
        "7-Eleven", "FamilyMart", "Ministop", "Alfamart",
        "Watsons", "Mercury Drug", "South Star Drug", "Rural Bank",
        "SM Hypermarket", "S&R Membership Shopping", "Landmark", "Rustans",
        "Daiso", "National Book Store", "Ace Hardware", "Handyman"
    ]
    
    # Retailer type distribution (realistic for Philippine market)
    type_distribution = {
        "Sari-Sari Store": 0.45,      # 45% - most common
        "Convenience Store": 0.20,    # 20% - growing rapidly
        "Supermarket": 0.15,         # 15% - traditional retail
        "Drugstore": 0.08,           # 8% - health-focused
        "Wholesale Club": 0.05,      # 5% - bulk buyers
        "Specialty Store": 0.04,      # 4% - niche markets
        "Hypermarket": 0.02,         # 2% - large format
        "Department Store": 0.01,     # 1% - premium retail
    }
    
    # Generate retailers by type
    retailer_id = start_id
    for retailer_type, percentage in type_distribution.items():
        type_count = int(num_retailers * percentage)
        
        for i in range(type_count):
            # Use accurate geography data
            region, province, city = pick_ph_location()
            
            # Generate retailer name based on type
            if retailer_type in ["Supermarket", "Hypermarket", "Convenience Store", "Drugstore"]:
                if random.random() < 0.3:  # 30% chance of being a major chain
                    retailer_name = random.choice(major_chains) + f" {city}"
                else:
                    retailer_name = f"{fake.company().split()[0]} {retailer_type}"
            elif retailer_type == "Sari-Sari Store":
                # Realistic sari-sari store names
                store_names = ["Tindahan ni", "Sari-Sari Store", "Mini Store", "Variety Store"]
                owner_names = ["Aling Nene", "Kuya Jun", "Nanay Tess", "Tito Boy", "Mang Jose"]
                retailer_name = f"{random.choice(owner_names)}'s {random.choice(store_names)}"
            else:
                retailer_name = f"{fake.company().split()[0]} {retailer_type}"
            
            retailers.append({
                "retailer_key": retailer_id,
                "retailer_id": f"R{retailer_id:04}",
                "retailer_name": retailer_name,
                "retailer_type": retailer_type,
                "city": city,
                "province": province,
                "region": region,
                "country": "PH"
            })
            
            retailer_id += 1
    
    return retailers

def generate_dim_campaigns(num_campaigns=50, start_id=1):
    """Generate campaigns dimension table"""
    campaigns = []
    campaign_types = ["TV Commercial", "Social Media", "Print Ads", "Radio", "Influencer", "Email", "Billboard", "Events"]
    
    for i in range(num_campaigns):
        start_date = fake.date_between_dates(date_start=date(2020, 1, 1), date_end=date.today())
        duration = random.randint(7, 90)
        end_date = start_date + timedelta(days=duration)
        
        # Budget based on campaign type
        campaign_type = random.choice(campaign_types)
        if campaign_type in ["TV Commercial", "Billboard"]:
            budget = random.randint(5000000, 30000000)
        elif campaign_type in ["Social Media", "Influencer"]:
            budget = random.randint(1000000, 8000000)
        elif campaign_type in ["Print Ads", "Radio"]:
            budget = random.randint(2000000, 15000000)
        else:
            budget = random.randint(500000, 5000000)
        
        campaigns.append({
            "campaign_key": start_id + i,
            "campaign_id": f"MKT{start_id + i:04}",
            "campaign_name": fake.catch_phrase(),
            "campaign_type": campaign_type,
            "start_date": start_date,
            "end_date": end_date,
            "budget": budget,
            "currency": "PHP"
        })
    
    return campaigns

def generate_fact_sales(employees, products, retailers, campaigns, target_amount, start_date=None, end_date=None, start_id=1):
    """Generate sales fact table with realistic FMCG sales patterns"""
    if start_date is None:
        start_date = date(2015, 1, 1)
    if end_date is None:
        end_date = date.today() - timedelta(days=1)
    
    sales = []
    
    # Calculate number of transactions based on target amount
    avg_transaction_value = random.uniform(800, 2000)
    num_transactions = int(target_amount / avg_transaction_value)
    
    print(f"Generating {num_transactions:,} sales transactions...")
    
    # Create lookup dictionaries for foreign keys
    employee_lookup = {e["employee_id"]: e["employee_key"] for e in employees if e["employment_status"] == "Active"}
    product_lookup = {p["product_id"]: p["product_key"] for p in products if p["status"] == "Active"}
    retailer_lookup = {r["retailer_id"]: r["retailer_key"] for r in retailers}
    campaign_lookup = {c["campaign_id"]: c["campaign_key"] for c in campaigns}
    
    # Create retailer type lookup for behavior patterns
    retailer_type_lookup = {r["retailer_key"]: r["retailer_type"] for r in retailers}
    
    for i in range(num_transactions):
        # Get foreign keys
        emp_key = random.choice(list(employee_lookup.values()))
        prod_key = random.choice(list(product_lookup.values()))
        retailer_key = random.choice(list(retailer_lookup.values()))
        retailer_type = retailer_type_lookup[retailer_key]
        
        # Campaign assignment based on retailer type
        campaign_key = None
        if retailer_type in ["Supermarket", "Hypermarket", "Department Store"]:
            # Large retailers more likely to have campaigns
            campaign_key = random.choice(list(campaign_lookup.values())) if random.random() < 0.4 else None
        elif retailer_type in ["Convenience Store", "Drugstore"]:
            campaign_key = random.choice(list(campaign_lookup.values())) if random.random() < 0.2 else None
        else:
            campaign_key = random.choice(list(campaign_lookup.values())) if random.random() < 0.1 else None
        
        # Generate sale date with seasonal patterns
        sale_date = random_date_range(start_date, end_date)
        
        # Seasonal demand variations
        seasonal_multiplier = 1.0
        month = sale_date.date().month
        if month in [11, 12]:  # Holiday season
            seasonal_multiplier = random.uniform(1.3, 1.8)
        elif month in [1, 2]:  # Post-holiday slowdown
            seasonal_multiplier = random.uniform(0.7, 0.9)
        elif month in [4, 5]:  # Summer season
            seasonal_multiplier = random.uniform(1.1, 1.3)
        elif month in [6, 7, 8]:  # Rainy season
            seasonal_multiplier = random.uniform(0.9, 1.1)
        
        # Get product details
        product = next(p for p in products if p["product_key"] == prod_key)
        
        # Retailer-specific order patterns
        if retailer_type == "Sari-Sari Store":
            case_quantity = random.randint(1, 10)  # Small orders
        elif retailer_type in ["Convenience Store", "Drugstore"]:
            case_quantity = random.randint(5, 25)  # Medium orders
        elif retailer_type in ["Supermarket", "Specialty Store"]:
            case_quantity = random.randint(10, 50)  # Large orders
        else:  # Hypermarket, Wholesale, Department Store
            case_quantity = random.randint(20, 100)  # Very large orders
        
        # Apply seasonal multiplier to quantity
        case_quantity = max(1, int(case_quantity * seasonal_multiplier))
        
        unit_price = product["wholesale_price"]
        
        # Volume-based discounts
        discount = 0
        if retailer_type in ["Hypermarket", "Wholesale Club"]:
            # Large retailers get better discounts
            if case_quantity > 50:
                discount = random.choice([10, 15, 20, 25])
            elif case_quantity > 25:
                discount = random.choice([5, 10, 15])
        elif retailer_type in ["Supermarket", "Department Store"]:
            if case_quantity > 30:
                discount = random.choice([5, 10, 15])
            elif case_quantity > 15:
                discount = random.choice([2, 5, 10])
        else:
            # Small retailers get minimal discounts
            if case_quantity > 20:
                discount = random.choice([2, 5])
            elif case_quantity > 10:
                discount = random.choice([1, 2])
        
        discount_amount = round(unit_price * case_quantity * (discount / 100), 2)
        subtotal = unit_price * case_quantity
        
        # Tax rates (Philippines VAT system)
        tax_rate = 12  # Standard VAT rate
        tax_amount = round((subtotal - discount_amount) * (tax_rate / 100), 2)
        total_amount = round(subtotal - discount_amount + tax_amount, 2)
        
        # Commission based on retailer type and product category
        if product["category"] in ["Personal Care", "Health & Wellness"]:
            commission_rate = random.uniform(0.03, 0.10)  # Higher margins
        elif product["category"] in ["Beverages", "Food & Snacks"]:
            commission_rate = random.uniform(0.02, 0.06)  # Lower margins
        else:
            commission_rate = random.uniform(0.025, 0.08)  # Standard margins
        
        commission_amount = round(total_amount * commission_rate, 2)
        
        # Delivery calculation based on retailer type and geography
        retailer = next(r for r in retailers if r["retailer_key"] == retailer_key)
        region = retailer["region"]
        
        # Delivery time based on region and retailer type
        if retailer_type == "Sari-Sari Store":
            # Small stores, frequent deliveries
            if "NCR" in region:
                expected_days = random.randint(1, 2)
            elif "Region III" in region or "Region IV-A" in region:
                expected_days = random.randint(2, 4)
            else:
                expected_days = random.randint(3, 7)
        elif retailer_type in ["Convenience Store", "Drugstore"]:
            # Regular deliveries
            if "NCR" in region:
                expected_days = random.randint(1, 3)
            elif "Region III" in region or "Region IV-A" in region:
                expected_days = random.randint(3, 5)
            else:
                expected_days = random.randint(5, 10)
        else:
            # Large retailers, scheduled deliveries
            if "NCR" in region:
                expected_days = random.randint(2, 4)
            elif "Region III" in region or "Region IV-A" in region:
                expected_days = random.randint(4, 7)
            else:
                expected_days = random.randint(7, 14)
        
        expected_delivery = sale_date.date() + timedelta(days=expected_days)
        
        # Determine delivery status
        today = date.today()
        if sale_date.date() < today:
            # Historical orders - determine actual delivery
            delay_probability = 0.2 if "NCR" in region else 0.4  # Higher delay outside NCR
            if random.random() < delay_probability:
                delay = random.randint(1, 5)
                actual_delivery = expected_delivery + timedelta(days=delay)
                delivery_status = "Delayed" if actual_delivery <= today else "In Transit"
            else:
                actual_delivery = expected_delivery
                delivery_status = "Delivered" if actual_delivery <= today else "In Transit"
        else:
            # Future orders
            actual_delivery = None
            delivery_status = "Processing" if expected_delivery > today else "In Transit"
        
        # Payment method based on retailer type
        if retailer_type == "Sari-Sari Store":
            payment_method = random.choice(["Cash", "COD", "Bank Transfer"])
        elif retailer_type in ["Convenience Store", "Drugstore"]:
            payment_method = random.choice(["Bank Transfer", "Credit Card", "COD"])
        else:
            payment_method = random.choice(["Credit Card", "Bank Transfer", "Net Terms"])
        
        # Payment status based on payment method and retailer type
        if payment_method == "Cash" or payment_method == "COD":
            payment_status = "Paid"
        elif payment_method == "Net Terms":
            payment_status = random.choices(["Paid", "Pending", "Overdue"], weights=[0.7, 0.2, 0.1])[0]
        else:
            payment_status = random.choices(["Paid", "Pending"], weights=[0.9, 0.1])[0]
        
        sales.append({
            "sale_key": start_id + i,
            "sale_date": sale_date.date(),
            "product_key": prod_key,
            "employee_key": emp_key,
            "retailer_key": retailer_key,
            "campaign_key": campaign_key,
            "case_quantity": case_quantity,
            "unit_price": unit_price,
            "discount_percent": discount,
            "tax_rate": tax_rate,
            "total_amount": total_amount,
            "commission_amount": commission_amount,
            "currency": "PHP",
            "payment_method": payment_method,
            "payment_status": payment_status,
            "delivery_status": delivery_status,
            "expected_delivery_date": expected_delivery,
            "actual_delivery_date": actual_delivery
        })
    
    return sales

def generate_fact_operating_costs(target_amount, start_date=None, end_date=None, start_id=1):
    """Generate operating costs fact table"""
    if start_date is None:
        start_date = date(2015, 1, 1)
    if end_date is None:
        end_date = date.today()
    
    costs = []
    cost_counter = 0
    current = start_date
    
    # Cost categories for ₱100M/year FMCG company (realistic cost structure)
    # Monthly costs scaled to achieve 15-20% profit margins
    categories = {
        "Factory Rent": {"type": "Fixed", "amount": 160000},
        "Warehouse Rent": {"type": "Fixed", "amount": 100000},
        "Office Rent": {"type": "Fixed", "amount": 60000},
        "Utilities": {"type": "Variable", "amount": 80000},
        "Raw Materials": {"type": "Variable", "amount": 1600000},
        "Packaging": {"type": "Variable", "amount": 400000},
        "Equipment": {"type": "Fixed", "amount": 120000},
        "Transportation": {"type": "Variable", "amount": 300000},
        "Marketing": {"type": "Variable", "amount": 500000},
        "Commissions": {"type": "Variable", "amount": 300000},
        "Quality Testing": {"type": "Variable", "amount": 80000},
        "Insurance": {"type": "Fixed", "amount": 120000},
        "Legal": {"type": "Fixed", "amount": 60000},
        "IT Systems": {"type": "Fixed", "amount": 160000},
        "Benefits": {"type": "Fixed", "amount": 600000},
        "Training": {"type": "Fixed", "amount": 40000},
        "Maintenance": {"type": "Fixed", "amount": 100000},
        "Security": {"type": "Fixed", "amount": 60000},
        "Factory Utilities": {"type": "Variable", "amount": 160000},
        "Waste Management": {"type": "Fixed", "amount": 30000},
        "Employee Salaries": {"type": "Fixed", "amount": 1600000},
        "Sales Team Salaries": {"type": "Fixed", "amount": 600000},
        "Management Salaries": {"type": "Fixed", "amount": 800000},
        "Administrative Salaries": {"type": "Fixed", "amount": 400000},
        "Warehouse Staff Salaries": {"type": "Fixed", "amount": 300000},
        "Delivery Driver Salaries": {"type": "Fixed", "amount": 400000},
        "Payroll Taxes": {"type": "Fixed", "amount": 600000},
        "Health Insurance": {"type": "Fixed", "amount": 200000},
        "Retirement Contributions": {"type": "Fixed", "amount": 160000},
        "Workmen's Compensation": {"type": "Fixed", "amount": 40000},
        "Office Supplies": {"type": "Fixed", "amount": 60000},
        "Communication Expenses": {"type": "Fixed", "amount": 100000},
        "Travel Expenses": {"type": "Variable", "amount": 160000},
        "Entertainment Expenses": {"type": "Variable", "amount": 80000},
        "Professional Services": {"type": "Fixed", "amount": 160000},
        "Accounting Services": {"type": "Fixed", "amount": 60000},
        "Banking Fees": {"type": "Fixed", "amount": 20000},
        "Credit Card Processing": {"type": "Variable", "amount": 120000},
        "Depreciation": {"type": "Fixed", "amount": 160000},
        "Property Taxes": {"type": "Fixed", "amount": 100000},
        "Business Licenses": {"type": "Fixed", "amount": 20000},
        "Research & Development": {"type": "Fixed", "amount": 200000}
    }
    
    while current <= end_date:
        for category, details in categories.items():
            # Apply inflation and seasonal variations
            years_elapsed = (current.year - start_date.year) + current.month / 12
            inflation = 1.028 ** years_elapsed
            
            seasonal = 1.0
            if current.month in [10, 11, 12]:
                seasonal = random.uniform(1.2, 1.5)
            elif current.month in [1, 2]:
                seasonal = random.uniform(0.85, 0.95)
            
            amount = round(details["amount"] * inflation * seasonal * random.uniform(0.85, 1.15), 2)
            
            costs.append({
                "cost_key": start_id + cost_counter,
                "cost_date": current,
                "category": category,
                "cost_type": details["type"],
                "amount": amount,
                "currency": "PHP"
            })
            cost_counter += 1
        
        current += timedelta(days=30)
    
    return costs

def generate_fact_inventory(products, start_date=None, end_date=None, start_id=1):
    """Generate inventory fact table"""
    if start_date is None:
        start_date = date(2015, 1, 1)
    if end_date is None:
        end_date = date.today()
    
    inventory = []
    warehouses = ["NCR Warehouse", "Luzon Hub", "Visayas Center", "Mindanao Depot"]
    
    inv_counter = 0
    
    # Generate monthly inventory snapshots
    current = start_date
    while current <= end_date:
        for product in products:
            warehouse = random.choice(warehouses)
            cases_on_hand = random.randint(100, 5000)
            unit_cost = product["wholesale_price"] * random.uniform(0.6, 0.8)
            
            inventory.append({
                "inventory_key": start_id + inv_counter,
                "inventory_date": current,
                "product_key": product["product_key"],
                "warehouse_location": warehouse,
                "cases_on_hand": cases_on_hand,
                "unit_cost": round(unit_cost, 2),
                "currency": "PHP"
            })
            inv_counter += 1
        
        current += timedelta(days=30)
    
    return inventory

def generate_fact_marketing_costs(campaigns, target_amount, start_date=None, end_date=None, start_id=1):
    """Generate marketing costs fact table"""
    if start_date is None:
        start_date = date(2015, 1, 1)
    if end_date is None:
        end_date = date.today()
    
    marketing_costs = []
    cost_counter = 0
    current = start_date
    
    # Generate monthly marketing cost snapshots
    while current <= end_date:
        # Generate marketing costs for active campaigns
        active_campaigns = [c for c in campaigns if c["start_date"] <= current <= c["end_date"]]
        
        if active_campaigns:
            # Distribute campaign budgets across active months
            for campaign in active_campaigns:
                campaign_duration = (campaign["end_date"] - campaign["start_date"]).days
                days_elapsed = (current - campaign["start_date"]).days + 1
                
                if 0 <= days_elapsed <= campaign_duration:
                    # Calculate monthly portion of campaign budget
                    monthly_budget = campaign["budget"] / max(1, campaign_duration / 30)
                    
                    # Add some variation for realistic spending
                    variation = random.uniform(0.8, 1.2)
                    actual_cost = round(monthly_budget * variation, 2)
                    
                    marketing_costs.append({
                        "marketing_cost_key": start_id + cost_counter,
                        "cost_date": current,
                        "campaign_key": campaign["campaign_key"],
                        "campaign_id": campaign["campaign_id"],
                        "campaign_type": campaign["campaign_type"],
                        "cost_category": "Campaign Spend",
                        "amount": actual_cost,
                        "currency": campaign["currency"]
                    })
                    cost_counter += 1
        
        # Add general marketing overhead costs
        overhead_costs = [
            {"category": "Marketing Team Salaries", "amount": 1500000},
            {"category": "Digital Advertising", "amount": 800000},
            {"category": "Market Research", "amount": 300000},
            {"category": "Brand Management", "amount": 500000},
            {"category": "Events & Sponsorships", "amount": 400000}
        ]
        
        for cost in overhead_costs:
            # Apply inflation and seasonal variations
            years_elapsed = (current.year - start_date.year) + current.month / 12
            inflation = 1.025 ** years_elapsed
            
            seasonal = 1.0
            if current.month in [11, 12]:  # Holiday season
                seasonal = random.uniform(1.3, 1.6)
            elif current.month in [1, 2]:  # Post-holiday
                seasonal = random.uniform(0.7, 0.9)
            
            amount = round(cost["amount"] * inflation * seasonal * random.uniform(0.9, 1.1), 2)
            
            marketing_costs.append({
                "marketing_cost_key": start_id + cost_counter,
                "cost_date": current,
                "campaign_key": None,
                "campaign_id": None,
                "campaign_type": None,
                "cost_category": cost["category"],
                "amount": amount,
                "currency": "PHP"
            })
            cost_counter += 1
        
        current += timedelta(days=30)
    
    return marketing_costs
