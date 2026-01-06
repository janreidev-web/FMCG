"""
Data generators for FMCG Data Analytics Platform
"""

import random
import pandas as pd
from datetime import datetime, timedelta, date
from typing import List, Dict, Any, Tuple
from faker import Faker
try:
    from ..utils.logger import default_logger
except ImportError:
    # Fallback if logger module not available
    default_logger = None
try:
    from ..utils.id_generation import id_generator
except ImportError:
    # Fallback for direct import
    import sys
    import os
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
    from id_generation import id_generator

# =====================================================
# PHILIPPINES REGIONAL GEOGRAPHY (OFFICIAL)
# Region → Province → Key Cities / Municipalities
# =====================================================

PH_GEOGRAPHY = {
    "Region I - Ilocos": {
        "Ilocos Norte": ["Laoag"],
        "Ilocos Sur": ["Vigan"],
        "La Union": ["San Fernando"],
        "Pangasinan": ["Dagupan", "San Carlos", "Urdaneta"]
    },

    "Region II - Cagayan Valley": {
        "Batanes": ["Basco"],
        "Cagayan": ["Tuguegarao"],
        "Isabela": ["Ilagan"],
        "Nueva Vizcaya": ["Bayombong"],
        "Quirino": ["Cabarroguis"]
    },

    "Region III - Central Luzon": {
        "Aurora": ["Baler"],
        "Bataan": ["Balanga"],
        "Bulacan": ["Malolos"],
        "Nueva Ecija": ["Palayan", "Cabanatuan"],
        "Pampanga": ["San Fernando"],
        "Tarlac": ["Tarlac City"],
        "Zambales": ["Olongapo"]
    },

    "Region IV-A - CALABARZON": {
        "Cavite": ["Tagaytay", "Dasmariñas"],
        "Laguna": ["Santa Rosa", "Biñan", "San Pedro"],
        "Batangas": ["Batangas City", "Lipa"],
        "Rizal": ["Antipolo"],
        "Quezon": ["Lucena"]
    },

    "Region IV-B - MIMAROPA": {
        "Occidental Mindoro": ["Mamburao"],
        "Oriental Mindoro": ["Calapan"],
        "Marinduque": ["Boac"],
        "Romblon": ["Romblon"],
        "Palawan": ["Puerto Princesa"]
    },

    "Region V - Bicol": {
        "Albay": ["Legazpi"],
        "Camarines Norte": ["Daet"],
        "Camarines Sur": ["Naga"],
        "Catanduanes": ["Virac"],
        "Masbate": ["Masbate City"],
        "Sorsogon": ["Sorsogon City"]
    },

    "Region VI - Western Visayas": {
        "Aklan": ["Kalibo"],
        "Antique": ["San Jose de Buenavista"],
        "Capiz": ["Roxas City"],
        "Iloilo": ["Iloilo City"],
        "Negros Occidental": ["Bacolod"]
    },

    "Region VII - Central Visayas": {
        "Bohol": ["Tagbilaran"],
        "Cebu": ["Cebu City", "Lapu-Lapu", "Mandaue"],
        "Negros Oriental": ["Dumaguete"],
        "Siquijor": ["Siquijor"]
    },

    "Region VIII - Eastern Visayas": {
        "Biliran": ["Naval"],
        "Eastern Samar": ["Borongan"],
        "Leyte": ["Tacloban"],
        "Northern Samar": ["Catarman"],
        "Samar": ["Catbalogan"],
        "Southern Leyte": ["Maasin"]
    },

    "Region IX - Zamboanga Peninsula": {
        "Zamboanga del Norte": ["Dipolog"],
        "Zamboanga del Sur": ["Pagadian"],
        "Zamboanga Sibugay": ["Ipil"]
    },

    "Region X - Northern Mindanao": {
        "Bukidnon": ["Malaybalay"],
        "Camiguin": ["Mambajao"],
        "Lanao del Norte": ["Iligan"],
        "Misamis Occidental": ["Oroquieta"],
        "Misamis Oriental": ["Cagayan de Oro"]
    },

    "Region XI - Davao Region": {
        "Davao de Oro": ["Nabunturan"],
        "Davao del Norte": ["Tagum"],
        "Davao del Sur": ["Digos"],
        "Davao Occidental": ["Malita"],
        "Davao Oriental": ["Mati"]
    },

    "Region XII - SOCCSKSARGEN": {
        "Cotabato": ["Kidapawan"],
        "Sarangani": ["Alabel"],
        "South Cotabato": ["Koronadal", "General Santos"],
        "Sultan Kudarat": ["Isulan"]
    },

    "Region XIII - Caraga": {
        "Agusan del Norte": ["Butuan"],
        "Agusan del Sur": ["Bayugan"],
        "Surigao del Norte": ["Surigao City"],
        "Surigao del Sur": ["Tandag"],
        "Dinagat Islands": ["San Jose"]
    },

    "Region XIV - BARMM": {
        "Basilan": ["Isabela City"],
        "Lanao del Sur": ["Marawi"],
        "Maguindanao": ["Cotabato City"],
        "Sulu": ["Jolo"],
        "Tawi-Tawi": ["Bongao"]
    },

    "Region XV - NCR": {
        "Metro Manila": [
            "Manila", "Quezon City", "Makati", "Pasig",
            "Taguig", "Mandaluyong", "Muntinlupa",
            "Parañaque", "Marikina", "Caloocan",
            "Las Piñas", "Valenzuela", "Malabon",
            "Navotas", "San Juan", "Pasay", "Pateros"
        ]
    },

    "Region XVI - CAR": {
        "Abra": ["Bangued"],
        "Apayao": ["Kabugao"],
        "Benguet": ["Baguio City"],
        "Ifugao": ["Lagawe"],
        "Kalinga": ["Tabuk"],
        "Mountain Province": ["Bontoc"]
    }
}

def pick_ph_location():
    """Pick a random Philippine location (region, province, city)"""
    region = random.choice(list(PH_GEOGRAPHY.keys()))
    province = random.choice(list(PH_GEOGRAPHY[region].keys()))
    city = random.choice(PH_GEOGRAPHY[region][province])
    return region, province, city


class DataGenerator:
    """Base class for data generators"""
    
    def __init__(self, faker: Faker):
        self.faker = faker
        self.logger = default_logger
        
    def generate_id(self, start: int = 1) -> int:
        """Generate unique ID"""
        return start + random.randint(0, 1000000)


class LocationGenerator(DataGenerator):
    """Generate Philippines location data using official geography"""
    
    def generate_locations(self, count: int) -> pd.DataFrame:
        """Generate location data using official Philippines geography"""
        locations = []
        
        for i in range(count):
            # Use official Philippines geography
            region, province, city = pick_ph_location()
            
            location = {
                "location_id": id_generator.generate_id('dim_locations'),
                "region": region,
                "province": province,
                "city": city,
                "latitude": float(self.faker.latitude()),
                "longitude": float(self.faker.longitude()),
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
            locations.append(location)
        
        return pd.DataFrame(locations)


class DepartmentGenerator(DataGenerator):
    """Generate department data"""
    
    DEPARTMENTS = [
        {"name": "Sales", "budget": 5000000, "description": "Sales and marketing operations"},
        {"name": "Finance", "budget": 2000000, "description": "Financial planning and accounting"},
        {"name": "Human Resources", "budget": 1500000, "description": "Employee management and development"},
        {"name": "Operations", "budget": 3000000, "description": "Supply chain and logistics"},
        {"name": "IT", "budget": 2500000, "description": "Information technology and systems"},
        {"name": "Marketing", "budget": 3500000, "description": "Brand management and promotions"},
        {"name": "Customer Service", "budget": 1000000, "description": "Customer support and relations"},
        {"name": "Quality Assurance", "budget": 800000, "description": "Product quality control"},
        {"name": "Research & Development", "budget": 4000000, "description": "Product innovation and development"},
        {"name": "Administration", "budget": 1200000, "description": "Administrative support services"}
    ]
    
    def generate_departments(self) -> pd.DataFrame:
        """Generate department data"""
        departments = []
        
        for i, dept in enumerate(self.DEPARTMENTS):
            department = {
                "department_id": id_generator.generate_id('dim_departments'),
                "department_name": dept["name"],
                "parent_department_id": None,  # Top level departments
                "manager_id": None,
                "budget": dept["budget"],
                "description": dept["description"],
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
            departments.append(department)
        
        return pd.DataFrame(departments)


class JobGenerator(DataGenerator):
    """Generate job position data"""
    
    JOBS = [
        {"title": "Sales Representative", "level": "Entry", "min_salary": 18000, "max_salary": 25000, "dept": "Sales", "work_type": "Field"},
        {"title": "Sales Manager", "level": "Mid", "min_salary": 35000, "max_salary": 45000, "dept": "Sales", "work_type": "Hybrid"},
        {"title": "Sales Director", "level": "Senior", "min_salary": 60000, "max_salary": 80000, "dept": "Sales", "work_type": "Office"},
        {"title": "Accountant", "level": "Entry", "min_salary": 20000, "max_salary": 30000, "dept": "Finance", "work_type": "Office"},
        {"title": "Finance Manager", "level": "Mid", "min_salary": 40000, "max_salary": 55000, "dept": "Finance", "work_type": "Office"},
        {"title": "HR Specialist", "level": "Entry", "min_salary": 22000, "max_salary": 32000, "dept": "Human Resources", "work_type": "Office"},
        {"title": "HR Manager", "level": "Mid", "min_salary": 38000, "max_salary": 50000, "dept": "Human Resources", "work_type": "Office"},
        {"title": "Operations Supervisor", "level": "Mid", "min_salary": 32000, "max_salary": 42000, "dept": "Operations", "work_type": "Field"},
        {"title": "Operations Manager", "level": "Senior", "min_salary": 50000, "max_salary": 70000, "dept": "Operations", "work_type": "Hybrid"},
        {"title": "IT Support", "level": "Entry", "min_salary": 25000, "max_salary": 35000, "dept": "IT", "work_type": "Hybrid"},
        {"title": "Software Developer", "level": "Mid", "min_salary": 40000, "max_salary": 60000, "dept": "IT", "work_type": "Remote"},
        {"title": "IT Manager", "level": "Senior", "min_salary": 55000, "max_salary": 75000, "dept": "IT", "work_type": "Office"},
        {"title": "Marketing Coordinator", "level": "Entry", "min_salary": 20000, "max_salary": 28000, "dept": "Marketing", "work_type": "Office"},
        {"title": "Marketing Manager", "level": "Mid", "min_salary": 42000, "max_salary": 58000, "dept": "Marketing", "work_type": "Office"},
        {"title": "Customer Service Rep", "level": "Entry", "min_salary": 16000, "max_salary": 22000, "dept": "Customer Service", "work_type": "Office"},
        {"title": "QA Analyst", "level": "Entry", "min_salary": 24000, "max_salary": 34000, "dept": "Quality Assurance", "work_type": "Field"},
        {"title": "R&D Specialist", "level": "Mid", "min_salary": 45000, "max_salary": 65000, "dept": "Research & Development", "work_type": "Office"},
        {"title": "Admin Assistant", "level": "Entry", "min_salary": 15000, "max_salary": 20000, "dept": "Administration", "work_type": "Office"},
    ]
    
    def __init__(self, faker: Faker, departments_df: pd.DataFrame):
        super().__init__(faker)
        self.departments_df = departments_df
        self.dept_name_to_id = dict(zip(departments_df["department_name"], departments_df["department_id"]))
    
    def generate_jobs(self) -> pd.DataFrame:
        """Generate job position data"""
        jobs = []
        
        for i, job in enumerate(self.JOBS):
            job_data = {
                "job_id": id_generator.generate_id('dim_jobs'),
                "job_title": job["title"],
                "job_level": job["level"],
                "min_salary": job["min_salary"],
                "max_salary": job["max_salary"],
                "department_id": self.dept_name_to_id[job["dept"]],
                "work_type": job["work_type"],
                "description": f"{job['level']} level {job['title']} position",
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
            jobs.append(job_data)
        
        return pd.DataFrame(jobs)


class EmployeeGenerator(DataGenerator):
    """Generate employee data"""
    
    def __init__(self, faker: Faker, departments_df: pd.DataFrame, jobs_df: pd.DataFrame, locations_df: pd.DataFrame):
        super().__init__(faker)
        self.departments_df = departments_df
        self.jobs_df = jobs_df
        self.locations_df = locations_df
    
    def generate_employees(self, count: int) -> pd.DataFrame:
        """Generate employee data"""
        employees = []
        
        for i in range(count):
            # Random job assignment
            job = self.jobs_df.sample(1).iloc[0]
            
            # Find matching department with error handling
            matching_depts = self.departments_df[self.departments_df["department_id"] == job["department_id"]]
            if len(matching_depts) == 0:
                # Fallback: use first department
                department = self.departments_df.iloc[0]
            else:
                department = matching_depts.iloc[0]
            
            location = self.locations_df.sample(1).iloc[0]
            
            # Generate realistic salary within job range
            salary = random.uniform(job["min_salary"], job["max_salary"])
            
            # Random hire date within last 10 years
            hire_date = self.faker.date_between(start_date="-10y", end_date="today")
            
            # 10% chance of being terminated
            termination_date = None
            if random.random() < 0.1:
                termination_date = self.faker.date_between(start_date=hire_date, end_date="today")
            
            # Generate gender first, then name to match
            gender = random.choice(["Male", "Female"])
            
            # Generate names based on gender
            if gender == "Male":
                first_name = self.faker.first_name_male()
                last_name = self.faker.last_name_male()
            else:
                first_name = self.faker.first_name_female()
                last_name = self.faker.last_name_female()
            
            employee = {
                "employee_id": id_generator.generate_id('dim_employees'),
                "first_name": first_name,
                "last_name": last_name,
                "gender": gender,
                "email": self.faker.email(),
                "phone": self.faker.basic_phone_number() if hasattr(self.faker, 'basic_phone_number') else f"+63-{random.randint(900000000, 999999999)}",
                "department_id": department["department_id"],
                "job_id": job["job_id"],
                "hire_date": hire_date,
                "termination_date": termination_date,
                "location_id": location["location_id"],
                "bank_id": None,  # Will be set after bank generation
                "insurance_id": None,  # Will be set after insurance generation
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
            employees.append(employee)
        
        return pd.DataFrame(employees)


class ProductGenerator(DataGenerator):
    """Generate product data"""
    
    FMCG_CATEGORIES = [
        {"name": "Beverages", "subcategories": ["Soft Drinks", "Juices", "Water", "Energy Drinks"]},
        {"name": "Food", "subcategories": ["Snacks", "Canned Goods", "Dairy", "Bakery"]},
        {"name": "Personal Care", "subcategories": ["Soap", "Shampoo", "Toothpaste", "Deodorant"]},
        {"name": "Household", "subcategories": ["Cleaning", "Laundry", "Paper Products", "Air Fresheners"]},
        {"name": "Health", "subcategories": ["Vitamins", "Medicine", "First Aid", "Supplements"]},
        {"name": "Baby", "subcategories": ["Diapers", "Baby Food", "Formula", "Wipes"]},
        {"name": "Pet", "subcategories": ["Dog Food", "Cat Food", "Pet Toys", "Accessories"]}
    ]
    
    BRANDS = [
        "Nestlé", "Unilever", "Procter & Gamble", "Coca-Cola", "PepsiCo",
        "Mondelez", "Johnson & Johnson", "Colgate-Palmolive", "Kimberly-Clark",
        "Reckitt Benckiser", "L'Oréal", "Danone", "General Mills", "Kellogg's"
    ]
    
    def generate_products(self, count: int) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Generate product, category, subcategory, and brand data"""
        
        # Generate categories
        categories = []
        for i, cat in enumerate(self.FMCG_CATEGORIES):
            categories.append({
                "category_id": id_generator.generate_id('dim_categories'),
                "category_name": cat["name"],
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            })
        categories_df = pd.DataFrame(categories)
        
        # Generate subcategories
        subcategories = []
        subcat_id = 1
        for cat in self.FMCG_CATEGORIES:
            for subcat in cat["subcategories"]:
                subcategories.append({
                    "subcategory_id": id_generator.generate_id('dim_subcategories'),
                    "subcategory_name": subcat,
                    "category_id": list(categories_df[categories_df["category_name"] == cat["name"]]["category_id"])[0],
                    "created_at": datetime.now(),
                    "updated_at": datetime.now()
                })
        subcategories_df = pd.DataFrame(subcategories)
        
        # Generate brands
        brands = []
        for i, brand in enumerate(self.BRANDS):
            brands.append({
                "brand_id": id_generator.generate_id('dim_brands'),
                "brand_name": brand,
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            })
        brands_df = pd.DataFrame(brands)
        
        # Generate products
        products = []
        for i in range(count):
            category = categories_df.sample(1).iloc[0]
            subcategory = subcategories_df[subcategories_df["category_id"] == category["category_id"]].sample(1).iloc[0]
            brand = brands_df.sample(1).iloc[0]
            
            # Generate realistic pricing
            base_price = random.uniform(10, 500)
            cost = base_price * random.uniform(0.3, 0.7)
            
            product = {
                "product_id": id_generator.generate_id('dim_products'),
                "product_name": f"{brand['brand_name']} {subcategory['subcategory_name']} {i+1}",
                "sku": f"SKU-{i+1:06d}",
                "category_id": category["category_id"],
                "subcategory_id": subcategory["subcategory_id"],
                "brand_id": brand["brand_id"],
                "unit_price": round(base_price, 2),
                "cost": round(cost, 2),
                "status": random.choice(["Active", "Discontinued", "Pending"]),
                "launch_date": self.faker.date_between(start_date="-5y", end_date="today"),
                "discontinued_date": None,
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
            products.append(product)
        
        products_df = pd.DataFrame(products)
        
        return products_df, categories_df, subcategories_df, brands_df


class RetailerGenerator(DataGenerator):
    """Generate retailer data"""
    
    RETAILER_TYPES = ["Sari-Sari Store", "Supermarket", "Convenience Store", "Wholesale", "Pharmacy", "Department Store"]
    
    def generate_retailers(self, count: int, locations_df: pd.DataFrame) -> pd.DataFrame:
        """Generate retailer data"""
        retailers = []
        
        for i in range(count):
            location = locations_df.sample(1).iloc[0]
            
            retailer = {
                "retailer_id": id_generator.generate_id('dim_retailers'),
                "retailer_name": self.faker.company(),
                "retailer_type": random.choice(self.RETAILER_TYPES),
                "location_id": location["location_id"],
                "contact_person": self.faker.name(),
                "phone": self.faker.basic_phone_number() if hasattr(self.faker, 'basic_phone_number') else f"+63-{random.randint(900000000, 999999999)}",
                "email": self.faker.email(),
                "credit_limit": random.uniform(10000, 100000),
                "payment_terms": random.choice(["Net 30", "Net 60", "COD", "Net 90"]),
                "status": random.choice(["Active", "Inactive", "Suspended"]),
                "registration_date": self.faker.date_between(start_date="-5y", end_date="today"),
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
            retailers.append(retailer)
        
        return pd.DataFrame(retailers)


class CampaignGenerator(DataGenerator):
    """Generate marketing campaign data"""
    
    CAMPAIGN_TYPES = [
        "Product Launch", "Seasonal Promotion", "Brand Awareness", 
        "Discount Campaign", "Loyalty Program", "Digital Marketing",
        "In-Store Promotion", "Social Media Campaign"
    ]
    
    def generate_campaigns(self, count: int) -> pd.DataFrame:
        """Generate marketing campaign data"""
        campaigns = []
        
        for i in range(count):
            start_date = self.faker.date_between(start_date="-2y", end_date="today")
            duration = random.randint(30, 180)  # 1-6 months
            end_date = start_date + timedelta(days=duration)
            
            campaign = {
                "campaign_id": id_generator.generate_id('dim_campaigns'),
                "campaign_name": f"Campaign {i+1}: {random.choice(self.CAMPAIGN_TYPES)}",
                "campaign_type": random.choice(self.CAMPAIGN_TYPES),
                "start_date": start_date,
                "end_date": end_date,
                "budget": random.uniform(50000, 500000),
                "target_audience": random.choice(["All Customers", "Young Adults", "Families", "Business Owners"]),
                "status": random.choice(["Active", "Completed", "Planned", "Cancelled"]),
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
            campaigns.append(campaign)
        
        return pd.DataFrame(campaigns)
