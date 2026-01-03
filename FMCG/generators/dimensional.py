"""
Normalized Dimensional Model Data Generator for FMCG Analytics
Generates data for the normalized schema to reduce redundancy
"""

import random
from datetime import datetime, timedelta, date
from faker import Faker
from helpers import random_date_range
from geography import PH_GEOGRAPHY, pick_ph_location

fake = Faker()

def generate_dim_locations(num_locations=500, start_id=1):
    """Generate locations dimension table with normalized address data"""
    locations = []
    location_set = set()  # To avoid duplicates
    
    for i in range(num_locations):
        # Generate unique location combinations
        max_attempts = 50
        for _ in range(max_attempts):
            region, province, city = pick_ph_location()
            street_address = fake.street_address()
            postal_code = fake.postcode()
            
            # Create unique key
            location_key = f"{street_address}|{city}|{province}|{region}"
            
            if location_key not in location_set:
                location_set.add(location_key)
                break
        else:
            # If we can't find a unique location, use a generic one
            region, province, city = pick_ph_location()
            street_address = f"Address {i+1}"
            postal_code = fake.postcode()
        
        locations.append({
            "location_key": start_id + i,
            "street_address": street_address,
            "city": city,
            "province": province,
            "region": region,
            "postal_code": postal_code,
            "country": "PH"
        })
    
    return locations

def generate_dim_departments(start_id=1):
    """Generate departments dimension table"""
    departments = [
        {"department_key": start_id, "department_name": "Sales", "department_code": "SLS", "parent_department_key": None},
        {"department_key": start_id + 1, "department_name": "Marketing", "department_code": "MKT", "parent_department_key": None},
        {"department_key": start_id + 2, "department_name": "Operations", "department_code": "OPS", "parent_department_key": None},
        {"department_key": start_id + 3, "department_name": "Finance", "department_code": "FIN", "parent_department_key": None},
        {"department_key": start_id + 4, "department_name": "Human Resources", "department_code": "HR", "parent_department_key": None},
        {"department_key": start_id + 5, "department_name": "Supply Chain", "department_code": "SCH", "parent_department_key": None},
        {"department_key": start_id + 6, "department_name": "Quality Assurance", "department_code": "QA", "parent_department_key": None},
        {"department_key": start_id + 7, "department_name": "IT", "department_code": "IT", "parent_department_key": None},
        {"department_key": start_id + 8, "department_name": "Customer Service", "department_code": "CS", "parent_department_key": None},
        {"department_key": start_id + 9, "department_name": "Administration", "department_code": "ADM", "parent_department_key": None},
        
        # Sub-departments (optional hierarchical structure)
        {"department_key": start_id + 10, "department_name": "Field Sales", "department_code": "SLS-FS", "parent_department_key": start_id},
        {"department_key": start_id + 11, "department_name": "Inside Sales", "department_code": "SLS-IS", "parent_department_key": start_id},
        {"department_key": start_id + 12, "department_name": "Digital Marketing", "department_code": "MKT-DM", "parent_department_key": start_id + 1},
        {"department_key": start_id + 13, "department_name": "Brand Marketing", "department_code": "MKT-BM", "parent_department_key": start_id + 1},
        {"department_key": start_id + 14, "department_name": "Warehouse Operations", "department_code": "OPS-WH", "parent_department_key": start_id + 2},
    ]
    
    return departments

def generate_dim_jobs(departments, start_id=1):
    """Generate jobs dimension table with normalized job information"""
    jobs = []
    job_key = start_id
    
    # Define job positions by department
    job_positions = {
        "Sales": [
            {"title": "Sales Representative", "level": "Entry", "min_sal": 18000, "max_sal": 28000, "setup": "Field-based", "type": "Full-time"},
            {"title": "Senior Sales Rep", "level": "Junior", "min_sal": 25000, "max_sal": 35000, "setup": "Field-based", "type": "Full-time"},
            {"title": "Sales Supervisor", "level": "Senior", "min_sal": 35000, "max_sal": 50000, "setup": "Hybrid", "type": "Full-time"},
            {"title": "Area Sales Manager", "level": "Manager", "min_sal": 50000, "max_sal": 70000, "setup": "Hybrid", "type": "Full-time"},
            {"title": "Regional Sales Manager", "level": "Senior", "min_sal": 70000, "max_sal": 90000, "setup": "Hybrid", "type": "Full-time"},
            {"title": "Sales Director", "level": "Director", "min_sal": 100000, "max_sal": 150000, "setup": "Hybrid", "type": "Full-time"},
        ],
        "Marketing": [
            {"title": "Marketing Assistant", "level": "Entry", "min_sal": 20000, "max_sal": 30000, "setup": "Hybrid", "type": "Full-time"},
            {"title": "Brand Specialist", "level": "Junior", "min_sal": 30000, "max_sal": 45000, "setup": "Hybrid", "type": "Full-time"},
            {"title": "Digital Marketing Specialist", "level": "Junior", "min_sal": 35000, "max_sal": 50000, "setup": "Remote", "type": "Full-time"},
            {"title": "Brand Manager", "level": "Manager", "min_sal": 60000, "max_sal": 80000, "setup": "Hybrid", "type": "Full-time"},
            {"title": "Marketing Manager", "level": "Senior", "min_sal": 80000, "max_sal": 100000, "setup": "Hybrid", "type": "Full-time"},
            {"title": "Marketing Director", "level": "Director", "min_sal": 120000, "max_sal": 150000, "setup": "Hybrid", "type": "Full-time"},
        ],
        "Operations": [
            {"title": "Operations Staff", "level": "Entry", "min_sal": 15000, "max_sal": 25000, "setup": "On-site", "type": "Full-time"},
            {"title": "Warehouse Supervisor", "level": "Senior", "min_sal": 30000, "max_sal": 45000, "setup": "On-site", "type": "Full-time"},
            {"title": "Operations Supervisor", "level": "Senior", "min_sal": 35000, "max_sal": 50000, "setup": "On-site", "type": "Full-time"},
            {"title": "Operations Manager", "level": "Manager", "min_sal": 50000, "max_sal": 70000, "setup": "On-site", "type": "Full-time"},
            {"title": "Plant Manager", "level": "Senior", "min_sal": 80000, "max_sal": 110000, "setup": "On-site", "type": "Full-time"},
            {"title": "Operations Director", "level": "Director", "min_sal": 120000, "max_sal": 150000, "setup": "Hybrid", "type": "Full-time"},
        ],
        "Finance": [
            {"title": "Accounting Staff", "level": "Entry", "min_sal": 20000, "max_sal": 30000, "setup": "Hybrid", "type": "Full-time"},
            {"title": "Financial Analyst", "level": "Junior", "min_sal": 30000, "max_sal": 45000, "setup": "Hybrid", "type": "Full-time"},
            {"title": "Senior Accountant", "level": "Senior", "min_sal": 40000, "max_sal": 60000, "setup": "Hybrid", "type": "Full-time"},
            {"title": "Finance Manager", "level": "Manager", "min_sal": 60000, "max_sal": 85000, "setup": "Hybrid", "type": "Full-time"},
            {"title": "CFO", "level": "Director", "min_sal": 120000, "max_sal": 180000, "setup": "Hybrid", "type": "Full-time"},
            {"title": "Finance Director", "level": "Director", "min_sal": 100000, "max_sal": 150000, "setup": "Hybrid", "type": "Full-time"},
        ],
        "Human Resources": [
            {"title": "HR Assistant", "level": "Entry", "min_sal": 18000, "max_sal": 28000, "setup": "Hybrid", "type": "Full-time"},
            {"title": "HR Specialist", "level": "Junior", "min_sal": 25000, "max_sal": 40000, "setup": "Hybrid", "type": "Full-time"},
            {"title": "HR Supervisor", "level": "Senior", "min_sal": 35000, "max_sal": 50000, "setup": "Hybrid", "type": "Full-time"},
            {"title": "HR Manager", "level": "Manager", "min_sal": 50000, "max_sal": 75000, "setup": "Hybrid", "type": "Full-time"},
            {"title": "HR Director", "level": "Director", "min_sal": 90000, "max_sal": 130000, "setup": "Hybrid", "type": "Full-time"},
        ],
        "Supply Chain": [
            {"title": "Logistics Coordinator", "level": "Entry", "min_sal": 20000, "max_sal": 30000, "setup": "Hybrid", "type": "Full-time"},
            {"title": "Supply Chain Analyst", "level": "Junior", "min_sal": 30000, "max_sal": 45000, "setup": "Hybrid", "type": "Full-time"},
            {"title": "Warehouse Manager", "level": "Manager", "min_sal": 50000, "max_sal": 70000, "setup": "On-site", "type": "Full-time"},
            {"title": "Supply Chain Manager", "level": "Senior", "min_sal": 70000, "max_sal": 95000, "setup": "Hybrid", "type": "Full-time"},
            {"title": "Supply Chain Director", "level": "Director", "min_sal": 110000, "max_sal": 150000, "setup": "Hybrid", "type": "Full-time"},
        ],
        "Quality Assurance": [
            {"title": "QA Inspector", "level": "Entry", "min_sal": 18000, "max_sal": 28000, "setup": "On-site", "type": "Full-time"},
            {"title": "QA Specialist", "level": "Junior", "min_sal": 25000, "max_sal": 40000, "setup": "On-site", "type": "Full-time"},
            {"title": "QA Supervisor", "level": "Senior", "min_sal": 35000, "max_sal": 50000, "setup": "On-site", "type": "Full-time"},
            {"title": "QA Manager", "level": "Manager", "min_sal": 50000, "max_sal": 70000, "setup": "Hybrid", "type": "Full-time"},
            {"title": "QA Director", "level": "Director", "min_sal": 80000, "max_sal": 120000, "setup": "Hybrid", "type": "Full-time"},
        ],
        "IT": [
            {"title": "IT Support", "level": "Entry", "min_sal": 20000, "max_sal": 30000, "setup": "Hybrid", "type": "Full-time"},
            {"title": "System Administrator", "level": "Junior", "min_sal": 30000, "max_sal": 45000, "setup": "Hybrid", "type": "Full-time"},
            {"title": "IT Specialist", "level": "Junior", "min_sal": 35000, "max_sal": 50000, "setup": "Remote", "type": "Full-time"},
            {"title": "IT Manager", "level": "Manager", "min_sal": 60000, "max_sal": 85000, "setup": "Hybrid", "type": "Full-time"},
            {"title": "IT Director", "level": "Director", "min_sal": 100000, "max_sal": 140000, "setup": "Hybrid", "type": "Full-time"},
        ],
        "Customer Service": [
            {"title": "Customer Service Rep", "level": "Entry", "min_sal": 18000, "max_sal": 25000, "setup": "Remote", "type": "Full-time"},
            {"title": "Senior CSR", "level": "Junior", "min_sal": 22000, "max_sal": 32000, "setup": "Remote", "type": "Full-time"},
            {"title": "Customer Service Supervisor", "level": "Senior", "min_sal": 35000, "max_sal": 50000, "setup": "Hybrid", "type": "Full-time"},
            {"title": "Customer Service Manager", "level": "Manager", "min_sal": 50000, "max_sal": 70000, "setup": "Hybrid", "type": "Full-time"},
            {"title": "Service Director", "level": "Director", "min_sal": 80000, "max_sal": 120000, "setup": "Hybrid", "type": "Full-time"},
        ],
        "Administration": [
            {"title": "Administrative Assistant", "level": "Entry", "min_sal": 18000, "max_sal": 28000, "setup": "On-site", "type": "Full-time"},
            {"title": "Office Manager", "level": "Junior", "min_sal": 30000, "max_sal": 45000, "setup": "On-site", "type": "Full-time"},
            {"title": "Executive Assistant", "level": "Senior", "min_sal": 35000, "max_sal": 50000, "setup": "Hybrid", "type": "Full-time"},
            {"title": "Admin Manager", "level": "Manager", "min_sal": 50000, "max_sal": 70000, "setup": "Hybrid", "type": "Full-time"},
            {"title": "Admin Director", "level": "Director", "min_sal": 80000, "max_sal": 120000, "setup": "Hybrid", "type": "Full-time"},
        ],
    }
    
    # Create department lookup
    dept_lookup = {dept["department_name"]: dept["department_key"] for dept in departments}
    
    # Generate jobs
    for department, positions in job_positions.items():
        dept_key = dept_lookup.get(department)
        if dept_key:
            for position in positions:
                jobs.append({
                    "job_key": job_key,
                    "job_title": position["title"],
                    "job_level": position["level"],
                    "department_key": dept_key,
                    "work_setup": position["setup"],
                    "work_type": position["type"],
                    "base_salary_min": position["min_sal"],
                    "base_salary_max": position["max_sal"],
                })
                job_key += 1
    
    return jobs

def generate_dim_banks(start_id=1):
    """Generate banks dimension table"""
    banks = [
        {"bank_key": start_id, "bank_name": "BDO", "bank_code": "BDO", "branch_code": None},
        {"bank_key": start_id + 1, "bank_name": "BPI", "bank_code": "BPI", "branch_code": None},
        {"bank_key": start_id + 2, "bank_name": "Metrobank", "bank_code": "MB", "branch_code": None},
        {"bank_key": start_id + 3, "bank_name": "Landbank", "bank_code": "LBP", "branch_code": None},
        {"bank_key": start_id + 4, "bank_name": "PNB", "bank_code": "PNB", "branch_code": None},
        {"bank_key": start_id + 5, "bank_name": "UnionBank", "bank_code": "UB", "branch_code": None},
        {"bank_key": start_id + 6, "bank_name": "China Bank", "bank_code": "CHIB", "branch_code": None},
        {"bank_key": start_id + 7, "bank_name": "Security Bank", "bank_code": "SECB", "branch_code": None},
        {"bank_key": start_id + 8, "bank_name": "RCBC", "bank_code": "RCBC", "branch_code": None},
        {"bank_key": start_id + 9, "bank_name": "PSBank", "bank_code": "PSB", "branch_code": None},
    ]
    
    return banks

def generate_dim_insurance(start_id=1):
    """Generate insurance dimension table"""
    insurance = [
        {"insurance_key": start_id, "provider_name": "PhilHealth", "provider_type": "Health", "coverage_level": "Standard"},
        {"insurance_key": start_id + 1, "provider_name": "Maxicare", "provider_type": "Health", "coverage_level": "Premium"},
        {"insurance_key": start_id + 2, "provider_name": "MediCard", "provider_type": "Health", "coverage_level": "Standard"},
        {"insurance_key": start_id + 3, "provider_name": "Intellicare", "provider_type": "Health", "coverage_level": "Basic"},
        {"insurance_key": start_id + 4, "provider_name": "Sun Life", "provider_type": "Life", "coverage_level": "Premium"},
        {"insurance_key": start_id + 5, "provider_name": "Manulife", "provider_type": "Life", "coverage_level": "Standard"},
        {"insurance_key": start_id + 6, "provider_name": "AXA", "provider_type": "Health", "coverage_level": "Premium"},
        {"insurance_key": start_id + 7, "provider_name": "Pacific Cross", "provider_type": "Health", "coverage_level": "Standard"},
    ]
    
    return insurance

def generate_dim_employees_normalized(num_employees, locations, jobs, banks, insurance, start_id=1):
    """Generate normalized employees dimension table"""
    employees = []
    
    # Create lookups
    location_lookup = {loc["location_key"]: loc for loc in locations}
    job_lookup = {job["job_key"]: job for job in jobs}
    bank_lookup = {bank["bank_key"]: bank for bank in banks}
    insurance_lookup = {ins["insurance_key"]: ins for ins in insurance}
    
    # Department distribution for realistic company structure
    dept_distribution = {
        "Sales": 0.24,
        "Operations": 0.28,
        "Marketing": 0.10,
        "Supply Chain": 0.12,
        "Customer Service": 0.08,
        "Finance": 0.06,
        "Quality Assurance": 0.05,
        "Human Resources": 0.04,
        "IT": 0.02,
        "Administration": 0.01
    }
    
    # Get jobs by department
    jobs_by_dept = {}
    for job in jobs:
        dept_name = next((dept["department_name"] for dept in generate_dim_departments() if dept["department_key"] == job["department_key"]), "Unknown")
        if dept_name not in jobs_by_dept:
            jobs_by_dept[dept_name] = []
        jobs_by_dept[dept_name].append(job)
    
    employee_id = start_id
    for dept_name, percentage in dept_distribution.items():
        dept_count = int(num_employees * percentage)
        dept_jobs = jobs_by_dept.get(dept_name, [])
        
        for i in range(dept_count):
            # Generate personal information
            gender = random.choice(["Male", "Female", "Non-binary"]) if random.random() < 0.05 else random.choice(["Male", "Female"])
            
            if gender == "Male":
                first_name = fake.first_name_male()
                last_name = fake.last_name()
            elif gender == "Female":
                first_name = fake.first_name_female()
                last_name = fake.last_name()
            else:
                first_name = fake.first_name()
                last_name = fake.last_name()
            
            full_name = f"{first_name} {last_name}"
            birth_date = fake.date_between_dates(date_start=date(1970, 1, 1), date_end=date(2005, 12, 31))
            age = (date.today() - birth_date).days // 365
            
            # Government IDs
            tin_number = f"TIN-{random.randint(100000000, 999999999)}"
            sss_number = f"SSC-{random.randint(1000000000, 9999999999)}"
            philhealth_number = f"PH-{random.randint(100000000, 999999999)}"
            pagibig_number = f"PG-{random.randint(1000000000, 9999999999)}"
            
            blood_type = random.choices(["O+", "A+", "B+", "AB+", "O-", "A-", "B-", "AB-"], weights=[0.44, 0.22, 0.11, 0.05, 0.03, 0.09, 0.04, 0.02])[0]
            
            # Randomly select related dimension keys
            location_key = random.choice(locations)["location_key"]
            job_key = random.choice(dept_jobs)["job_key"] if dept_jobs else random.choice(jobs)["job_key"]
            bank_key = random.choice(banks)["bank_key"]
            insurance_key = random.choice(insurance)["insurance_key"]
            
            # Contact information
            phone = fake.phone_number()
            email = fake.email()
            personal_email = fake.free_email()
            
            # Emergency contact
            emergency_contact_name = fake.name()
            emergency_contact_relation = random.choice(["Spouse", "Parent", "Sibling", "Child", "Friend"])
            emergency_contact_phone = fake.phone_number()
            
            # Employment dates and status
            hire_date = fake.date_between_dates(date_start=date(2015, 1, 1), date_end=date.today())
            
            # Realistic turnover rate
            years_employed = (date.today() - hire_date).days / 365.25
            turnover_probability = 1 - (0.85 ** years_employed)
            
            if random.random() < turnover_probability:
                termination_date = fake.date_between_dates(date_start=hire_date, date_end=date.today())
                employment_status = "Terminated"
            else:
                termination_date = None
                employment_status = "Active"
            
            employees.append({
                "employee_key": employee_id,
                "employee_id": f"E{employee_id:05}",
                "full_name": full_name,
                "gender": gender,
                "birth_date": birth_date,
                "age": age,
                "hire_date": hire_date,
                "termination_date": termination_date,
                "employment_status": employment_status,
                "location_key": location_key,
                "job_key": job_key,
                "bank_key": bank_key,
                "insurance_key": insurance_key,
                "tin_number": tin_number,
                "sss_number": sss_number,
                "philhealth_number": philhealth_number,
                "pagibig_number": pagibig_number,
                "blood_type": blood_type,
                "phone": phone,
                "email": email,
                "personal_email": personal_email,
                "emergency_contact_name": emergency_contact_name,
                "emergency_contact_relation": emergency_contact_relation,
                "emergency_contact_phone": emergency_contact_phone,
            })
            
            employee_id += 1
    
    return employees

def generate_fact_employees(employees, jobs, start_id=1):
    """Generate employee fact table with time-varying metrics"""
    employee_facts = []
    fact_key = start_id
    
    # Create job lookup
    job_lookup = {job["job_key"]: job for job in jobs}
    
    for employee in employees:
        if employee["employment_status"] != "Active":
            continue  # Only generate facts for active employees
        
        job = job_lookup.get(employee["job_key"])
        if not job:
            continue
        
        # Base salary from job range
        base_salary = random.randint(job["base_salary_min"], job["base_salary_max"])
        
        # Adjust for work type
        if job["work_type"] == "Part-time":
            base_salary = int(base_salary * 0.6)
        elif job["work_type"] == "Contract":
            base_salary = int(base_salary * 0.9)
        elif job["work_type"] == "Intern":
            base_salary = random.randint(8000, 15000)
        elif job["work_type"] == "Probationary":
            base_salary = int(base_salary * 0.8)
        
        # Performance and development data
        performance_rating = random.choices([5, 4, 3, 2, 1], weights=[0.15, 0.35, 0.30, 0.15, 0.05])[0]
        last_review_date = fake.date_between_dates(date_start=employee["hire_date"], date_end=date.today())
        
        # Training completed
        training_courses = [
            "Sales Fundamentals", "Product Knowledge", "Customer Service Excellence",
            "Leadership Development", "Supply Chain Management", "Quality Control",
            "Digital Marketing", "Financial Planning", "Safety Training", "Compliance"
        ]
        num_trainings = random.randint(0, 5)
        training_completed = ", ".join(random.sample(training_courses, num_trainings)) if num_trainings > 0 else ""
        
        # Skills
        skills = [
            "Communication", "Teamwork", "Problem Solving", "Leadership",
            "Analytical Skills", "Time Management", "Adaptability", "Technical Skills"
        ]
        num_skills = random.randint(3, 7)
        employee_skills = ", ".join(random.sample(skills, num_skills))
        
        # Workforce analytics
        years_of_service = (date.today() - employee["hire_date"]).days // 365
        attendance_rate = random.uniform(0.85, 0.98)
        overtime_hours_monthly = random.randint(0, 20) if job["work_type"] == "Full-time" else 0
        
        # Engagement and satisfaction
        engagement_score = random.randint(1, 10)
        satisfaction_index = random.randint(60, 95)
        
        # Leave balances
        vacation_leave_balance = random.randint(0, 15)
        sick_leave_balance = random.randint(0, 10)
        personal_leave_balance = random.randint(0, 5)
        
        # Bank account
        account_number = f"{random.randint(1000, 9999)}-{random.randint(100000, 999999)}-{random.randint(10, 99)}"
        account_name = employee["full_name"]
        
        employee_facts.append({
            "employee_fact_key": fact_key,
            "employee_key": employee["employee_key"],
            "effective_date": date.today(),
            "monthly_salary": base_salary,
            "performance_rating": performance_rating,
            "last_review_date": last_review_date,
            "training_completed": training_completed,
            "skills": employee_skills,
            "benefit_enrollment_date": employee["hire_date"],
            "years_of_service": years_of_service,
            "attendance_rate": round(attendance_rate, 3),
            "overtime_hours_monthly": overtime_hours_monthly,
            "engagement_score": engagement_score,
            "satisfaction_index": satisfaction_index,
            "vacation_leave_balance": vacation_leave_balance,
            "sick_leave_balance": sick_leave_balance,
            "personal_leave_balance": personal_leave_balance,
            "account_number": account_number,
            "account_name": account_name,
        })
        
        fact_key += 1
    
    return employee_facts

def generate_fact_inventory(products, start_id=1):
    """Generate inventory fact table with normalized location references"""
    inventory = []
    
    # Define warehouse locations (using existing locations)
    warehouse_locations = [
        "NCR - Main Warehouse",
        "Laguna - South Warehouse", 
        "Cebu - Visayas Warehouse",
        "Davao - Mindanao Warehouse"
    ]
    
    inventory_key = start_id
    
    for product in products:
        # Generate inventory records for each warehouse location
        for location in warehouse_locations:
            # Random inventory levels
            cases_on_hand = random.randint(50, 5000)
            
            # Unit cost based on wholesale price with some variation
            base_cost = product["wholesale_price"] * 0.7  # Assume 30% margin
            unit_cost = round(base_cost * random.uniform(0.95, 1.05), 2)
            
            # Generate inventory date (recent snapshot)
            inventory_date = fake.date_between_dates(date_start=date.today() - timedelta(days=30), date_end=date.today())
            
            inventory.append({
                "inventory_key": inventory_key,
                "inventory_date": inventory_date,
                "product_key": product["product_key"],
                "location_key": random.randint(1, 500),  # Random location key from dim_locations
                "cases_on_hand": cases_on_hand,
                "unit_cost": unit_cost,
                "currency": "PHP"
            })
            
            inventory_key += 1
    
    return inventory

def generate_dim_retailers_normalized(num_retailers, locations, start_id=1):
    """Generate normalized retailers dimension table"""
    retailers = []
    
    # Retailer types and distribution
    retailer_types = [
        "Sari-Sari Store", "Convenience Store", "Supermarket", "Drugstore",
        "Wholesale Club", "Specialty Store", "Hypermarket", "Department Store"
    ]
    
    type_distribution = {
        "Sari-Sari Store": 0.45,
        "Convenience Store": 0.20,
        "Supermarket": 0.15,
        "Drugstore": 0.08,
        "Wholesale Club": 0.05,
        "Specialty Store": 0.04,
        "Hypermarket": 0.02,
        "Department Store": 0.01,
    }
    
    # Major chains
    major_chains = [
        "SM Supermarket", "Robinsons Supermarket", "Puregold", "Waltermart",
        "7-Eleven", "FamilyMart", "Ministop", "Alfamart",
        "Watsons", "Mercury Drug", "South Star Drug", "Rural Bank",
        "SM Hypermarket", "S&R Membership Shopping", "Landmark", "Rustans"
    ]
    
    retailer_id = start_id
    for retailer_type, percentage in type_distribution.items():
        type_count = int(num_retailers * percentage)
        
        for i in range(type_count):
            # Select location
            location = random.choice(locations)
            location_key = location["location_key"]
            
            # Generate retailer name
            if retailer_type in ["Supermarket", "Hypermarket", "Convenience Store", "Drugstore"]:
                if random.random() < 0.3:
                    retailer_name = random.choice(major_chains) + f" {location['city']}"
                else:
                    retailer_name = f"{fake.company().split()[0]} {retailer_type}"
            elif retailer_type == "Sari-Sari Store":
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
                "location_key": location_key,
            })
            
            retailer_id += 1
    
    return retailers
