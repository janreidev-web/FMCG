"""
Normalized Dimensional Model Data Generator for FMCG Analytics
Generates data for the normalized schema to reduce redundancy
"""

import random
from datetime import datetime, timedelta, date
from faker import Faker
from helpers import random_date_range
from geography import PH_GEOGRAPHY, pick_ph_location
from config import DAILY_SALES_AMOUNT

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
    """Generate optimized employee fact table with comprehensive metrics"""
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
        
        # Compensation metrics
        annual_bonus = random.randint(0, int(base_salary * 0.3)) if random.random() < 0.7 else None
        total_compensation = base_salary * 12 + (annual_bonus or 0)
        
        # Performance metrics
        performance_rating = random.choices([5, 4, 3, 2, 1], weights=[0.15, 0.35, 0.30, 0.15, 0.05])[0]
        last_review_date = fake.date_between_dates(date_start=employee["hire_date"], date_end=date.today())
        promotion_eligible = performance_rating >= 4 and random.random() < 0.6
        
        # Work metrics
        years_of_service = (date.today() - employee["hire_date"]).days // 365
        attendance_rate = random.uniform(0.85, 0.98)
        overtime_hours_monthly = random.randint(0, 20) if job["work_type"] == "Full-time" else 0
        productivity_score = random.randint(60, 100) if random.random() < 0.8 else None
        
        # Engagement metrics
        engagement_score = random.randint(1, 10)
        satisfaction_index = random.randint(60, 95)
        retention_risk_score = random.randint(1, 10) if satisfaction_index < 75 else None
        
        # Development metrics (quantified instead of strings)
        training_hours_completed = random.randint(0, 120)
        certifications_count = random.randint(0, 5)
        skill_gap_score = random.randint(1, 10) if random.random() < 0.3 else None
        
        # Benefits metrics
        benefit_enrollment_date = employee["hire_date"]
        health_utilization_rate = round(random.uniform(0.1, 0.8), 3) if random.random() < 0.7 else None
        
        # Leave metrics
        vacation_leave_balance = random.randint(0, 15)
        sick_leave_balance = random.randint(0, 10)
        personal_leave_balance = random.randint(0, 5)
        
        # Financial wellness
        salary_grade = (base_salary // 10000) + 1  # Simple salary grade calculation
        cost_center_allocation = round(random.uniform(0.8, 1.2), 3)  # Multiplier for cost allocation
        
        employee_facts.append({
            "employee_fact_key": fact_key,
            "employee_key": employee["employee_key"],
            "effective_date": date.today(),
            
            # Compensation metrics
            "monthly_salary": base_salary,
            "annual_bonus": annual_bonus,
            "total_compensation": total_compensation,
            
            # Performance metrics
            "performance_rating": performance_rating,
            "last_review_date": last_review_date,
            "promotion_eligible": promotion_eligible,
            
            # Work metrics
            "years_of_service": years_of_service,
            "attendance_rate": round(attendance_rate, 3),
            "overtime_hours_monthly": overtime_hours_monthly,
            "productivity_score": productivity_score,
            
            # Engagement metrics
            "engagement_score": engagement_score,
            "satisfaction_index": satisfaction_index,
            "retention_risk_score": retention_risk_score,
            
            # Development metrics
            "training_hours_completed": training_hours_completed,
            "certifications_count": certifications_count,
            "skill_gap_score": skill_gap_score,
            
            # Benefits metrics
            "benefit_enrollment_date": benefit_enrollment_date,
            "health_utilization_rate": health_utilization_rate,
            
            # Leave metrics
            "vacation_leave_balance": vacation_leave_balance,
            "sick_leave_balance": sick_leave_balance,
            "personal_leave_balance": personal_leave_balance,
            
            # Financial wellness
            "salary_grade": salary_grade,
            "cost_center_allocation": cost_center_allocation,
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

def generate_dim_products(start_id=1):
    """Generate products dimension table"""
    products = []
    
    # FMCG product categories and data
    product_data = [
        # Food Products
        {"category": "Food", "subcategory": "Rice", "brand": "Datu", "name": "Premium Jasmine Rice", "wholesale": 45.50, "retail": 52.00},
        {"category": "Food", "subcategory": "Rice", "brand": "Villar", "name": "Regular Milled Rice", "wholesale": 38.00, "retail": 43.00},
        {"category": "Food", "subcategory": "Noodles", "brand": "Lucky Me", "name": "Instant Noodles Original", "wholesale": 8.50, "retail": 10.00},
        {"category": "Food", "subcategory": "Noodles", "brand": "Nissin", "name": "Ramen Noodles", "wholesale": 12.00, "retail": 14.50},
        {"category": "Food", "subcategory": "Canned Goods", "brand": "Mega", "name": "Sardines in Tomato Sauce", "wholesale": 15.50, "retail": 18.00},
        {"category": "Food", "subcategory": "Canned Goods", "brand": "Century", "name": "Tuna Flakes in Oil", "wholesale": 28.00, "retail": 32.00},
        {"category": "Food", "subcategory": "Biscuits", "brand": "Monde", "name": "Chocolate Sandwich Cookies", "wholesale": 22.50, "retail": 26.00},
        {"category": "Food", "subcategory": "Biscuits", "brand": "Rebisco", "name": "Cream Sandwich", "wholesale": 18.00, "retail": 21.00},
        {"category": "Food", "subcategory": "Coffee", "brand": "Nescafe", "name": "3-in-1 Coffee", "wholesale": 6.50, "retail": 8.00},
        {"category": "Food", "subcategory": "Coffee", "brand": "Great Taste", "name": "White Coffee", "wholesale": 7.00, "retail": 8.50},
        
        # Beverages
        {"category": "Beverages", "subcategory": "Soft Drinks", "brand": "Coca-Cola", "name": "Coca-Cola 1.5L", "wholesale": 45.00, "retail": 52.00},
        {"category": "Beverages", "subcategory": "Soft Drinks", "brand": "Pepsi", "name": "Pepsi 1.5L", "wholesale": 44.00, "retail": 51.00},
        {"category": "Beverages", "subcategory": "Juice", "brand": "Zesto", "name": "Orange Juice 1L", "wholesale": 28.50, "retail": 33.00},
        {"category": "Beverages", "subcategory": "Juice", "brand": "Del Monte", "name": "Pineapple Juice 1L", "wholesale": 35.00, "retail": 40.00},
        {"category": "Beverages", "subcategory": "Water", "brand": "Nestle", "name": "Pure Life Water 500ml", "wholesale": 8.00, "retail": 10.00},
        {"category": "Beverages", "subcategory": "Water", "brand": "Wilkins", "name": "Distilled Water 500ml", "wholesale": 9.00, "retail": 11.00},
        
        # Personal Care
        {"category": "Personal Care", "subcategory": "Soap", "brand": "Safeguard", "name": "Antibacterial Soap", "wholesale": 18.50, "retail": 22.00},
        {"category": "Personal Care", "subcategory": "Soap", "brand": "Dove", "name": "Beauty Bath Soap", "wholesale": 25.00, "retail": 29.00},
        {"category": "Personal Care", "subcategory": "Shampoo", "brand": "Head & Shoulders", "name": "Anti-Dandruff Shampoo", "wholesale": 32.00, "retail": 37.00},
        {"category": "Personal Care", "subcategory": "Shampoo", "brand": "Pantene", "name": "Smooth & Silky Shampoo", "wholesale": 35.00, "retail": 40.00},
        {"category": "Personal Care", "subcategory": "Toothpaste", "brand": "Colgate", "name": "Total Toothpaste", "wholesale": 45.00, "retail": 52.00},
        {"category": "Personal Care", "subcategory": "Toothpaste", "brand": "Close Up", "name": "Red Hot Toothpaste", "wholesale": 42.00, "retail": 48.00},
        
        # Household Care
        {"category": "Household", "subcategory": "Detergent", "brand": "Tide", "name": "Laundry Detergent Powder", "wholesale": 28.00, "retail": 32.00},
        {"category": "Household", "subcategory": "Detergent", "brand": "Surf", "name": "Laundry Detergent Powder", "wholesale": 25.00, "retail": 29.00},
        {"category": "Household", "subcategory": "Fabric Softener", "brand": "Downy", "name": "Fabric Softener", "wholesale": 22.00, "retail": 26.00},
        {"category": "Household", "subcategory": "Fabric Softener", "brand": "Comfort", "name": "Fabric Softener", "wholesale": 20.00, "retail": 24.00},
        {"category": "Household", "subcategory": "Disinfectant", "brand": "Domex", "name": "Bleach Disinfectant", "wholesale": 35.00, "retail": 40.00},
        {"category": "Household", "subcategory": "Disinfectant", "brand": "Zonrox", "name": "Color Safe Bleach", "wholesale": 32.00, "retail": 37.00},
    ]
    
    for i, product in enumerate(product_data):
        products.append({
            "product_key": start_id + i,
            "product_id": f"P{start_id + i:03}",
            "product_name": product["name"],
            "category": product["category"],
            "subcategory": product["subcategory"],
            "brand": product["brand"],
            "wholesale_price": product["wholesale"],
            "retail_price": product["retail"],
            "status": "Active",
            "created_date": fake.date_between_dates(date_start=date(2020, 1, 1), date_end=date.today())
        })
    
    return products

def generate_dim_campaigns(start_id=1):
    """Generate campaigns dimension table"""
    campaigns = []
    
    campaign_data = [
        # Historical campaigns (2015-2022)
        {"name": "Launch Campaign 2015", "type": "Product Launch", "start": "2015-01-01", "end": "2015-03-31", "budget": 3000000},
        {"name": "Holiday Season 2015", "type": "Holiday", "start": "2015-11-01", "end": "2015-12-31", "budget": 5000000},
        {"name": "Summer Promotion 2016", "type": "Seasonal", "start": "2016-06-01", "end": "2016-08-31", "budget": 4000000},
        {"name": "Brand Building 2016", "type": "Brand Building", "start": "2016-02-01", "end": "2016-05-31", "budget": 3500000},
        {"name": "Year End Sale 2017", "type": "Seasonal", "start": "2017-11-15", "end": "2017-12-31", "budget": 6000000},
        {"name": "Spring Campaign 2017", "type": "Seasonal", "start": "2017-03-01", "end": "2017-05-31", "budget": 4500000},
        {"name": "Back to School 2018", "type": "Seasonal", "start": "2018-05-15", "end": "2018-06-30", "budget": 3000000},
        {"name": "Festival Season 2018", "type": "Holiday", "start": "2018-10-01", "end": "2018-12-31", "budget": 7000000},
        {"name": "Market Expansion 2019", "type": "Market Expansion", "start": "2019-01-01", "end": "2019-06-30", "budget": 8000000},
        {"name": "Digital Push 2019", "type": "Brand Building", "start": "2019-07-01", "end": "2019-12-31", "budget": 5500000},
        {"name": "Growth Campaign 2020", "type": "Market Expansion", "start": "2020-01-01", "end": "2020-12-31", "budget": 10000000},
        {"name": "Recovery Campaign 2021", "type": "Brand Building", "start": "2021-01-15", "end": "2021-12-31", "budget": 8500000},
        {"name": "Anniversary Sale 2022", "type": "Seasonal", "start": "2022-09-01", "end": "2022-11-30", "budget": 6500000},
        
        # Recent campaigns (2023-2025)
        {"name": "Christmas Promotion 2023", "type": "Holiday", "start": "2023-11-01", "end": "2023-12-31", "budget": 8000000},
        {"name": "Back to School 2023", "type": "Seasonal", "start": "2023-06-01", "end": "2023-08-31", "budget": 4000000},
        {"name": "Loyalty Program 2023", "type": "Retention", "start": "2023-09-01", "end": "2023-12-31", "budget": 2000000},
        {"name": "Regional Expansion 2023", "type": "Market Expansion", "start": "2023-10-01", "end": "2024-03-31", "budget": 6000000},
        {"name": "Flash Sale 2024", "type": "Promotional", "start": "2024-01-01", "end": "2024-01-07", "budget": 1500000},
        {"name": "New Product Launch 2024", "type": "Product Launch", "start": "2024-01-15", "end": "2024-03-15", "budget": 3000000},
        {"name": "Brand Awareness 2024", "type": "Brand Building", "start": "2024-02-01", "end": "2024-04-30", "budget": 2500000},
        {"name": "Summer Sale 2024", "type": "Seasonal", "start": "2024-03-01", "end": "2024-05-31", "budget": 5000000},
        {"name": "Spring Festival 2025", "type": "Holiday", "start": "2025-01-20", "end": "2025-02-28", "budget": 4500000},
        {"name": "Summer Campaign 2025", "type": "Seasonal", "start": "2025-06-01", "end": "2025-08-31", "budget": 5500000},
        {"name": "Back to School 2025", "type": "Seasonal", "start": "2025-05-15", "end": "2025-06-30", "budget": 3500000},
        {"name": "Year End Sale 2025", "type": "Seasonal", "start": "2025-11-01", "end": "2025-12-31", "budget": 7000000},
    ]
    
    for i, campaign in enumerate(campaign_data):
        campaigns.append({
            "campaign_key": start_id + i,
            "campaign_id": f"C{start_id + i:03}",
            "campaign_name": campaign["name"],
            "campaign_type": campaign["type"],
            "start_date": date.fromisoformat(campaign["start"]),
            "end_date": date.fromisoformat(campaign["end"]),
            "budget": campaign["budget"],
            "currency": "PHP"
        })
    
    return campaigns

def generate_fact_sales(employees, products, retailers, campaigns, target_amount, start_date=None, end_date=None, start_id=1):
    """Generate sales fact table with optimized batch processing"""
    sales = []
    sale_key = start_id
    
    if start_date is None:
        start_date = date.today()
    if end_date is None:
        end_date = start_date
    
    # Filter active employees, products, and retailers
    active_employees = [e for e in employees if e.get('employment_status') == 'Active']
    active_products = [p for p in products if p.get('status') == 'Active']
    
    # Calculate total days and daily target
    total_days = (end_date - start_date).days + 1
    daily_target = target_amount / total_days
    
    current_amount = 0
    current_date = start_date
    
    print(f"Generating sales from {start_date} to {end_date} ({total_days} days)")
    print(f"Daily target: ₱{daily_target:,.0f}")
    
    while current_date <= end_date and current_amount < target_amount:
        # Calculate daily sales target with some variation
        daily_variation = random.uniform(0.7, 1.3)  # ±30% variation
        today_target = min(daily_target * daily_variation, target_amount - current_amount)
        
        # Calculate number of sales for today (batch processing)
        avg_sale_amount = 15000  # Average sale amount based on product prices
        num_sales_today = max(1, int(today_target / avg_sale_amount))
        
        # Generate batch of sales for today
        for i in range(num_sales_today):
            if current_amount >= target_amount:
                break
                
            # Random selection
            employee = random.choice(active_employees)
            product = random.choice(active_products)
            retailer = random.choice(retailers)
            campaign = random.choice(campaigns) if random.random() < 0.3 else None
            
            # Generate sale quantity and calculate amounts
            case_quantity = random.randint(1, 10)
            unit_price = product["retail_price"]
            discount_percent = random.uniform(0, 0.15) if campaign else 0
            tax_rate = 0.12  # 12% VAT
            
            subtotal = case_quantity * unit_price
            discount_amount = subtotal * discount_percent
            taxable_amount = subtotal - discount_amount
            tax_amount = taxable_amount * tax_rate
            total_amount = taxable_amount + tax_amount
            
            # Commission calculation
            commission_rate = 0.05 if campaign else 0.03
            commission_amount = total_amount * commission_rate
            
            # Payment and delivery
            payment_method = random.choice(["Cash", "Credit Card", "Bank Transfer", "Mobile Payment"])
            payment_status = "Paid"
            delivery_status = random.choice(["Pending", "In Transit", "Delivered"])
            
            expected_delivery = current_date + timedelta(days=random.randint(1, 5))
            actual_delivery = expected_delivery if delivery_status == "Delivered" else None
            
            sales.append({
                "sale_key": sale_key,
                "sale_date": current_date,
                "product_key": product["product_key"],
                "employee_key": employee["employee_key"],
                "retailer_key": retailer["retailer_key"],
                "campaign_key": campaign["campaign_key"] if campaign else None,
                "case_quantity": case_quantity,
                "unit_price": unit_price,
                "discount_percent": discount_percent,
                "tax_rate": tax_rate,
                "total_amount": total_amount,
                "commission_amount": commission_amount,
                "currency": "PHP",
                "payment_method": payment_method,
                "payment_status": payment_status,
                "delivery_status": delivery_status,
                "expected_delivery_date": expected_delivery,
                "actual_delivery_date": actual_delivery,
            })
            
            current_amount += total_amount
            sale_key += 1
        
        # Progress reporting
        if len(sales) % 10000 == 0:
            progress = (current_amount / target_amount) * 100
            print(f"Progress: {progress:.1f}% - Generated {len(sales):,} sales - ₱{current_amount:,.0f}")
        
        current_date += timedelta(days=1)
    
    print(f"Completed: Generated {len(sales):,} sales totaling ₱{current_amount:,.0f}")
    return sales

def generate_fact_operating_costs(target_amount, start_date=None, end_date=None, start_id=1):
    """Generate operating costs fact table"""
    costs = []
    cost_key = start_id
    
    if start_date is None:
        start_date = date.today() - timedelta(days=365)
    if end_date is None:
        end_date = date.today()
    
    cost_categories = [
        {"category": "Salaries & Wages", "types": ["Base Salary", "Overtime Pay", "Bonuses"]},
        {"category": "Rent & Utilities", "types": ["Office Rent", "Warehouse Rent", "Electricity", "Water", "Internet"]},
        {"category": "Marketing & Sales", "types": ["Advertising", "Promotions", "Sales Commissions"]},
        {"category": "Operations", "types": ["Logistics", "Distribution", "Maintenance"]},
        {"category": "Administrative", "types": ["Office Supplies", "Insurance", "Legal Fees"]},
    ]
    
    current_amount = 0
    daily_target = target_amount / 365
    
    for category_data in cost_categories:
        category = category_data["category"]
        for cost_type in category_data["types"]:
            # Generate monthly costs for this type
            monthly_amount = daily_target * 30 * random.uniform(0.1, 0.3)
            
            current_date = start_date
            while current_date <= end_date:
                costs.append({
                    "cost_key": cost_key,
                    "cost_date": current_date,
                    "category": category,
                    "cost_type": cost_type,
                    "amount": monthly_amount / 30,  # Daily amount
                    "currency": "PHP"
                })
                
                current_amount += monthly_amount / 30
                cost_key += 1
                current_date += timedelta(days=1)
    
    return costs

def generate_fact_marketing_costs(campaigns, target_amount, start_date=None, end_date=None, start_id=1):
    """Generate marketing costs fact table"""
    costs = []
    cost_key = start_id
    
    if start_date is None:
        start_date = date.today() - timedelta(days=365)
    if end_date is None:
        end_date = date.today()
    
    cost_categories = [
        "Digital Advertising", "Print Media", "TV/Radio", "Events", "Sponsorships", 
        "Social Media", "Content Creation", "Market Research", "Brand Materials"
    ]
    
    # Ensure we have campaigns to work with
    if not campaigns:
        # Generate some default marketing costs even without campaigns
        daily_target = target_amount / ((end_date - start_date).days + 1) if (end_date - start_date).days > 0 else target_amount
        
        current_date = start_date
        while current_date <= end_date:
            for category in cost_categories:
                costs.append({
                    "marketing_cost_key": cost_key,
                    "cost_date": current_date,
                    "campaign_key": None,
                    "campaign_id": None,
                    "campaign_type": "General",
                    "cost_category": category,
                    "amount": daily_target / len(cost_categories) * random.uniform(0.8, 1.2),
                    "currency": "PHP"
                })
                cost_key += 1
            current_date += timedelta(days=1)
    else:
        for campaign in campaigns:
            # Generate costs for each campaign
            campaign_budget = campaign["budget"]
            cost_per_category = campaign_budget / len(cost_categories)
            
            # Use campaign date range but ensure it overlaps with requested range
            campaign_start = max(start_date, campaign["start_date"])
            campaign_end = min(end_date, campaign["end_date"])
            
            # Only generate costs if there's an overlap
            if campaign_start <= campaign_end:
                for category in cost_categories:
                    current_date = campaign_start
                    
                    while current_date <= campaign_end:
                        daily_cost = cost_per_category / ((campaign["end_date"] - campaign["start_date"]).days + 1)
                        
                        costs.append({
                            "marketing_cost_key": cost_key,
                            "cost_date": current_date,
                            "campaign_key": campaign["campaign_key"],
                            "campaign_id": campaign["campaign_id"],
                            "campaign_type": campaign["campaign_type"],
                            "cost_category": category,
                            "amount": daily_cost * random.uniform(0.8, 1.2),  # Add some variation
                            "currency": "PHP"
                        })
                        
                        cost_key += 1
                        current_date += timedelta(days=1)
    
    return costs
