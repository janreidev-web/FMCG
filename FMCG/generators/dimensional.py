"""
Normalized Dimensional Model Data Generator for FMCG Analytics
Generates data for the normalized schema to reduce redundancy
"""

import random
from datetime import datetime, timedelta, date
from faker import Faker
import pandas as pd
import hashlib

# Handle both relative and absolute imports
try:
    from ..helpers import random_date_range
    from ..geography import PH_GEOGRAPHY, pick_ph_location
    from ..config import DAILY_SALES_AMOUNT
    from ..id_generation import generate_unique_id, generate_readable_id, generate_unique_sale_key
except ImportError:
    # Fallback to absolute imports when running as script
    from helpers import random_date_range
    from geography import PH_GEOGRAPHY, pick_ph_location
    from config import DAILY_SALES_AMOUNT
    from id_generation import generate_unique_id, generate_readable_id, generate_unique_sale_key

fake = Faker()

def generate_unique_wage_key(employee_id, effective_date, sequence_num):
    """Generate unique wage key using employee + date + sequence"""
    # Use hash-based approach for large employee ids
    date_str = effective_date.strftime("%Y%m%d")
    combined_str = f"{employee_id}{date_str}{sequence_num}"
    # Create hash to ensure reasonable length and fit within BigQuery limits
    unique_hash = hashlib.md5(combined_str.encode()).hexdigest()[:12]
    unique_id = int(unique_hash, 16)
    
    # Ensure it fits in 19 digits (BigQuery INTEGER limit)
    max_safe_int = 9223372036854775807
    if unique_id > max_safe_int:
        unique_id = unique_id % max_safe_int
    
    return unique_id

def generate_unique_cost_key(cost_date, category_code, sequence_num):
    """Generate unique cost key using date + category + sequence"""
    # Format: YYYYMMDD + category_code (2 digits) + sequence (6 digits)
    date_str = cost_date.strftime("%Y%m%d")
    category_map = {"Salaries & Wages": 10, "Rent & Utilities": 20, "Marketing & Sales": 30, 
                    "Operations": 40, "Administrative": 50}
    cat_code = category_map.get(category_code, 99)
    return int(f"{date_str}{cat_code:02d}{sequence_num:06d}")

def generate_unique_inventory_key(product_key, location_key, inventory_date, sequence_num):
    """Generate unique inventory key using product + location + date + sequence"""
    # Use hash-based approach for large keys
    date_str = inventory_date.strftime("%Y%m%d")
    combined_str = f"{product_key}{location_key}{date_str}{sequence_num}"
    # Create hash to ensure reasonable length and fit within BigQuery limits
    unique_hash = hashlib.md5(combined_str.encode()).hexdigest()[:12]
    unique_id = int(unique_hash, 16)
    
    # Ensure it fits in 19 digits (BigQuery INTEGER limit)
    max_safe_int = 9223372036854775807
    if unique_id > max_safe_int:
        unique_id = unique_id % max_safe_int
    
    return f"INV{unique_id:015d}"  # Return as string with INV prefix

def generate_unique_marketing_cost_key(campaign_key, cost_date, category_code, sequence_num):
    """Generate unique marketing cost key using campaign + date + category + sequence"""
    # Use hash-based approach for large campaign keys
    date_str = cost_date.strftime("%Y%m%d")
    category_map = {"Digital Advertising": 10, "Print Media": 20, "TV/Radio": 30, "Events": 40,
                    "Sponsorships": 50, "Social Media": 60, "Content Creation": 70, 
                    "Market Research": 80, "Brand Materials": 90}
    cat_code = category_map.get(category_code, 99)
    camp_key = campaign_key if campaign_key else 0
    combined_str = f"{camp_key}{date_str}{cat_code}{sequence_num}"
    # Create hash to ensure reasonable length and fit within BigQuery limits
    unique_hash = hashlib.md5(combined_str.encode()).hexdigest()[:12]
    unique_id = int(unique_hash, 16)
    
    # Ensure it fits in 19 digits (BigQuery INTEGER limit)
    max_safe_int = 9223372036854775807
    if unique_id > max_safe_int:
        unique_id = unique_id % max_safe_int
    
    return unique_id

def generate_unique_employee_fact_key(employee_id, effective_date, sequence_num):
    """Generate unique employee fact key using employee + date + sequence"""
    # Use hash-based approach for large employee ids
    date_str = effective_date.strftime("%Y%m%d")
    combined_str = f"{employee_id}{date_str}{sequence_num}"
    # Create hash to ensure reasonable length and fit within BigQuery limits
    unique_hash = hashlib.md5(combined_str.encode()).hexdigest()[:12]
    unique_id = int(unique_hash, 16)
    
    # Ensure it fits in 19 digits (BigQuery INTEGER limit)
    max_19_digit = 9999999999999999999
    unique_id = unique_id % max_19_digit
    
    return f"EF{unique_id:016d}"  # Return as string with EF prefix

def generate_dim_locations(num_locations=500, start_id=1):
    """Generate locations dimension table with normalized address data"""
    locations = []
    location_set = set()  # To avoid duplicates
    
    for i in range(num_locations):
        # Generate unique location combinations
        max_attempts = 50
        for _ in range(max_attempts):
            region, province, city = pick_ph_location()
            postal_code = fake.postcode()
            
            # Create unique key
            location_key = f"{city}|{province}|{region}"
            
            if location_key not in location_set:
                location_set.add(location_key)
                break
        else:
            # If we can't find a unique location, use a generic one
            region, province, city = pick_ph_location()
            postal_code = fake.postcode()
        
        locations.append({
            "location_id": generate_readable_id("LOC", "location", 4),
            "city": city,
            "province": province,
            "region": region,
            "country": "Philippines"
        })
    
    return locations

def generate_dim_departments(start_id=1):
    """Generate departments dimension table"""
    departments = [
        {"department_id": generate_readable_id("DEPT", "department", 3), "department_name": "Sales", "department_code": "SLS"},
        {"department_id": generate_readable_id("DEPT", "department", 3), "department_name": "Marketing", "department_code": "MKT"},
        {"department_id": generate_readable_id("DEPT", "department", 3), "department_name": "Operations", "department_code": "OPS"},
        {"department_id": generate_readable_id("DEPT", "department", 3), "department_name": "Finance", "department_code": "FIN"},
        {"department_id": generate_readable_id("DEPT", "department", 3), "department_name": "Human Resources", "department_code": "HR"},
        {"department_id": generate_readable_id("DEPT", "department", 3), "department_name": "Supply Chain", "department_code": "SCH"},
        {"department_id": generate_readable_id("DEPT", "department", 3), "department_name": "Quality Assurance", "department_code": "QA"},
        {"department_id": generate_readable_id("DEPT", "department", 3), "department_name": "IT", "department_code": "IT"},
        {"department_id": generate_readable_id("DEPT", "department", 3), "department_name": "Customer Service", "department_code": "CS"},
        {"department_id": generate_readable_id("DEPT", "department", 3), "department_name": "Administration", "department_code": "ADM"},
    ]
    
    return departments

def generate_dim_jobs(departments, start_id=1):
    """Generate jobs dimension table with optimized salary ranges for realistic wage/revenue ratio"""
    jobs = []

    # Optimized salary ranges for Philippine FMCG industry standards
    # Target: ₱8B revenue over 10 years × 20% = ₱1.6B total wages ÷ 350 employees = ₱228K avg annual salary per employee
    salary_ranges = {
        "Entry": (18000, 25000),     # ₱216K-₱300K annually - Fresh grads, assistants
        "Junior": (25000, 40000),     # ₱300K-₱480K annually - 1-3 yrs experience
        "Senior": (40000, 70000),     # ₱480K-₱840K annually - Specialists / leads
        "Manager": (70000, 120000),   # ₱840K-₱1.44M annually - People + budget ownership
        "Director": (120000, 200000)  # ₱1.44M-₱2.4M annually - Exec / VP / C-level
    }

    # Job positions by department
    department_jobs = {
        "Sales": [
            {"title": "Sales Representative", "level": "Entry", "setup": "Field", "type": "Full-time"},
            {"title": "Junior Sales Executive", "level": "Junior", "setup": "Hybrid", "type": "Full-time"},
            {"title": "Senior Sales Executive", "level": "Senior", "setup": "Hybrid", "type": "Full-time"},
            {"title": "Sales Manager", "level": "Manager", "setup": "Hybrid", "type": "Full-time"},
            {"title": "Regional Sales Director", "level": "Director", "setup": "Remote", "type": "Full-time"},
        ],
        "Operations": [
            {"title": "Operations Assistant", "level": "Entry", "setup": "On-site", "type": "Full-time"},
            {"title": "Junior Operations Analyst", "level": "Junior", "setup": "Hybrid", "type": "Full-time"},
            {"title": "Senior Operations Specialist", "level": "Senior", "setup": "Hybrid", "type": "Full-time"},
            {"title": "Operations Manager", "level": "Manager", "setup": "On-site", "type": "Full-time"},
            {"title": "VP of Operations", "level": "Director", "setup": "Hybrid", "type": "Full-time"},
        ],
        "Marketing": [
            {"title": "Marketing Assistant", "level": "Entry", "setup": "Hybrid", "type": "Full-time"},
            {"title": "Junior Marketing Specialist", "level": "Junior", "setup": "Hybrid", "type": "Full-time"},
            {"title": "Senior Marketing Manager", "level": "Senior", "setup": "Hybrid", "type": "Full-time"},
            {"title": "Marketing Director", "level": "Manager", "setup": "Hybrid", "type": "Full-time"},
            {"title": "Chief Marketing Officer (CMO)", "level": "Director", "setup": "Hybrid", "type": "Full-time"},
        ],
        "Supply Chain": [
            {"title": "Supply Chain Coordinator", "level": "Entry", "setup": "On-site", "type": "Full-time"},
            {"title": "Junior Logistics Analyst", "level": "Junior", "setup": "Hybrid", "type": "Full-time"},
            {"title": "Senior Supply Chain Planner", "level": "Senior", "setup": "Hybrid", "type": "Full-time"},
            {"title": "Supply Chain Manager", "level": "Manager", "setup": "Hybrid", "type": "Full-time"},
            {"title": "Director of Supply Chain", "level": "Director", "setup": "Hybrid", "type": "Full-time"},
        ],
        "Customer Service": [
            {"title": "Customer Service Representative", "level": "Entry", "setup": "On-site", "type": "Full-time"},
            {"title": "Senior Customer Service Rep", "level": "Junior", "setup": "On-site", "type": "Full-time"},
            {"title": "Customer Service Supervisor", "level": "Senior", "setup": "On-site", "type": "Full-time"},
            {"title": "Customer Service Manager", "level": "Manager", "setup": "Hybrid", "type": "Full-time"},
        ],
        "Finance": [
            {"title": "Finance Assistant", "level": "Entry", "setup": "Hybrid", "type": "Full-time"},
            {"title": "Junior Accountant", "level": "Junior", "setup": "Hybrid", "type": "Full-time"},
            {"title": "Financial Analyst", "level": "Senior", "setup": "Hybrid", "type": "Full-time"},
            {"title": "Finance Manager", "level": "Manager", "setup": "Hybrid", "type": "Full-time"},
            {"title": "Chief Financial Officer (CFO)", "level": "Director", "setup": "Hybrid", "type": "Full-time"},
        ],
        "Human Resources": [
            {"title": "HR Assistant", "level": "Entry", "setup": "Hybrid", "type": "Full-time"},
            {"title": "Junior HR Specialist", "level": "Junior", "setup": "Hybrid", "type": "Full-time"},
            {"title": "Senior HR Business Partner", "level": "Senior", "setup": "Hybrid", "type": "Full-time"},
            {"title": "HR Manager", "level": "Manager", "setup": "Hybrid", "type": "Full-time"},
        ],
        "IT": [
            {"title": "IT Support Staff", "level": "Entry", "setup": "On-site", "type": "Full-time"},
            {"title": "Junior Software Developer", "level": "Junior", "setup": "Hybrid", "type": "Full-time"},
            {"title": "Senior Systems Administrator", "level": "Senior", "setup": "Hybrid", "type": "Full-time"},
            {"title": "IT Manager", "level": "Manager", "setup": "Hybrid", "type": "Full-time"},
        ],
        "Research & Development": [
            {"title": "Research Assistant", "level": "Entry", "setup": "Lab", "type": "Full-time"},
            {"title": "Junior Scientist", "level": "Junior", "setup": "Lab", "type": "Full-time"},
            {"title": "Senior R&D Engineer", "level": "Senior", "setup": "Lab", "type": "Full-time"},
            {"title": "R&D Director", "level": "Director", "setup": "Hybrid", "type": "Full-time"},
        ],
        "Quality Assurance": [
            {"title": "QA Tester", "level": "Entry", "setup": "On-site", "type": "Full-time"},
            {"title": "Junior QA Engineer", "level": "Junior", "setup": "Hybrid", "type": "Full-time"},
            {"title": "Senior QA Lead", "level": "Senior", "setup": "Hybrid", "type": "Full-time"},
            {"title": "QA Manager", "level": "Manager", "setup": "Hybrid", "type": "Full-time"},
        ],
        "Legal": [
            {"title": "Legal Assistant", "level": "Entry", "setup": "Office", "type": "Full-time"},
            {"title": "Junior Legal Counsel", "level": "Junior", "setup": "Office", "type": "Full-time"},
            {"title": "Senior Legal Counsel", "level": "Senior", "setup": "Office", "type": "Full-time"},
            {"title": "Chief Legal Officer", "level": "Director", "setup": "Hybrid", "type": "Full-time"},
        ],
        "Administration": [
            {"title": "Administrative Assistant", "level": "Entry", "setup": "Office", "type": "Full-time"},
            {"title": "Executive Assistant", "level": "Junior", "setup": "Office", "type": "Full-time"},
            {"title": "Office Manager", "level": "Senior", "setup": "Office", "type": "Full-time"},
        ]
    }

    # Create department lookup
    dept_lookup = {dept["department_name"]: dept["department_id"] for dept in departments}

    # Generate jobs
    for department, positions in department_jobs.items():
        dept_id = dept_lookup.get(department)
        if dept_id:
            for position in positions:
                level = position["level"]
                min_sal, max_sal = salary_ranges[level]
                
                jobs.append({
                    "job_id": generate_readable_id("JOB", "job", 5),
                    "job_title": position["title"],
                    "job_level": level,
                    "department_id": dept_id,
                    "work_setup": position["setup"],
                    "work_type": position["type"],
                    "base_salary_min": min_sal,
                    "base_salary_max": max_sal,
                })

    return jobs

def generate_dim_banks(start_id=1):
    """Generate banks dimension table"""
    banks = [
        {"bank_id": generate_readable_id("BANK", "bank", 2), "bank_name": "BDO", "bank_code": "BDO", "branch_code": "001"},
        {"bank_id": generate_readable_id("BANK", "bank", 2), "bank_name": "BPI", "bank_code": "BPI", "branch_code": "002"},
        {"bank_id": generate_readable_id("BANK", "bank", 2), "bank_name": "Metrobank", "bank_code": "MB", "branch_code": "003"},
        {"bank_id": generate_readable_id("BANK", "bank", 2), "bank_name": "Landbank", "bank_code": "LBP", "branch_code": "004"},
        {"bank_id": generate_readable_id("BANK", "bank", 2), "bank_name": "PNB", "bank_code": "PNB", "branch_code": "005"},
        {"bank_id": generate_readable_id("BANK", "bank", 2), "bank_name": "UnionBank", "bank_code": "UB", "branch_code": "006"},
        {"bank_id": generate_readable_id("BANK", "bank", 2), "bank_name": "China Bank", "bank_code": "CHIB", "branch_code": "007"},
        {"bank_id": generate_readable_id("BANK", "bank", 2), "bank_name": "Security Bank", "bank_code": "SECB", "branch_code": "008"},
        {"bank_id": generate_readable_id("BANK", "bank", 2), "bank_name": "RCBC", "bank_code": "RCBC", "branch_code": "009"},
        {"bank_id": generate_readable_id("BANK", "bank", 2), "bank_name": "PSBank", "bank_code": "PSB", "branch_code": "010"},
    ]
    
    return banks

def generate_dim_insurance(start_id=1):
    """Generate insurance dimension table"""
    insurance = [
        {"insurance_id": generate_readable_id("INS", "insurance", 2), "provider_name": "PhilHealth", "provider_type": "Health", "coverage_level": "Standard"},
        {"insurance_id": generate_readable_id("INS", "insurance", 2), "provider_name": "Maxicare", "provider_type": "Health", "coverage_level": "Premium"},
        {"insurance_id": generate_readable_id("INS", "insurance", 2), "provider_name": "MediCard", "provider_type": "Health", "coverage_level": "Standard"},
        {"insurance_id": generate_readable_id("INS", "insurance", 2), "provider_name": "Intellicare", "provider_type": "Health", "coverage_level": "Basic"},
        {"insurance_id": generate_readable_id("INS", "insurance", 2), "provider_name": "Sun Life", "provider_type": "Life", "coverage_level": "Premium"},
        {"insurance_id": generate_readable_id("INS", "insurance", 2), "provider_name": "Manulife", "provider_type": "Life", "coverage_level": "Standard"},
        {"insurance_id": generate_readable_id("INS", "insurance", 2), "provider_name": "AXA", "provider_type": "Health", "coverage_level": "Premium"},
        {"insurance_id": generate_readable_id("INS", "insurance", 2), "provider_name": "Pacific Cross", "provider_type": "Health", "coverage_level": "Standard"},
    ]
    
    return insurance

def generate_dim_employees_normalized(num_employees, locations, jobs, banks, insurance, departments=None, start_id=1):
    """Generate simplified employees dimension table with job-based compensation"""
    employees = []
    
    # Validate inputs
    if not locations or not jobs or not banks or not insurance:
        raise ValueError("All dimension data (locations, jobs, banks, insurance) must be provided")
    
    # Create lookups for robust foreign key relationships
    location_lookup = {loc["location_id"]: loc for loc in locations}
    job_lookup = {job["job_id"]: job for job in jobs}
    
    # Create department lookup from passed departments or generate if not provided
    if departments is None:
        departments = generate_dim_departments()
    dept_lookup = {dept["department_name"]: dept["department_id"] for dept in departments}
    dept_reverse_lookup = {dept["department_id"]: dept["department_name"] for dept in departments}
    
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
    
    # Get jobs by department using robust lookup
    jobs_by_dept = {}
    for job in jobs:
        dept_name = dept_reverse_lookup.get(job["department_id"], "Unknown")
        if dept_name not in jobs_by_dept:
            jobs_by_dept[dept_name] = []
        jobs_by_dept[dept_name].append(job)
    
    # Calculate department counts with proper distribution
    dept_counts = {}
    remaining_employees = num_employees
    
    for dept_name, percentage in dept_distribution.items():
        if dept_name == list(dept_distribution.keys())[-1]:  # Last department gets remaining employees
            dept_count = remaining_employees
        else:
            dept_count = max(0, int(num_employees * percentage))
            remaining_employees -= dept_count
        
        dept_counts[dept_name] = dept_count
        dept_jobs = jobs_by_dept.get(dept_name, [])
        
        for i in range(dept_count):
            # Generate unique employee id
            employee_id = generate_readable_id("EMP", "employee", 6)
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
            
            # Generate contact information
            phone = fake.phone_number()
            email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 999)}@company.com"
            personal_email = fake.email()
            
            # Generate dates
            # Ensure birth date range is valid (18-65 years old)
            min_birth_date = date.today() - timedelta(days=65*365)
            max_birth_date = date.today() - timedelta(days=18*365)
            
            # Add safety check to ensure valid date range
            if min_birth_date >= max_birth_date:
                # Fallback to a safe range if calculation fails
                min_birth_date = date(1960, 1, 1)
                max_birth_date = date(2006, 1, 1)
            
            birth_date = fake.date_between_dates(date_start=min_birth_date, date_end=max_birth_date)
            
            # Ensure good distribution of hire dates from 2015 onwards
            # 60% hired in first 3 years (2015-2017), 40% distributed across remaining years
            if random.random() < 0.6:
                # Early hires (2015-2017) to ensure sales generation works
                hire_date = fake.date_between_dates(date_start=date(2015, 1, 1), date_end=date(2017, 12, 31))
            else:
                # Later hires (2018-today)
                hire_date = fake.date_between_dates(date_start=date(2018, 1, 1), date_end=date.today())
            
            # Assign random job from department
            job = random.choice(dept_jobs) if dept_jobs else None
            
            # Ensure job is assigned (fallback if no jobs available for department)
            if not job:
                # Create a default job if no department jobs available
                job = {
                    "job_id": generate_readable_id("JOB", "job", 5),
                    "job_title": "General Staff",
                    "job_level": "Entry",
                    "department_id": dept_lookup.get(dept_name, "DEPT001"),
                    "work_setup": "On-site",
                    "work_type": "Full-time"
                }
            
            # Generate government IDs
            tin_number = f"{random.randint(100000000, 999999999)}"
            sss_number = f"{random.randint(1000000000, 9999999999)}"
            philhealth_number = f"{random.randint(100000000000, 999999999999)}"
            pagibig_number = f"{random.randint(1000000000, 9999999999)}"
            
            # Random location, bank, insurance
            location = random.choice(locations)
            bank = random.choice(banks)
            ins = random.choice(insurance)
            
            # Employment status (95% active for realistic company with 20% wage ratio)
            employment_status = random.choices(["Active", "Terminated"], weights=[0.95, 0.05])[0]
            
            # If terminated, generate termination date
            termination_date = None
            if employment_status == "Terminated":
                # Ensure termination date is after hire date and before today
                min_termination = hire_date + timedelta(days=365)
                max_termination = date.today()
                
                if min_termination < max_termination:
                    termination_date = fake.date_between_dates(date_start=min_termination, date_end=max_termination)
                else:
                    # If hire date is too recent, set termination to None (keep as Active)
                    employment_status = "Active"
            
            # Blood type
            blood_type = random.choice(["A+", "A-", "B+", "B-", "O+", "O-", "AB+", "AB-"])
            
            # Emergency contact
            emergency_contact_name = fake.name()
            emergency_contact_relation = random.choice(["Spouse", "Parent", "Sibling", "Friend"])
            emergency_contact_phone = fake.phone_number()
            
            employees.append({
                "employee_id": employee_id,
                "first_name": first_name,
                "last_name": last_name,
                "gender": gender,
                "birth_date": birth_date,
                "phone": phone,
                "email": email,
                "personal_email": personal_email,
                "hire_date": hire_date,
                "termination_date": termination_date,
                "employment_status": employment_status,
                "location_id": location["location_id"],
                "job_id": job["job_id"] if job else None,
                "bank_id": bank["bank_id"],
                "insurance_id": ins["insurance_id"],
                "tin_number": tin_number,
                "sss_number": sss_number,
                "philhealth_number": philhealth_number,
                "pagibig_number": pagibig_number,
                "blood_type": blood_type,
                "emergency_contact_name": emergency_contact_name,
                "emergency_contact_relation": emergency_contact_relation,
                "emergency_contact_phone": emergency_contact_phone
            })
    
    return employees

def generate_fact_employee_wages(employees, jobs, departments=None, start_date=None, end_date=None, start_id=1):
    """Generate annual wage records for all employees with historical data from 2015 to present"""
    wages = []
    wage_sequence = 0
    
    # Create job lookup
    job_lookup = {job["job_id"]: job for job in jobs}
    
    # Create department lookup if departments are provided
    dept_lookup = {}
    if departments:
        dept_lookup = {dept["department_id"]: dept["department_name"] for dept in departments}
    
    # Default start date is 2015-01-01 for historical data
    if start_date is None:
        start_date = date(2015, 1, 1)
    
    # Default end date is today if not provided
    if end_date is None:
        end_date = date.today()
    
    for employee in employees:
        job = job_lookup.get(employee["job_id"])
        if not job:
            continue
        
        # Determine employment period
        hire_date = employee["hire_date"]
        if employee["employment_status"] == "Terminated" and employee["termination_date"]:
            end_employment = employee["termination_date"]
        else:
            end_employment = end_date
        
        # For historical data, start from 2015 or hire date, whichever is later
        # But ensure we have data from 2015 for all employees who were employed at any point since 2015
        historical_start = max(start_date, hire_date)
        
        # Skip if employee wasn't employed during the historical period
        if end_employment < start_date or hire_date > end_date:
            continue
        
        # Get department name
        department_name = dept_lookup.get(job.get("department_id"), "Unknown")
        
        # Initial salary based on job (back-calculated for 2015)
        base_salary = random.randint(job["base_salary_min"], job["base_salary_max"])
        
        # Adjust for work type
        if job["work_type"] == "Part-time":
            base_salary = int(base_salary * 0.6)
        elif job["work_type"] == "Contract":
            base_salary = int(base_salary * 0.9)
        elif job["work_type"] == "Intern":
            base_salary = random.randint(18000, 25000)
        elif job["work_type"] == "Probationary":
            base_salary = int(base_salary * 0.8)
        
        # Generate wage records for each year from historical_start to min(end_employment, end_date)
        # Start from the year of historical start
        current_year_start = date(historical_start.year, 1, 1)
        if current_year_start < historical_start:
            # If started mid-year, start from historical start
            current_year_start = historical_start
        
        current_salary = base_salary
        years_of_service = 0
        
        # Pre-calculate salary progression for consistency
        salary_by_year = {}
        salary_by_year[0] = base_salary  # Year 0 = starting salary
        
        # Calculate salary for each year up to 10 years (raises cap at 10 years)
        for year in range(1, 11):
            raise_percentage = random.uniform(0.03, 0.08)
            if job["job_level"] in ["Manager", "Director"]:
                raise_percentage = random.uniform(0.05, 0.10)
            elif job["job_level"] == "Senior":
                raise_percentage = random.uniform(0.04, 0.09)
            
            # Apply raise to previous year's salary
            salary_by_year[year] = int(salary_by_year[year - 1] * (1 + raise_percentage))
        
        # Generate records for each year
        while current_year_start <= min(end_employment, end_date):
            # Determine the end date for this record (end of year or end_employment, whichever is earlier)
            year_end = date(current_year_start.year, 12, 31)
            record_end_date = min(year_end, end_employment, end_date)
            
            # Calculate years of service at the start of this record
            years_of_service = (current_year_start - hire_date).days // 365
            if years_of_service < 0:
                years_of_service = 0  # For employees hired after 2015, back-calculate service
            
            # Get salary for this year (capped at 10 years for raises)
            salary_year = min(years_of_service, 10)
            current_salary = salary_by_year.get(salary_year, salary_by_year[10])
            
            # Calculate annual salary (12 months worth)
            annual_salary = current_salary * 12
            monthly_salary = current_salary
            
            # Use the start of the year as effective_date for consistency
            # But if hired mid-year, use hire date for first record
            effective_date = date(current_year_start.year, 1, 1)
            if effective_date < historical_start:
                effective_date = historical_start
            
            wage_sequence += 1
            wages.append({
                "wage_id": generate_unique_wage_key(employee["employee_id"], effective_date, wage_sequence),
                "employee_id": employee["employee_id"],
                "effective_date": effective_date,
                "job_title": job["job_title"],
                "job_level": job["job_level"],
                "department": department_name,
                "monthly_salary": monthly_salary,
                "annual_salary": annual_salary,
                "currency": "PHP",
                "years_of_service": years_of_service,
                "salary_grade": (current_salary // 10000) + 1,
                "employment_status": employee["employment_status"]
            })
            
            # Move to next year
            current_year_start = date(current_year_start.year + 1, 1, 1)
    
    return wages

def generate_fact_employees(employees, jobs, start_id=1):
    """Generate simplified employee fact table with current metrics"""
    employee_facts = []
    fact_sequence = 0
    
    # Create job lookup
    job_lookup = {job["job_id"]: job for job in jobs}
    
    for employee in employees:
        if employee["employment_status"] != "Active":
            continue  # Only generate facts for active employees
        
        job = job_lookup.get(employee["job_id"])
        if not job:
            continue
        
        # Performance metrics - ensure no empty values
        performance_rating = random.choices([5, 4, 3, 2, 1], weights=[0.15, 0.35, 0.30, 0.15, 0.05])[0]
        # Convert hire_date to date if it's a Timestamp for date_between_dates
        hire_date_for_review = employee["hire_date"]
        if hasattr(hire_date_for_review, 'date'):
            hire_date_for_review = hire_date_for_review.date()
        last_review_date = fake.date_between_dates(date_start=hire_date_for_review, date_end=date.today())
        promotion_eligible = performance_rating >= 4 and random.random() < 0.6
        
        # Work metrics - ensure no empty values
        # Convert hire_date to date if it's a Timestamp
        hire_date = employee["hire_date"]
        if hasattr(hire_date, 'date'):
            hire_date = hire_date.date()
        years_of_service = (date.today() - hire_date).days // 365
        attendance_rate = round(random.uniform(0.85, 0.98), 3)
        overtime_hours_monthly = random.randint(0, 20) if job["work_type"] == "Full-time" else 0
        productivity_score = random.randint(60, 100)  # Always generate a value
        
        # Engagement metrics - ensure no empty values
        engagement_score = random.randint(1, 10)
        satisfaction_index = random.randint(60, 95)
        retention_risk_score = random.randint(1, 10) if satisfaction_index < 75 else 1  # Default to low risk
        
        # Development metrics - ensure no empty values
        training_hours_completed = random.randint(0, 120)
        certifications_count = random.randint(0, 5)
        skill_gap_score = random.randint(1, 10)  # Always generate a value
        
        # Benefits metrics - ensure no empty values
        benefit_enrollment_date = employee["hire_date"]
        health_utilization_rate = round(random.uniform(0.1, 0.8), 3)  # Always generate a value
        
        # Leave metrics
        vacation_leave_balance = random.randint(0, 15)
        sick_leave_balance = random.randint(0, 10)
        personal_leave_balance = random.randint(0, 5)
        
        fact_sequence += 1
        effective_date = date.today()
        employee_facts.append({
            "employee_fact_id": generate_unique_employee_fact_key(employee["employee_id"], effective_date, fact_sequence),
            "employee_id": employee["employee_id"],
            "effective_date": effective_date,
            
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
        })
    
    return employee_facts

def generate_fact_inventory(products, locations=None, start_id=1):
    """Generate inventory fact table with normalized location references"""
    inventory = []
    
    # If no locations provided, generate some sample location IDs
    if locations:
        # Use actual location IDs from the locations dimension
        location_ids = [loc["location_id"] for loc in locations[:4]]  # Use first 4 locations as warehouses
    else:
        # Fallback to sample location IDs
        location_ids = ["LOC000001", "LOC000002", "LOC000003", "LOC000004"]
    
    inventory_sequence = 0
    
    for product in products:
        # Generate inventory records for each warehouse location
        for location_id in location_ids:
            # Random inventory levels
            cases_on_hand = random.randint(50, 5000)
            
            # Unit cost based on wholesale price with some variation
            base_cost = product["wholesale_price"] * 0.7  # Assume 30% margin
            unit_cost = round(base_cost * random.uniform(0.95, 1.05), 2)
            
            # Generate inventory date (recent snapshot)
            inventory_date = fake.date_between_dates(date_start=date.today() - timedelta(days=30), date_end=date.today())
            
            inventory_sequence += 1
            inventory.append({
                "inventory_id": generate_unique_inventory_key(product["product_id"], location_id, inventory_date, inventory_sequence),
                "inventory_date": inventory_date,
                "product_id": product["product_id"],
                "location_id": location_id,  # Now using string location_id
                "cases_on_hand": cases_on_hand,
                "unit_cost": unit_cost,
                "currency": "PHP"
            })
    
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
    
    for retailer_type, percentage in type_distribution.items():
        type_count = int(num_retailers * percentage)
        
        for i in range(type_count):
            # Generate unique retailer id
            retailer_id = generate_readable_id("R", "retailer", 5)
            
            # Select location
            location = random.choice(locations)
            location_id = location["location_id"]
            
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
                "retailer_id": retailer_id,
                "retailer_name": retailer_name,
                "retailer_type": retailer_type,
                "location_id": location_id,
            })
    
    return retailers

def generate_dim_categories(start_id=1):
    """Generate product categories dimension table"""
    categories = [
        {"category_id": generate_readable_id("CAT", "category", 2), "category_name": "Beverages", "category_code": "BEV"},
        {"category_id": generate_readable_id("CAT", "category", 2), "category_name": "Food", "category_code": "FOD"},
        {"category_id": generate_readable_id("CAT", "category", 2), "category_name": "Personal Care", "category_code": "PER"},
        {"category_id": generate_readable_id("CAT", "category", 2), "category_name": "Household", "category_code": "HH"},
        {"category_id": generate_readable_id("CAT", "category", 2), "category_name": "Health", "category_code": "HLH"},
    ]
    return categories

def generate_dim_brands(start_id=1):
    """Generate brands dimension table"""
    brands = [
        {"brand_id": generate_readable_id("BR", "brand", 3), "brand_name": "Nestlé", "brand_code": "NES"},
        {"brand_id": generate_readable_id("BR", "brand", 3), "brand_name": "Unilever", "brand_code": "UNI"},
        {"brand_id": generate_readable_id("BR", "brand", 3), "brand_name": "Procter & Gamble", "brand_code": "P&G"},
        {"brand_id": generate_readable_id("BR", "brand", 3), "brand_name": "Coca-Cola", "brand_code": "COK"},
        {"brand_id": generate_readable_id("BR", "brand", 3), "brand_name": "PepsiCo", "brand_code": "PEP"},
        {"brand_id": generate_readable_id("BR", "brand", 3), "brand_name": "Mondelez", "brand_code": "MON"},
        {"brand_id": generate_readable_id("BR", "brand", 3), "brand_name": "Johnson & Johnson", "brand_code": "JNJ"},
        {"brand_id": generate_readable_id("BR", "brand", 3), "brand_name": "Colgate-Palmolive", "brand_code": "COL"},
    ]
    return brands

def generate_dim_subcategories(start_id=1):
    """Generate product subcategories dimension table"""
    subcategories = [
        {"subcategory_id": generate_readable_id("SUB", "subcategory", 3), "subcategory_name": "Soft Drinks", "category_code": "BEV"},
        {"subcategory_id": generate_readable_id("SUB", "subcategory", 3), "subcategory_name": "Juices", "category_code": "BEV"},
        {"subcategory_id": generate_readable_id("SUB", "subcategory", 3), "subcategory_name": "Water", "category_code": "BEV"},
        {"subcategory_id": generate_readable_id("SUB", "subcategory", 3), "subcategory_name": "Snacks", "category_code": "FOD"},
        {"subcategory_id": generate_readable_id("SUB", "subcategory", 3), "subcategory_name": "Dairy", "category_code": "FOD"},
        {"subcategory_id": generate_readable_id("SUB", "subcategory", 3), "subcategory_name": "Bakery", "category_code": "FOD"},
        {"subcategory_id": generate_readable_id("SUB", "subcategory", 3), "subcategory_name": "Soap", "category_code": "PER"},
        {"subcategory_id": generate_readable_id("SUB", "subcategory", 3), "subcategory_name": "Shampoo", "category_code": "PER"},
        {"subcategory_id": generate_readable_id("SUB", "subcategory", 3), "subcategory_name": "Cleaning", "category_code": "HH"},
        {"subcategory_id": generate_readable_id("SUB", "subcategory", 3), "subcategory_name": "Vitamins", "category_code": "HLH"},
    ]
    return subcategories

def generate_dim_products(num_products, categories, brands, subcategories, start_id=1):
    """Generate products dimension table with normalized foreign keys"""
    products = []
    
    # Create lookup dictionaries for foreign keys
    category_lookup = {cat["category_name"]: cat for cat in categories}
    brand_lookup = {brand["brand_name"]: brand for brand in brands}
    subcategory_lookup = {sub["subcategory_name"]: sub for sub in subcategories}
    
    # FMCG product categories and data
    product_data = [
        # Food Products
        {"category": "Food", "subcategory": "Snacks", "brand": "Mondelez", "name": "Chocolate Sandwich Cookies", "wholesale": 22.50, "retail": 26.00},
        {"category": "Food", "subcategory": "Snacks", "brand": "Mondelez", "name": "Cream Sandwich", "wholesale": 18.00, "retail": 21.00},
        {"category": "Food", "subcategory": "Dairy", "brand": "Nestlé", "name": "3-in-1 Coffee", "wholesale": 6.50, "retail": 8.00},
        {"category": "Food", "subcategory": "Dairy", "brand": "Nestlé", "name": "White Coffee", "wholesale": 7.00, "retail": 8.50},
        {"category": "Food", "subcategory": "Bakery", "brand": "Mondelez", "name": "Premium Crackers", "wholesale": 15.50, "retail": 18.00},
        {"category": "Food", "subcategory": "Bakery", "brand": "Mondelez", "name": "Assorted Biscuits", "wholesale": 12.00, "retail": 14.50},
        
        # Beverages
        {"category": "Beverages", "subcategory": "Soft Drinks", "brand": "Coca-Cola", "name": "Coca-Cola 1.5L", "wholesale": 45.00, "retail": 52.00},
        {"category": "Beverages", "subcategory": "Soft Drinks", "brand": "PepsiCo", "name": "Pepsi 1.5L", "wholesale": 44.00, "retail": 51.00},
        {"category": "Beverages", "subcategory": "Juices", "brand": "Nestlé", "name": "Orange Juice 1L", "wholesale": 28.50, "retail": 33.00},
        {"category": "Beverages", "subcategory": "Juices", "brand": "Nestlé", "name": "Pineapple Juice 1L", "wholesale": 35.00, "retail": 40.00},
        {"category": "Beverages", "subcategory": "Water", "brand": "Nestlé", "name": "Pure Life Water 500ml", "wholesale": 8.00, "retail": 10.00},
        {"category": "Beverages", "subcategory": "Water", "brand": "Nestlé", "name": "Distilled Water 500ml", "wholesale": 9.00, "retail": 11.00},
        
        # Personal Care
        {"category": "Personal Care", "subcategory": "Soap", "brand": "Procter & Gamble", "name": "Antibacterial Soap", "wholesale": 18.50, "retail": 22.00},
        {"category": "Personal Care", "subcategory": "Soap", "brand": "Unilever", "name": "Beauty Bath Soap", "wholesale": 25.00, "retail": 29.00},
        {"category": "Personal Care", "subcategory": "Shampoo", "brand": "Procter & Gamble", "name": "Anti-Dandruff Shampoo", "wholesale": 32.00, "retail": 37.00},
        {"category": "Personal Care", "subcategory": "Shampoo", "brand": "Procter & Gamble", "name": "Smooth & Silky Shampoo", "wholesale": 35.00, "retail": 40.00},
        {"category": "Personal Care", "subcategory": "Soap", "brand": "Colgate-Palmolive", "name": "Total Toothpaste", "wholesale": 45.00, "retail": 52.00},
        {"category": "Personal Care", "subcategory": "Soap", "brand": "Colgate-Palmolive", "name": "Red Hot Toothpaste", "wholesale": 42.00, "retail": 48.00},
        
        # Household Care
        {"category": "Household", "subcategory": "Cleaning", "brand": "Procter & Gamble", "name": "Laundry Detergent Powder", "wholesale": 28.00, "retail": 32.00},
        {"category": "Household", "subcategory": "Cleaning", "brand": "Unilever", "name": "Laundry Detergent Powder", "wholesale": 25.00, "retail": 29.00},
        {"category": "Household", "subcategory": "Cleaning", "brand": "Procter & Gamble", "name": "Fabric Softener", "wholesale": 22.00, "retail": 26.00},
        {"category": "Household", "subcategory": "Cleaning", "brand": "Unilever", "name": "Fabric Softener", "wholesale": 20.00, "retail": 24.00},
        {"category": "Household", "subcategory": "Cleaning", "brand": "Unilever", "name": "Bleach Disinfectant", "wholesale": 35.00, "retail": 40.00},
        {"category": "Household", "subcategory": "Cleaning", "brand": "Unilever", "name": "Color Safe Bleach", "wholesale": 32.00, "retail": 37.00},
        
        # Health
        {"category": "Health", "subcategory": "Vitamins", "brand": "Johnson & Johnson", "name": "Multivitamin Tablets", "wholesale": 150.00, "retail": 180.00},
        {"category": "Health", "subcategory": "Vitamins", "brand": "Johnson & Johnson", "name": "Vitamin C Supplement", "wholesale": 85.00, "retail": 100.00},
    ]
    
    for i, product in enumerate(product_data):
        # Get foreign key references
        category_ref = category_lookup.get(product["category"])
        brand_ref = brand_lookup.get(product["brand"])
        subcategory_ref = subcategory_lookup.get(product["subcategory"])
        
        if not category_ref or not brand_ref or not subcategory_ref:
            continue  # Skip if foreign key references not found
        
        # Ensure good distribution of created dates from 2015 onwards
        if random.random() < 0.7:
            created_date = fake.date_between_dates(date_start=date(2015, 1, 1), date_end=date(2017, 12, 31))
        else:
            created_date = fake.date_between_dates(date_start=date(2018, 1, 1), date_end=date.today())
        
        # Determine product status based on age
        years_since_creation = (date.today() - created_date).days / 365.0
        
        if years_since_creation > 8:
            delist_probability = 0.40
        elif years_since_creation > 5:
            delist_probability = 0.25
        elif years_since_creation > 3:
            delist_probability = 0.15
        else:
            delist_probability = 0.05
        
        status = "Delisted" if random.random() < delist_probability else "Active"
        
        products.append({
            "product_id": generate_readable_id("P", "product", 4),
            "product_name": product["name"],
            "category_id": category_ref["category_id"],
            "brand_id": brand_ref["brand_id"],
            "subcategory_id": subcategory_ref["subcategory_id"],
            "wholesale_price": product["wholesale"],
            "retail_price": product["retail"],
            "status": status,
            "created_date": created_date
        })
    
    return products

def generate_dim_dates(start_id=1):
    """Generate date dimension table based on Power BI DAX logic"""
    dates = []
    date_key = start_id
    
    # Date range from 2015 to 2030 (matching Power BI DAX)
    start_date = date(2015, 1, 1)
    end_date = date(2030, 12, 31)
    
    current_date = start_date
    while current_date <= end_date:
        # Calculate date attributes based on Power BI DAX
        year = current_date.year
        year_month = current_date.strftime("%Y-%m")
        month = current_date.strftime("%B")
        month_number = current_date.month
        quarter_number = (current_date.month - 1) // 3 + 1
        quarter = f"Q{quarter_number}"
        day = current_date.day
        day_of_week = current_date.strftime("%A")
        day_of_week_number = current_date.weekday() + 1  # Monday=1, Sunday=7
        is_weekend = day_of_week_number in {6, 7}  # Saturday=6, Sunday=7
        
        dates.append({
            "date_id": generate_unique_id("date"),
            "date": current_date,
            "year": year,
            "year_month": year_month,
            "month": month,
            "month_number": month_number,
            "quarter": quarter,
            "quarter_number": quarter_number,
            "day": day,
            "day_of_week": day_of_week,
            "day_of_week_number": day_of_week_number,
            "is_weekend": is_weekend
        })
        current_date += timedelta(days=1)
    
    return dates

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
        
        # Current year campaigns (2026)
        {"name": "New Year Promotion 2026", "type": "Holiday", "start": "2026-01-01", "end": "2026-01-31", "budget": 5000000},
        {"name": "Spring Campaign 2026", "type": "Seasonal", "start": "2026-03-01", "end": "2026-05-31", "budget": 6000000},
        {"name": "Summer Sale 2026", "type": "Seasonal", "start": "2026-06-01", "end": "2026-08-31", "budget": 6500000},
        {"name": "Back to School 2026", "type": "Seasonal", "start": "2026-05-15", "end": "2026-06-30", "budget": 4000000},
        {"name": "Holiday Season 2026", "type": "Holiday", "start": "2026-11-01", "end": "2026-12-31", "budget": 8500000},
    ]
    
    for i, campaign in enumerate(campaign_data):
        campaigns.append({
            "campaign_id": generate_readable_id("C", "campaign", 4),
            "campaign_name": campaign["name"],
            "campaign_type": campaign["type"],
            "start_date": date.fromisoformat(campaign["start"]),
            "end_date": date.fromisoformat(campaign["end"]),
            "budget": campaign["budget"],
            "currency": "PHP"
        })
    
    return campaigns

def validate_relationships(employees, products, retailers, campaigns, locations, departments, jobs, banks, insurance, categories, brands, subcategories):
    """Validate all foreign key relationships for referential integrity"""
    print("Validating table relationships...")
    
    issues = []
    
    # Create lookup dictionaries with proper type handling for BigQuery data
    location_ids = {loc["location_id"] for loc in locations}
    department_ids = {dept["department_id"] for dept in departments}
    job_ids = {job["job_id"] for job in jobs}
    bank_ids = {bank["bank_id"] for bank in banks}
    insurance_ids = {ins["insurance_id"] for ins in insurance}
    product_ids = {prod["product_id"] for prod in products}
    retailer_ids = {ret["retailer_id"] for ret in retailers}
    campaign_ids = {camp["campaign_id"] for camp in campaigns}
    category_ids = {cat["category_id"] for cat in categories}
    brand_ids = {brand["brand_id"] for brand in brands}
    subcategory_ids = {sub["subcategory_id"] for sub in subcategories}
    
    # Helper function to safely convert BigQuery values to int
    def safe_int(value):
        """Safely convert value to int, handling None, NaN, and empty strings"""
        try:
            if value is None or value == '' or (isinstance(value, float) and pd.isna(value)):
                return None
            return int(value)
        except (ValueError, TypeError):
            return None
    
    # Validate employee relationships with type conversion
    for emp in employees:
        emp_id = emp.get('employee_id')
        
        # Skip validation for terminated employees
        if emp.get('employment_status') != 'Active':
            continue
            
        # Convert BigQuery values to int for comparison
        location_id = emp.get("location_id")
        job_id = emp.get("job_id")
        bank_id = emp.get("bank_id")
        insurance_id = emp.get("insurance_id")
        
        # Only validate if keys are not None
        if location_id is not None and location_id not in location_ids:
            issues.append(f"Employee {emp_id}: Invalid location_id {location_id}")
        if job_id is not None and job_id not in job_ids:
            issues.append(f"Employee {emp_id}: Invalid job_id {job_id}")
        if bank_id is not None and bank_id not in bank_ids:
            issues.append(f"Employee {emp_id}: Invalid bank_id {bank_id}")
        if insurance_id is not None and insurance_id not in insurance_ids:
            issues.append(f"Employee {emp_id}: Invalid insurance_id {insurance_id}")
    
    # Validate job department relationships
    for job in jobs:
        dept_id = job.get("department_id")
        if dept_id is not None and dept_id not in department_ids:
            issues.append(f"Job {job['job_id']}: Invalid department_id {dept_id}")
    
    # Validate retailer relationships
    for ret in retailers:
        loc_id = ret.get("location_id")
        if loc_id is not None and loc_id not in location_ids:
            issues.append(f"Retailer {ret['retailer_id']}: Invalid location_id {loc_id}")
    
    # Validate product relationships (NEW)
    for prod in products:
        category_id = prod.get("category_id")
        brand_id = prod.get("brand_id")
        subcategory_id = prod.get("subcategory_id")
        
        if category_id is not None and category_id not in category_ids:
            issues.append(f"Product {prod['product_id']}: Invalid category_id {category_id}")
        if brand_id is not None and brand_id not in brand_ids:
            issues.append(f"Product {prod['product_id']}: Invalid brand_id {brand_id}")
        if subcategory_id is not None and subcategory_id not in subcategory_ids:
            issues.append(f"Product {prod['product_id']}: Invalid subcategory_id {subcategory_id}")
    
    if issues:
        print(f"Found {len(issues)} relationship issues:")
        for issue in issues[:10]:  # Show first 10 issues
            print(f"  - {issue}")
        if len(issues) > 10:
            print(f"  - ... and {len(issues) - 10} more issues")
        return False
    else:
        print("All relationships validated successfully!")
        return True

def generate_daily_sales_with_delivery_updates(employees, products, retailers, campaigns, target_amount, start_date=None, end_date=None, start_id=1):
    """Generate daily sales with simulated delivery status updates for existing orders"""
    import pandas as pd
    from datetime import datetime
    
    sales = []
    sale_sequence = 0
    
    if start_date is None:
        start_date = date.today()
    if end_date is None:
        end_date = start_date
    
    # Filter active records and create safe lookups for robust relationships
    active_employees = [e for e in employees if e.get('employment_status') == 'Active']
    active_products = [p for p in products if p.get('status') == 'Active']
    
    # Create safe lookup dictionaries to prevent relationship errors
    employee_lookup = {emp["employee_id"]: emp for emp in active_employees}
    product_lookup = {prod["product_id"]: prod for prod in active_products}
    retailer_lookup = {ret["retailer_id"]: ret for ret in retailers}
    campaign_lookup = {camp["campaign_id"]: camp for camp in campaigns}
    
    # Validate we have enough records for relationships
    if not active_employees:
        raise ValueError("No active employees found for sales generation")
    if not active_products:
        raise ValueError("No active products found for sales generation")
    if not retailers:
        raise ValueError("No retailers found for sales generation")
    
    print(f"Generating daily sales for {start_date} with delivery simulation")
    
    # Generate new sales for today
    daily_sales_count = random.randint(50, 150)  # Daily sales volume
    for i in range(daily_sales_count):
        # Safe random selection with fallbacks
        employee = random.choice(active_employees)
        product = random.choice(active_products)
        retailer = random.choice(retailers)
        
        # Campaign selection (30% chance of having a campaign)
        campaign = None
        if campaigns and random.random() < 0.3:
            campaign = random.choice(campaigns)
        
        # Sales quantities and pricing
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
        
        # Payment and delivery - simulate realistic delivery progression
        payment_method = random.choice(["Cash", "Credit Card", "Bank Transfer", "Mobile Payment"])
        payment_status = "Paid"
        
        # Simulate delivery status based on current date and expected delivery
        delivery_statuses = ["Processing", "In Transit", "Delivered"]
        delivery_weights = [0.3, 0.4, 0.3]  # 30% processing, 40% in transit, 30% delivered
        
        delivery_status = random.choices(delivery_statuses, weights=delivery_weights)[0]
        
        expected_delivery = start_date + timedelta(days=random.randint(1, 5))
        actual_delivery = expected_delivery if delivery_status == "Delivered" else None
        
        sale_sequence += 1
        # Use current timestamp in milliseconds for additional uniqueness
        timestamp_ms = int((datetime.now().timestamp() * 1000) % 1000)
        sales.append({
            "sale_id": generate_readable_id("SAL", "sale", 6),
            "sale_date": start_date,
            "product_id": product["product_id"],
            "retailer_id": retailer["retailer_id"],
            "case_quantity": case_quantity,
            "unit_price": unit_price,
            "discount_percent": discount_percent,
            "discount_amount": discount_amount,
            "tax_rate": tax_rate,
            "tax_amount": tax_amount,
            "total_amount": total_amount,
            "commission_amount": commission_amount,
            "currency": "PHP",
            "payment_method": payment_method,
            "payment_status": payment_status,
            "delivery_status": delivery_status,
            "expected_delivery_date": expected_delivery,
            "actual_delivery_date": actual_delivery,
        })
    
    print(f"Generated {len(sales)} new daily sales for {start_date}")
    return sales

def generate_fact_sales(employees, products, retailers, campaigns, target_amount, start_date=None, end_date=None, start_id=1):
    """Generate sales fact table with realistic growth over time and robust relationships"""
    sales = []
    sale_sequence = 0
    
    if start_date is None:
        start_date = date.today()
    if end_date is None:
        end_date = start_date
    
    # Create safe lookup dictionaries to prevent relationship errors
    retailer_lookup = {ret["retailer_id"]: ret for ret in retailers}
    campaign_lookup = {camp["campaign_id"]: camp for camp in campaigns}
    
    # Validate we have enough records for relationships
    if not retailers:
        raise ValueError("No retailers found for sales generation")
    
    # Calculate total days and create realistic growth pattern
    total_days = (end_date - start_date).days + 1
    
    # Create growth factors for realistic business growth
    # For 10-year period: start small, grow to exceed target
    growth_start = 0.5  # Start at 50% of final daily rate
    growth_end = 1.5   # End at 140% of target (to exceed ₱8B total)
    
    # Find the earliest date when we have both employees and products available
    # Use 2015-01-01 as minimum for historical data, but consider actual availability
    historical_start_date = date(2015, 1, 1)
    earliest_employee_date = min(e.get('hire_date', date.today()) for e in employees)
    earliest_product_date = min(p.get('created_date', date.today()) for p in products)
    earliest_available_date = max(historical_start_date, earliest_employee_date, earliest_product_date)
    
    # Start sales from the later of: requested start_date or earliest_available_date
    actual_start_date = max(start_date, earliest_available_date)
    
    # For historical data generation, if no employees/products were available in the entire range,
    # create some with earlier dates to ensure data generation works
    if actual_start_date > end_date:
        print(f"Warning: No employees or products available in requested date range")
        print(f"Adjusting to generate data from {start_date} to {end_date} anyway")
        # Force generation by using the requested dates
        actual_start_date = start_date
    
    if actual_start_date != start_date:
        print(f"Adjusted start date from {start_date} to {actual_start_date} to ensure available employees and products")
    
    current_amount = 0
    current_date = actual_start_date
    
    print(f"Generating sales from {actual_start_date} to {end_date} ({(end_date - actual_start_date).days + 1} days)")
    print(f"Growth pattern: {growth_start*100:.0f}% to {growth_end*100:.0f}% over period")
    
    while current_date <= end_date and current_amount < target_amount * 1.4:  # Allow up to 140% of target
        # Filter employees available on this date (hired before or on current_date)
        # For historical data, be more flexible - if no employees available, use all active employees
        available_employees = [
            e for e in employees 
            if e.get('hire_date') and e.get('hire_date') <= current_date 
            and (e.get('employment_status') != 'Terminated' or (e.get('termination_date') and e.get('termination_date') >= current_date))
        ]
        
        # If no employees available for historical dates, use all active employees
        if not available_employees and current_date.year <= 2020:
            available_employees = [e for e in employees if e.get('employment_status') == 'Active']
        
        # Filter products available on this date (created before or on current_date)
        # For historical sales, products can be sold if they were created before the sale date
        # (regardless of current status, since status represents current state, not historical)
        available_products = [
            p for p in products 
            if p.get('created_date') and p.get('created_date') <= current_date
        ]
        
        # If no products available for historical dates, use all active products
        if not available_products and current_date.year <= 2020:
            available_products = [p for p in products if p.get('status') == 'Active']
        
        # If still no employees or products available, skip this day
        if not available_employees or not available_products:
            current_date += timedelta(days=1)
            continue
        
        # Create lookup dictionaries for this date
        employee_lookup = {emp["employee_id"]: emp for emp in available_employees}
        product_lookup = {prod["product_id"]: prod for prod in available_products}
        
        # Calculate progress through the period (0.0 to 1.0)
        progress = (current_date - start_date).days / total_days
        
        # Apply realistic growth curve (exponential growth with some variation)
        growth_factor = growth_start + (growth_end - growth_start) * (progress ** 1.2)
        growth_variation = random.uniform(0.85, 1.15)  # ±15% daily variation
        
        # Calculate daily target with growth
        base_daily_target = target_amount / total_days
        daily_target = base_daily_target * growth_factor * growth_variation
        
        # Calculate number of sales for today
        avg_sale_amount = 15000  # Average sale amount ₱15,000 for realistic FMCG wholesale transactions
        num_sales_today = max(20, int(daily_target / avg_sale_amount))  # Minimum 20 sales per day for realistic volume
        
        # Generate batch of sales for today
        for i in range(num_sales_today):
            if current_amount >= target_amount * 1.4:  # Stop if we exceed 140% of target
                break
                
            # Random selection from available records for this date
            employee = random.choice(available_employees)
            product = random.choice(available_products)
            retailer = random.choice(retailers)
            
            # Campaign selection - only use campaigns active on current_date
            active_campaigns = [
                c for c in campaigns 
                if c.get('start_date') <= current_date <= c.get('end_date')
            ]
            campaign = random.choice(active_campaigns) if active_campaigns and random.random() < 0.3 else None
            
            # Generate sale quantity and calculate amounts
            case_quantity = random.randint(100, 1000)  # Wholesale case quantities (100-1000 units) for ₱15K average sales
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
            
            sale_sequence += 1
            # Use current timestamp in milliseconds for additional uniqueness
            timestamp_ms = int((datetime.now().timestamp() * 1000) % 1000)
            sales.append({
                "sale_id": generate_readable_id("SAL", "sale", 6),
                "sale_date": current_date,
                "product_id": product["product_id"],
                "retailer_id": retailer["retailer_id"],
                "case_quantity": case_quantity,
                "unit_price": unit_price,
                "discount_percent": discount_percent,
                "discount_amount": discount_amount,
                "tax_rate": tax_rate,
                "tax_amount": tax_amount,
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
        
        # Progress reporting
        if len(sales) % 10000 == 0:
            progress_pct = (current_amount / target_amount) * 100
            print(f"Progress: {progress_pct:.1f}% - Generated {len(sales):,} sales - ₱{current_amount:,.0f}")
        
        current_date += timedelta(days=1)
    
    print(f"Completed: Generated {len(sales):,} sales totaling ₱{current_amount:,.0f}")
    print(f"Target was: ₱{target_amount:,.0f} - Achievement: {current_amount/target_amount*100:.1f}%")
    return sales

def generate_fact_operating_costs(target_amount, start_date=None, end_date=None, start_id=1):
    """Generate operating costs fact table"""
    costs = []
    cost_sequence = 0
    
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
    
    # Calculate total days and daily target
    total_days = (end_date - start_date).days + 1
    daily_target = target_amount / total_days
    
    # Calculate how many cost types we have
    total_cost_types = sum(len(cat["types"]) for cat in cost_categories)
    
    # Distribute daily target across all cost types
    daily_per_type = daily_target / total_cost_types
    
    current_date = start_date
    while current_date <= end_date:
        for category_data in cost_categories:
            category = category_data["category"]
            for cost_type in category_data["types"]:
                # Generate daily cost for this type with some variation
                amount = daily_per_type * random.uniform(0.8, 1.2)
                
                cost_sequence += 1
                costs.append({
                    "cost_id": generate_unique_cost_key(current_date, category, cost_sequence),
                    "cost_date": current_date,
                    "category": category,
                    "cost_type": cost_type,
                    "amount": amount,
                    "currency": "PHP"
                })
        
        current_date += timedelta(days=1)
    
    return costs

def generate_fact_marketing_costs(campaigns, target_amount, start_date=None, end_date=None, start_id=1):
    """Generate marketing costs fact table"""
    costs = []
    cost_sequence = 0
    
    if start_date is None:
        start_date = date.today() - timedelta(days=365)
    if end_date is None:
        end_date = date.today()
    
    cost_categories = [
        "Digital Advertising", "Print Media", "TV/Radio", "Events", "Sponsorships", 
        "Social Media", "Content Creation", "Market Research", "Brand Materials"
    ]
    
    # Calculate total days in the period
    total_days = (end_date - start_date).days + 1
    daily_target = target_amount / total_days
    
    # Ensure we have campaigns to work with
    if not campaigns:
        # Generate some default marketing costs even without campaigns
        current_date = start_date
        while current_date <= end_date:
            for category in cost_categories:
                cost_sequence += 1
                costs.append({
                    "marketing_cost_id": generate_unique_marketing_cost_key(None, current_date, category, cost_sequence),
                    "cost_date": current_date,
                    "campaign_id": None,
                    "campaign_type": "General",
                    "cost_category": category,
                    "amount": daily_target / len(cost_categories) * random.uniform(0.8, 1.2),
                    "currency": "PHP"
                })
            current_date += timedelta(days=1)
    else:
        # Generate costs for each day in the period, distributing across campaigns
        current_date = start_date
        while current_date <= end_date:
            # Find active campaigns for this date
            active_campaigns = [
                c for c in campaigns 
                if c['start_date'] <= current_date <= c['end_date']
            ]
            
            if active_campaigns:
                # Distribute daily target across active campaigns
                daily_per_campaign = daily_target / len(active_campaigns)

                for campaign in active_campaigns:
                    for category in cost_categories:
                        cost_sequence += 1
                        costs.append({
                            "marketing_cost_id": generate_unique_marketing_cost_key(campaign["campaign_id"], current_date, category, cost_sequence),
                            "cost_date": current_date,
                            "campaign_id": campaign["campaign_id"],
                            "campaign_type": campaign["campaign_type"],
                            "cost_category": category,
                            "amount": daily_per_campaign / len(cost_categories) * random.uniform(0.8, 1.2),
                            "currency": "PHP"
                        })
            else:
                # No active campaigns - generate general marketing costs
                for category in cost_categories:
                    cost_sequence += 1
                    costs.append({
                        "marketing_cost_key": generate_unique_marketing_cost_key(None, current_date, category, cost_sequence),
                        "cost_date": current_date,
                        "campaign_id": None,
                        "campaign_type": "General",
                        "cost_category": category,
                        "amount": daily_target / len(cost_categories) * random.uniform(0.8, 1.2),
                        "currency": "PHP"
                    })
            
            current_date += timedelta(days=1)
    
    return costs
