"""
Normalized Dimensional Model Data Generator for FMCG Analytics
Generates data for the normalized schema to reduce redundancy
"""

import random
from datetime import date, timedelta
from faker import Faker
import pandas as pd
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
            "country": "Philippines"
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
    """Generate jobs dimension table with optimized salary ranges for realistic wage/revenue ratio"""
    jobs = []
    job_key = start_id

    # Optimized salary ranges for 20% wage/revenue ratio
    salary_ranges = {
        "Entry": (30000, 50000),      # ₱360K-₱600K annually
        "Junior": (50000, 80000),     # ₱600K-₱960K annually
        "Senior": (80000, 140000),    # ₱960K-₱1.68M annually
        "Manager": (140000, 250000),  # ₱1.68M-₱3M annually
        "Director": (250000, 500000)  # ₱3M-₱6M annually
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
    dept_lookup = {dept["department_name"]: dept["department_key"] for dept in departments}

    # Generate jobs
    for department, positions in department_jobs.items():
        dept_key = dept_lookup.get(department)
        if dept_key:
            for position in positions:
                level = position["level"]
                min_sal, max_sal = salary_ranges[level]
                
                jobs.append({
                    "job_key": job_key,
                    "job_title": position["title"],
                    "job_level": level,
                    "department_key": dept_key,
                    "work_setup": position["setup"],
                    "work_type": position["type"],
                    "base_salary_min": min_sal,
                    "base_salary_max": max_sal,
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

def generate_dim_employees_normalized(num_employees, locations, jobs, banks, insurance, departments=None, start_id=1):
    """Generate simplified employees dimension table with job-based compensation"""
    employees = []
    
    # Validate inputs
    if not locations or not jobs or not banks or not insurance:
        raise ValueError("All dimension data (locations, jobs, banks, insurance) must be provided")
    
    # Create lookups for robust foreign key relationships
    location_lookup = {loc["location_key"]: loc for loc in locations}
    job_lookup = {job["job_key"]: job for job in jobs}
    
    # Create department lookup from passed departments or generate if not provided
    if departments is None:
        departments = generate_dim_departments()
    dept_lookup = {dept["department_name"]: dept["department_key"] for dept in departments}
    dept_reverse_lookup = {dept["department_key"]: dept["department_name"] for dept in departments}
    
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
        dept_name = dept_reverse_lookup.get(job["department_key"], "Unknown")
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
            hire_date = fake.date_between_dates(date_start=date(2015, 1, 1), date_end=date.today())
            
            # Assign random job from department
            job = random.choice(dept_jobs) if dept_jobs else None
            
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
            employment_status = random.choices(["Active", "Terminated", "On Leave"], weights=[0.95, 0.04, 0.01])[0]
            
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
                "employee_key": employee_id,
                "employee_id": f"EMP{employee_id:06d}",
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
                "location_key": location["location_key"],
                "job_key": job["job_key"] if job else None,
                "bank_key": bank["bank_key"],
                "insurance_key": ins["insurance_key"],
                "tin_number": tin_number,
                "sss_number": sss_number,
                "philhealth_number": philhealth_number,
                "pagibig_number": pagibig_number,
                "blood_type": blood_type,
                "emergency_contact_name": emergency_contact_name,
                "emergency_contact_relation": emergency_contact_relation,
                "emergency_contact_phone": emergency_contact_phone
            })
            
            employee_id += 1
    
    return employees

def generate_fact_employee_wages(employees, jobs, start_date=None, end_date=None, start_id=1):
    """Generate employee wage history with dates based on job title and seniority"""
    wages = []
    wage_key = start_id
    
    if start_date is None:
        start_date = date(2015, 1, 1)
    if end_date is None:
        end_date = date.today()
    
    # Create job lookup
    job_lookup = {job["job_key"]: job for job in jobs}
    
    for employee in employees:
        if employee["employment_status"] != "Active":
            continue  # Only generate wages for active employees
        
        job = job_lookup.get(employee["job_key"])
        if not job:
            continue
        
        hire_date = employee["hire_date"]
        
        # Generate wage history from hire date to present
        current_date = max(hire_date, start_date)
        
        # Initial salary based on job
        base_salary = random.randint(job["base_salary_min"], job["base_salary_max"])
        
        # Adjust for work type
        if job["work_type"] == "Part-time":
            base_salary = int(base_salary * 0.6)
        elif job["work_type"] == "Contract":
            base_salary = int(base_salary * 0.9)
        elif job["work_type"] == "Intern":
            base_salary = random.randint(18000, 25000)  # Intern salary based on Entry level scale
        elif job["work_type"] == "Probationary":
            base_salary = int(base_salary * 0.8)
        
        current_salary = base_salary
        
        # Generate annual salary changes (promotions, raises, etc.)
        while current_date <= end_date:
            # Calculate years of service
            years_of_service = (current_date - hire_date).days // 365
            
            # Apply annual raises (3-8% based on performance)
            if years_of_service > 0 and current_date.month == 1 and current_date.day == 1:
                # Higher raises for better job levels
                raise_percentage = random.uniform(0.03, 0.08)
                if job["job_level"] in ["Manager", "Director"]:
                    raise_percentage = random.uniform(0.05, 0.10)
                elif job["job_level"] == "Senior":
                    raise_percentage = random.uniform(0.04, 0.09)
                
                current_salary = int(current_salary * (1 + raise_percentage))
            
            # Generate monthly wage record
            wages.append({
                "wage_key": wage_key,
                "employee_key": employee["employee_key"],
                "effective_date": current_date,
                "job_title": job["job_title"],
                "job_level": job["job_level"],
                "department": job_lookup.get(job_lookup.get(employee["job_key"], {}).get("department_key"), {}).get("department_name", "Unknown"),
                "monthly_salary": current_salary,
                "annual_salary": current_salary * 12,
                "currency": "PHP",
                "years_of_service": years_of_service,
                "salary_grade": (current_salary // 10000) + 1
            })
            
            wage_key += 1
            current_date += timedelta(days=30)  # Monthly records
        
        # Ensure current year record exists
        current_year = date.today().year
        current_year_start = date(current_year, 1, 1)
        if hire_date <= current_year_start:
            # Add a record for current year if not present
            has_current_year = any(w['effective_date'].year == current_year for w in wages[-12:])  # Check last 12 records
            if not has_current_year:
                # Calculate final salary with all raises
                final_salary = current_salary
                years_of_service = (date.today() - hire_date).days // 365
                
                # Apply all raises up to current year
                for year in range(1, years_of_service + 1):
                    raise_percentage = random.uniform(0.03, 0.08)
                    if job["job_level"] in ["Manager", "Director"]:
                        raise_percentage = random.uniform(0.05, 0.10)
                    elif job["job_level"] == "Senior":
                        raise_percentage = random.uniform(0.04, 0.09)
                    final_salary = int(final_salary * (1 + raise_percentage))
                
                wages.append({
                    "wage_key": wage_key,
                    "employee_key": employee["employee_key"],
                    "effective_date": date(current_year, 6, 1),  # Mid-year record
                    "job_title": job["job_title"],
                    "job_level": job["job_level"],
                    "department": job_lookup.get(job_lookup.get(employee["job_key"], {}).get("department_key"), {}).get("department_name", "Unknown"),
                    "monthly_salary": final_salary,
                    "annual_salary": final_salary * 12,
                    "currency": "PHP",
                    "years_of_service": years_of_service,
                    "salary_grade": (final_salary // 10000) + 1
                })
                wage_key += 1
    
    return wages

def generate_fact_employees(employees, jobs, start_id=1):
    """Generate simplified employee fact table with current metrics"""
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
        
        # Development metrics
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
        
        employee_facts.append({
            "employee_fact_key": fact_key,
            "employee_key": employee["employee_key"],
            "effective_date": date.today(),
            
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
            "date_key": date_key,
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
        
        date_key += 1
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

def validate_relationships(employees, products, retailers, campaigns, locations, departments, jobs, banks, insurance):
    """Validate all foreign key relationships for referential integrity"""
    print("🔍 Validating table relationships...")
    
    issues = []
    
    # Create lookup dictionaries with proper type handling for BigQuery data
    location_keys = {int(loc["location_key"]) for loc in locations}
    department_keys = {int(dept["department_key"]) for dept in departments}
    job_keys = {int(job["job_key"]) for job in jobs}
    bank_keys = {int(bank["bank_key"]) for bank in banks}
    insurance_keys = {int(ins["insurance_key"]) for ins in insurance}
    product_keys = {int(prod["product_key"]) for prod in products}
    retailer_keys = {int(ret["retailer_key"]) for ret in retailers}
    campaign_keys = {int(camp["campaign_key"]) for camp in campaigns}
    
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
        emp_key = emp.get('employee_key')
        
        # Skip validation for terminated employees
        if emp.get('employment_status') != 'Active':
            continue
            
        # Convert BigQuery values to int for comparison
        location_key = safe_int(emp.get("location_key"))
        job_key = safe_int(emp.get("job_key"))
        bank_key = safe_int(emp.get("bank_key"))
        insurance_key = safe_int(emp.get("insurance_key"))
        
        # Only validate if keys are not None
        if location_key is not None and location_key not in location_keys:
            issues.append(f"Employee {emp_key}: Invalid location_key {location_key}")
        if job_key is not None and job_key not in job_keys:
            issues.append(f"Employee {emp_key}: Invalid job_key {job_key}")
        if bank_key is not None and bank_key not in bank_keys:
            issues.append(f"Employee {emp_key}: Invalid bank_key {bank_key}")
        if insurance_key is not None and insurance_key not in insurance_keys:
            issues.append(f"Employee {emp_key}: Invalid insurance_key {insurance_key}")
    
    # Validate job department relationships
    for job in jobs:
        dept_key = safe_int(job.get("department_key"))
        if dept_key is not None and dept_key not in department_keys:
            issues.append(f"Job {job['job_key']}: Invalid department_key {dept_key}")
    
    # Validate retailer relationships
    for ret in retailers:
        loc_key = safe_int(ret.get("location_key"))
        if loc_key is not None and loc_key not in location_keys:
            issues.append(f"Retailer {ret['retailer_key']}: Invalid location_key {loc_key}")
    
    if issues:
        print(f"❌ Found {len(issues)} relationship issues:")
        for issue in issues[:10]:  # Show first 10 issues
            print(f"  - {issue}")
        if len(issues) > 10:
            print(f"  - ... and {len(issues) - 10} more issues")
        return False
    else:
        print("✅ All relationships validated successfully!")
        return True

def generate_fact_sales(employees, products, retailers, campaigns, target_amount, start_date=None, end_date=None, start_id=1):
    """Generate sales fact table with realistic growth over time and robust relationships"""
    sales = []
    sale_key = start_id
    
    if start_date is None:
        start_date = date.today()
    if end_date is None:
        end_date = start_date
    
    # Filter active records and create safe lookups for robust relationships
    active_employees = [e for e in employees if e.get('employment_status') == 'Active']
    active_products = [p for p in products if p.get('status') == 'Active']
    
    # Create safe lookup dictionaries to prevent relationship errors
    employee_lookup = {emp["employee_key"]: emp for emp in active_employees}
    product_lookup = {prod["product_key"]: prod for prod in active_products}
    retailer_lookup = {ret["retailer_key"]: ret for ret in retailers}
    campaign_lookup = {camp["campaign_key"]: camp for camp in campaigns}
    
    # Validate we have enough records for relationships
    if not active_employees:
        raise ValueError("No active employees found for sales generation")
    if not active_products:
        raise ValueError("No active products found for sales generation")
    if not retailers:
        raise ValueError("No retailers found for sales generation")
    
    print(f"Using {len(active_employees)} employees, {len(active_products)} products, {len(retailers)} retailers, {len(campaigns)} campaigns")
    
    # Calculate total days and create realistic growth pattern
    total_days = (end_date - start_date).days + 1
    
    # Create growth factors for realistic business growth
    # For 10-year period: start small, grow to exceed target
    growth_start = 0.5  # Start at 50% of final daily rate
    growth_end = 1.3   # End at 130% of target (to exceed ₱6B total)
    
    current_amount = 0
    current_date = start_date
    
    print(f"Generating sales from {start_date} to {end_date} ({total_days} days)")
    print(f"Growth pattern: {growth_start*100:.0f}% to {growth_end*100:.0f}% over period")
    
    while current_date <= end_date and current_amount < target_amount * 1.4:  # Allow up to 140% of target
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
                
            # Random selection
            employee = random.choice(active_employees)
            product = random.choice(active_products)
            retailer = random.choice(retailers)
            campaign = random.choice(campaigns) if random.random() < 0.3 else None
            
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
            sale_key += 1
        
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
                
                costs.append({
                    "cost_key": cost_key,
                    "cost_date": current_date,
                    "category": category,
                    "cost_type": cost_type,
                    "amount": amount,
                    "currency": "PHP"
                })
                
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
    
    # Calculate total days in the period
    total_days = (end_date - start_date).days + 1
    daily_target = target_amount / total_days
    
    # Ensure we have campaigns to work with
    if not campaigns:
        # Generate some default marketing costs even without campaigns
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
                        costs.append({
                            "marketing_cost_key": cost_key,
                            "cost_date": current_date,
                            "campaign_key": campaign["campaign_key"],
                            "campaign_id": campaign["campaign_id"],
                            "campaign_type": campaign["campaign_type"],
                            "cost_category": category,
                            "amount": daily_per_campaign / len(cost_categories) * random.uniform(0.8, 1.2),
                            "currency": "PHP"
                        })
                        cost_key += 1
            else:
                # No active campaigns - generate general marketing costs
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
    
    return costs
