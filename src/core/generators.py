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
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
            locations.append(location)
        
        return pd.DataFrame(locations)


class DepartmentGenerator(DataGenerator):
    """Generate department data"""
    
    DEPARTMENTS = [
        {"name": "Executive", "budget": 10000000, "description": "Executive leadership and strategic management"},
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
                "budget": dept["budget"],
                "description": dept["description"],
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
            departments.append(department)
        
        return pd.DataFrame(departments)


class JobGenerator(DataGenerator):
    """Generate comprehensive job position data with realistic roles and career progression"""
    
    JOBS = [
        # Executive Level
        {"title": "Chief Executive Officer", "level": "Executive", "category": "Leadership", "min_salary": 150000, "max_salary": 250000, "dept": "Executive", "work_type": "Office", "is_managerial": True, "years_exp": 15, "education": "Master's Degree", "skills": "Strategic Planning, Leadership, Finance", "family": "Executive", "reporting_level": 1},
        {"title": "Chief Operating Officer", "level": "Executive", "category": "Leadership", "min_salary": 120000, "max_salary": 200000, "dept": "Executive", "work_type": "Office", "is_managerial": True, "years_exp": 12, "education": "Master's Degree", "skills": "Operations Management, Strategy, Leadership", "family": "Executive", "reporting_level": 1},
        {"title": "Chief Financial Officer", "level": "Executive", "category": "Finance", "min_salary": 110000, "max_salary": 180000, "dept": "Finance", "work_type": "Office", "is_managerial": True, "years_exp": 12, "education": "MBA/CPA", "skills": "Financial Planning, Risk Management, Compliance", "family": "Finance", "reporting_level": 1},
        {"title": "Chief Marketing Officer", "level": "Executive", "category": "Marketing", "min_salary": 100000, "max_salary": 170000, "dept": "Marketing", "work_type": "Office", "is_managerial": True, "years_exp": 12, "education": "Master's Degree", "skills": "Marketing Strategy, Brand Management, Analytics", "family": "Marketing", "reporting_level": 1},
        {"title": "Chief Technology Officer", "level": "Executive", "category": "Technology", "min_salary": 120000, "max_salary": 200000, "dept": "IT", "work_type": "Office", "is_managerial": True, "years_exp": 15, "education": "Master's Degree", "skills": "Technology Strategy, Innovation, Leadership", "family": "Technology", "reporting_level": 1},
        
        # Senior Management
        {"title": "Vice President of Sales", "level": "Senior", "category": "Sales", "min_salary": 80000, "max_salary": 120000, "dept": "Sales", "work_type": "Office", "is_managerial": True, "years_exp": 10, "education": "Bachelor's Degree", "skills": "Sales Strategy, Team Leadership, Negotiation", "family": "Sales", "reporting_level": 2},
        {"title": "Vice President of Operations", "level": "Senior", "category": "Operations", "min_salary": 75000, "max_salary": 110000, "dept": "Operations", "work_type": "Office", "is_managerial": True, "years_exp": 10, "education": "Bachelor's Degree", "skills": "Operations Management, Process Improvement, Logistics", "family": "Operations", "reporting_level": 2},
        {"title": "Vice President of Finance", "level": "Senior", "category": "Finance", "min_salary": 70000, "max_salary": 100000, "dept": "Finance", "work_type": "Office", "is_managerial": True, "years_exp": 10, "education": "MBA/CPA", "skills": "Financial Management, Reporting, Compliance", "family": "Finance", "reporting_level": 2},
        {"title": "Vice President of Human Resources", "level": "Senior", "category": "Human Resources", "min_salary": 65000, "max_salary": 95000, "dept": "Human Resources", "work_type": "Office", "is_managerial": True, "years_exp": 10, "education": "Master's Degree", "skills": "HR Strategy, Talent Management, Compliance", "family": "Human Resources", "reporting_level": 2},
        {"title": "Vice President of Marketing", "level": "Senior", "category": "Marketing", "min_salary": 70000, "max_salary": 100000, "dept": "Marketing", "work_type": "Office", "is_managerial": True, "years_exp": 10, "education": "Master's Degree", "skills": "Marketing Strategy, Brand Management, Digital Marketing", "family": "Marketing", "reporting_level": 2},
        
        # Directors
        {"title": "Sales Director", "level": "Senior", "category": "Sales", "min_salary": 60000, "max_salary": 85000, "dept": "Sales", "work_type": "Hybrid", "is_managerial": True, "years_exp": 8, "education": "Bachelor's Degree", "skills": "Sales Management, Territory Planning, Team Leadership", "family": "Sales", "reporting_level": 3},
        {"title": "Regional Sales Director", "level": "Senior", "category": "Sales", "min_salary": 65000, "max_salary": 90000, "dept": "Sales", "work_type": "Field", "is_managerial": True, "years_exp": 8, "education": "Bachelor's Degree", "skills": "Regional Sales, Account Management, Business Development", "family": "Sales", "reporting_level": 3},
        {"title": "Operations Director", "level": "Senior", "category": "Operations", "min_salary": 55000, "max_salary": 80000, "dept": "Operations", "work_type": "Hybrid", "is_managerial": True, "years_exp": 8, "education": "Bachelor's Degree", "skills": "Operations Strategy, Supply Chain, Quality Control", "family": "Operations", "reporting_level": 3},
        {"title": "Finance Director", "level": "Senior", "category": "Finance", "min_salary": 55000, "max_salary": 75000, "dept": "Finance", "work_type": "Office", "is_managerial": True, "years_exp": 8, "education": "MBA/CPA", "skills": "Financial Planning, Analysis, Reporting", "family": "Finance", "reporting_level": 3},
        {"title": "Marketing Director", "level": "Senior", "category": "Marketing", "min_salary": 55000, "max_salary": 80000, "dept": "Marketing", "work_type": "Office", "is_managerial": True, "years_exp": 8, "education": "Master's Degree", "skills": "Marketing Strategy, Campaign Management, Analytics", "family": "Marketing", "reporting_level": 3},
        {"title": "IT Director", "level": "Senior", "category": "Technology", "min_salary": 60000, "max_salary": 85000, "dept": "IT", "work_type": "Office", "is_managerial": True, "years_exp": 10, "education": "Master's Degree", "skills": "IT Strategy, Infrastructure Management, Security", "family": "Technology", "reporting_level": 3},
        {"title": "HR Director", "level": "Senior", "category": "Human Resources", "min_salary": 50000, "max_salary": 70000, "dept": "Human Resources", "work_type": "Office", "is_managerial": True, "years_exp": 8, "education": "Master's Degree", "skills": "HR Management, Employee Relations, Compliance", "family": "Human Resources", "reporting_level": 3},
        
        # Managers
        {"title": "Sales Manager", "level": "Mid", "category": "Sales", "min_salary": 35000, "max_salary": 50000, "dept": "Sales", "work_type": "Hybrid", "is_managerial": True, "years_exp": 5, "education": "Bachelor's Degree", "skills": "Sales Leadership, Coaching, Performance Management", "family": "Sales", "reporting_level": 4},
        {"title": "Area Sales Manager", "level": "Mid", "category": "Sales", "min_salary": 38000, "max_salary": 55000, "dept": "Sales", "work_type": "Field", "is_managerial": True, "years_exp": 5, "education": "Bachelor's Degree", "skills": "Area Sales, Account Management, Team Leadership", "family": "Sales", "reporting_level": 4},
        {"title": "Key Account Manager", "level": "Mid", "category": "Sales", "min_salary": 32000, "max_salary": 48000, "dept": "Sales", "work_type": "Field", "is_managerial": False, "years_exp": 4, "education": "Bachelor's Degree", "skills": "Account Management, Negotiation, Relationship Building", "family": "Sales", "reporting_level": 5},
        {"title": "Operations Manager", "level": "Mid", "category": "Operations", "min_salary": 32000, "max_salary": 48000, "dept": "Operations", "work_type": "Hybrid", "is_managerial": True, "years_exp": 5, "education": "Bachelor's Degree", "skills": "Operations Management, Process Improvement, Team Leadership", "family": "Operations", "reporting_level": 4},
        {"title": "Warehouse Manager", "level": "Mid", "category": "Operations", "min_salary": 30000, "max_salary": 45000, "dept": "Operations", "work_type": "Field", "is_managerial": True, "years_exp": 4, "education": "Bachelor's Degree", "skills": "Warehouse Operations, Inventory Management, Safety", "family": "Operations", "reporting_level": 4},
        {"title": "Finance Manager", "level": "Mid", "category": "Finance", "min_salary": 35000, "max_salary": 50000, "dept": "Finance", "work_type": "Office", "is_managerial": True, "years_exp": 5, "education": "MBA/CPA", "skills": "Financial Management, Budgeting, Analysis", "family": "Finance", "reporting_level": 4},
        {"title": "Accounting Manager", "level": "Mid", "category": "Finance", "min_salary": 32000, "max_salary": 45000, "dept": "Finance", "work_type": "Office", "is_managerial": True, "years_exp": 4, "education": "Accounting Degree", "skills": "Accounting, Reporting, Compliance", "family": "Finance", "reporting_level": 4},
        {"title": "Marketing Manager", "level": "Mid", "category": "Marketing", "min_salary": 33000, "max_salary": 48000, "dept": "Marketing", "work_type": "Office", "is_managerial": True, "years_exp": 5, "education": "Bachelor's Degree", "skills": "Marketing Management, Campaign Management, Analytics", "family": "Marketing", "reporting_level": 4},
        {"title": "Digital Marketing Manager", "level": "Mid", "category": "Marketing", "min_salary": 30000, "max_salary": 45000, "dept": "Marketing", "work_type": "Remote", "is_managerial": True, "years_exp": 4, "education": "Bachelor's Degree", "skills": "Digital Marketing, SEO, Social Media, Analytics", "family": "Marketing", "reporting_level": 4},
        {"title": "Brand Manager", "level": "Mid", "category": "Marketing", "min_salary": 32000, "max_salary": 46000, "dept": "Marketing", "work_type": "Office", "is_managerial": True, "years_exp": 4, "education": "Bachelor's Degree", "skills": "Brand Management, Market Research, Creative Strategy", "family": "Marketing", "reporting_level": 4},
        {"title": "HR Manager", "level": "Mid", "category": "Human Resources", "min_salary": 30000, "max_salary": 45000, "dept": "Human Resources", "work_type": "Office", "is_managerial": True, "years_exp": 5, "education": "Bachelor's Degree", "skills": "HR Management, Recruitment, Employee Relations", "family": "Human Resources", "reporting_level": 4},
        {"title": "IT Manager", "level": "Mid", "category": "Technology", "min_salary": 35000, "max_salary": 52000, "dept": "IT", "work_type": "Office", "is_managerial": True, "years_exp": 6, "education": "Bachelor's Degree", "skills": "IT Management, Infrastructure, Security", "family": "Technology", "reporting_level": 4},
        {"title": "Software Development Manager", "level": "Mid", "category": "Technology", "min_salary": 40000, "max_salary": 60000, "dept": "IT", "work_type": "Office", "is_managerial": True, "years_exp": 6, "education": "Bachelor's Degree", "skills": "Software Development, Agile, Team Leadership", "family": "Technology", "reporting_level": 4},
        
        # Senior Professionals
        {"title": "Senior Sales Representative", "level": "Senior", "category": "Sales", "min_salary": 28000, "max_salary": 40000, "dept": "Sales", "work_type": "Field", "is_managerial": False, "years_exp": 3, "education": "Bachelor's Degree", "skills": "Sales, Negotiation, Customer Relations", "family": "Sales", "reporting_level": 6},
        {"title": "Senior Accountant", "level": "Senior", "category": "Finance", "min_salary": 28000, "max_salary": 38000, "dept": "Finance", "work_type": "Office", "is_managerial": False, "years_exp": 4, "education": "Accounting Degree", "skills": "Accounting, Financial Reporting, Analysis", "family": "Finance", "reporting_level": 6},
        {"title": "Senior Financial Analyst", "level": "Senior", "category": "Finance", "min_salary": 30000, "max_salary": 42000, "dept": "Finance", "work_type": "Office", "is_managerial": False, "years_exp": 4, "education": "Bachelor's Degree", "skills": "Financial Analysis, Modeling, Reporting", "family": "Finance", "reporting_level": 6},
        {"title": "Senior Marketing Specialist", "level": "Senior", "category": "Marketing", "min_salary": 26000, "max_salary": 38000, "dept": "Marketing", "work_type": "Office", "is_managerial": False, "years_exp": 3, "education": "Bachelor's Degree", "skills": "Marketing, Campaign Management, Analytics", "family": "Marketing", "reporting_level": 6},
        {"title": "Senior Software Developer", "level": "Senior", "category": "Technology", "min_salary": 35000, "max_salary": 50000, "dept": "IT", "work_type": "Remote", "is_managerial": False, "years_exp": 4, "education": "Bachelor's Degree", "skills": "Software Development, Architecture, Testing", "family": "Technology", "reporting_level": 6},
        {"title": "Senior Quality Assurance Analyst", "level": "Senior", "category": "Quality Assurance", "min_salary": 26000, "max_salary": 36000, "dept": "Operations", "work_type": "Field", "is_managerial": False, "years_exp": 3, "education": "Bachelor's Degree", "skills": "Quality Assurance, Testing, Process Improvement", "family": "Quality Assurance", "reporting_level": 6},
        
        # Mid-Level Professionals
        {"title": "Sales Representative", "level": "Entry", "category": "Sales", "min_salary": 18000, "max_salary": 28000, "dept": "Sales", "work_type": "Field", "is_managerial": False, "years_exp": 1, "education": "Bachelor's Degree", "skills": "Sales, Communication, Customer Service", "family": "Sales", "reporting_level": 7},
        {"title": "Sales Coordinator", "level": "Entry", "category": "Sales", "min_salary": 16000, "max_salary": 24000, "dept": "Sales", "work_type": "Office", "is_managerial": False, "years_exp": 0, "education": "Bachelor's Degree", "skills": "Coordination, Administration, Customer Service", "family": "Sales", "reporting_level": 7},
        {"title": "Accountant", "level": "Entry", "category": "Finance", "min_salary": 20000, "max_salary": 30000, "dept": "Finance", "work_type": "Office", "is_managerial": False, "years_exp": 1, "education": "Accounting Degree", "skills": "Accounting, Bookkeeping, Financial Reporting", "family": "Finance", "reporting_level": 7},
        {"title": "Financial Analyst", "level": "Entry", "category": "Finance", "min_salary": 22000, "max_salary": 32000, "dept": "Finance", "work_type": "Office", "is_managerial": False, "years_exp": 2, "education": "Bachelor's Degree", "skills": "Financial Analysis, Excel, Reporting", "family": "Finance", "reporting_level": 7},
        {"title": "Marketing Coordinator", "level": "Entry", "category": "Marketing", "min_salary": 18000, "max_salary": 26000, "dept": "Marketing", "work_type": "Office", "is_managerial": False, "years_exp": 1, "education": "Bachelor's Degree", "skills": "Marketing Coordination, Social Media, Content", "family": "Marketing", "reporting_level": 7},
        {"title": "Marketing Specialist", "level": "Entry", "category": "Marketing", "min_salary": 20000, "max_salary": 30000, "dept": "Marketing", "work_type": "Office", "is_managerial": False, "years_exp": 2, "education": "Bachelor's Degree", "skills": "Marketing, Digital Marketing, Analytics", "family": "Marketing", "reporting_level": 7},
        {"title": "HR Specialist", "level": "Entry", "category": "Human Resources", "min_salary": 18000, "max_salary": 28000, "dept": "Human Resources", "work_type": "Office", "is_managerial": False, "years_exp": 1, "education": "Bachelor's Degree", "skills": "HR Administration, Recruitment, Employee Relations", "family": "Human Resources", "reporting_level": 7},
        {"title": "HR Coordinator", "level": "Entry", "category": "Human Resources", "min_salary": 16000, "max_salary": 24000, "dept": "Human Resources", "work_type": "Office", "is_managerial": False, "years_exp": 0, "education": "Bachelor's Degree", "skills": "HR Coordination, Administration, Onboarding", "family": "Human Resources", "reporting_level": 7},
        {"title": "IT Support Specialist", "level": "Entry", "category": "Technology", "min_salary": 18000, "max_salary": 28000, "dept": "IT", "work_type": "Hybrid", "is_managerial": False, "years_exp": 1, "education": "Bachelor's Degree", "skills": "IT Support, Troubleshooting, Customer Service", "family": "Technology", "reporting_level": 7},
        {"title": "Software Developer", "level": "Entry", "category": "Technology", "min_salary": 25000, "max_salary": 38000, "dept": "IT", "work_type": "Remote", "is_managerial": False, "years_exp": 2, "education": "Bachelor's Degree", "skills": "Software Development, Programming, Testing", "family": "Technology", "reporting_level": 7},
        {"title": "Junior Software Developer", "level": "Entry", "category": "Technology", "min_salary": 20000, "max_salary": 30000, "dept": "IT", "work_type": "Remote", "is_managerial": False, "years_exp": 0, "education": "Bachelor's Degree", "skills": "Programming, Web Development, Databases", "family": "Technology", "reporting_level": 7},
        
        # Operations Roles
        {"title": "Operations Supervisor", "level": "Mid", "category": "Operations", "min_salary": 25000, "max_salary": 35000, "dept": "Operations", "work_type": "Field", "is_managerial": True, "years_exp": 3, "education": "Bachelor's Degree", "skills": "Supervision, Operations, Safety", "family": "Operations", "reporting_level": 5},
        {"title": "Production Supervisor", "level": "Mid", "category": "Operations", "min_salary": 24000, "max_salary": 34000, "dept": "Operations", "work_type": "Field", "is_managerial": True, "years_exp": 3, "education": "Bachelor's Degree", "skills": "Production Supervision, Quality Control, Safety", "family": "Operations", "reporting_level": 5},
        {"title": "Logistics Coordinator", "level": "Entry", "category": "Operations", "min_salary": 18000, "max_salary": 26000, "dept": "Operations", "work_type": "Office", "is_managerial": False, "years_exp": 1, "education": "Bachelor's Degree", "skills": "Logistics, Coordination, Supply Chain", "family": "Operations", "reporting_level": 7},
        {"title": "Quality Assurance Analyst", "level": "Entry", "category": "Quality Assurance", "min_salary": 20000, "max_salary": 30000, "dept": "Operations", "work_type": "Field", "is_managerial": False, "years_exp": 1, "education": "Bachelor's Degree", "skills": "Quality Assurance, Testing, Documentation", "family": "Quality Assurance", "reporting_level": 7},
        {"title": "Quality Control Inspector", "level": "Entry", "category": "Quality Assurance", "min_salary": 17000, "max_salary": 25000, "dept": "Operations", "work_type": "Field", "is_managerial": False, "years_exp": 0, "education": "High School", "skills": "Quality Control, Inspection, Documentation", "family": "Quality Assurance", "reporting_level": 7},
        
        # Customer Service
        {"title": "Customer Service Manager", "level": "Mid", "category": "Customer Service", "min_salary": 28000, "max_salary": 40000, "dept": "Customer Service", "work_type": "Office", "is_managerial": True, "years_exp": 4, "education": "Bachelor's Degree", "skills": "Customer Service Management, Team Leadership, Problem Solving", "family": "Customer Service", "reporting_level": 4},
        {"title": "Customer Service Representative", "level": "Entry", "category": "Customer Service", "min_salary": 15000, "max_salary": 22000, "dept": "Customer Service", "work_type": "Office", "is_managerial": False, "years_exp": 0, "education": "High School", "skills": "Customer Service, Communication, Problem Solving", "family": "Customer Service", "reporting_level": 7},
        {"title": "Customer Service Team Lead", "level": "Mid", "category": "Customer Service", "min_salary": 22000, "max_salary": 30000, "dept": "Customer Service", "work_type": "Office", "is_managerial": True, "years_exp": 3, "education": "Bachelor's Degree", "skills": "Team Leadership, Customer Service, Training", "family": "Customer Service", "reporting_level": 5},
        
        # Research & Development
        {"title": "R&D Manager", "level": "Mid", "category": "Research & Development", "min_salary": 35000, "max_salary": 50000, "dept": "Research & Development", "work_type": "Office", "is_managerial": True, "years_exp": 5, "education": "Master's Degree", "skills": "R&D Management, Innovation, Project Management", "family": "Research & Development", "reporting_level": 4},
        {"title": "R&D Specialist", "level": "Entry", "category": "Research & Development", "min_salary": 25000, "max_salary": 38000, "dept": "Research & Development", "work_type": "Office", "is_managerial": False, "years_exp": 2, "education": "Bachelor's Degree", "skills": "Research, Analysis, Innovation", "family": "Research & Development", "reporting_level": 7},
        {"title": "Product Development Specialist", "level": "Entry", "category": "Research & Development", "min_salary": 24000, "max_salary": 36000, "dept": "Research & Development", "work_type": "Office", "is_managerial": False, "years_exp": 2, "education": "Bachelor's Degree", "skills": "Product Development, Market Research, Innovation", "family": "Research & Development", "reporting_level": 7},
        
        # Administration
        {"title": "Office Manager", "level": "Mid", "category": "Administration", "min_salary": 24000, "max_salary": 34000, "dept": "Administration", "work_type": "Office", "is_managerial": True, "years_exp": 3, "education": "Bachelor's Degree", "skills": "Office Management, Administration, Coordination", "family": "Administration", "reporting_level": 4},
        {"title": "Administrative Assistant", "level": "Entry", "category": "Administration", "min_salary": 14000, "max_salary": 20000, "dept": "Administration", "work_type": "Office", "is_managerial": False, "years_exp": 0, "education": "High School", "skills": "Administration, Communication, Organization", "family": "Administration", "reporting_level": 7},
        {"title": "Executive Assistant", "level": "Entry", "category": "Administration", "min_salary": 18000, "max_salary": 26000, "dept": "Administration", "work_type": "Office", "is_managerial": False, "years_exp": 2, "education": "Bachelor's Degree", "skills": "Executive Support, Communication, Organization", "family": "Administration", "reporting_level": 6},
        
        # Supply Chain & Procurement
        {"title": "Procurement Manager", "level": "Mid", "category": "Procurement", "min_salary": 30000, "max_salary": 45000, "dept": "Operations", "work_type": "Office", "is_managerial": True, "years_exp": 4, "education": "Bachelor's Degree", "skills": "Procurement, Negotiation, Supply Chain", "family": "Procurement", "reporting_level": 4},
        {"title": "Procurement Specialist", "level": "Entry", "category": "Procurement", "min_salary": 20000, "max_salary": 30000, "dept": "Operations", "work_type": "Office", "is_managerial": False, "years_exp": 2, "education": "Bachelor's Degree", "skills": "Procurement, Vendor Management, Negotiation", "family": "Procurement", "reporting_level": 7},
        {"title": "Supply Chain Analyst", "level": "Entry", "category": "Procurement", "min_salary": 22000, "max_salary": 32000, "dept": "Operations", "work_type": "Office", "is_managerial": False, "years_exp": 2, "education": "Bachelor's Degree", "skills": "Supply Chain, Analysis, Logistics", "family": "Procurement", "reporting_level": 7},
        
        # Data & Analytics
        {"title": "Data Analyst", "level": "Entry", "category": "Analytics", "min_salary": 22000, "max_salary": 34000, "dept": "IT", "work_type": "Office", "is_managerial": False, "years_exp": 2, "education": "Bachelor's Degree", "skills": "Data Analysis, SQL, Excel, Visualization", "family": "Analytics", "reporting_level": 7},
        {"title": "Business Analyst", "level": "Entry", "category": "Analytics", "min_salary": 24000, "max_salary": 36000, "dept": "IT", "work_type": "Office", "is_managerial": False, "years_exp": 2, "education": "Bachelor's Degree", "skills": "Business Analysis, Requirements, Process Mapping", "family": "Analytics", "reporting_level": 7},
        {"title": "Data Scientist", "level": "Mid", "category": "Analytics", "min_salary": 32000, "max_salary": 48000, "dept": "IT", "work_type": "Remote", "is_managerial": False, "years_exp": 3, "education": "Master's Degree", "skills": "Data Science, Machine Learning, Statistics", "family": "Analytics", "reporting_level": 6},
    ]
    
    def __init__(self, faker: Faker, departments_df: pd.DataFrame):
        super().__init__(faker)
        self.departments_df = departments_df
        self.dept_name_to_id = dict(zip(departments_df["department_name"], departments_df["department_id"]))
    
    def generate_jobs(self) -> pd.DataFrame:
        """Generate comprehensive job position data"""
        jobs = []
        
        for i, job in enumerate(self.JOBS):
            job_data = {
                "job_id": id_generator.generate_id('dim_jobs'),
                "job_title": job["title"],
                "job_level": job["level"],
                "job_category": job["category"],
                "min_salary": job["min_salary"],
                "max_salary": job["max_salary"],
                "department_id": self.dept_name_to_id[job["dept"]],
                "work_type": job["work_type"],
                "is_managerial": job["is_managerial"],
                "years_experience_required": job["years_exp"],
                "education_required": job["education"],
                "skills_required": job["skills"],
                "job_family": job["family"],
                "reporting_level": job["reporting_level"],
                "description": f"{job['level']} level {job['title']} position in {job['category']}. Requires {job['years_exp']} years experience and {job['education']}.",
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
            jobs.append(job_data)
        
        return pd.DataFrame(jobs)


class EmployeeGenerator(DataGenerator):
    """Generate employee data"""
    
    EMPLOYMENT_TYPES = [
        "Regular", "Contract", "Probationary", "Project-Based", 
        "Part-Time", "Intern", "Consultant", "Seasonal"
    ]
    
    WORK_SETUPS = [
        "On-Site", "Remote", "Hybrid", "Field-Based", 
        "Work-from-Home", "Office-Based", "Flexible"
    ]
    
    def __init__(self, faker: Faker, departments_df: pd.DataFrame, jobs_df: pd.DataFrame, locations_df: pd.DataFrame):
        super().__init__(faker)
        self.departments_df = departments_df
        self.jobs_df = jobs_df
        self.locations_df = locations_df
    
    def generate_employees(self, count: int) -> pd.DataFrame:
        """Generate employee data with IDs based on hire date order"""
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
            
            # Random hire date from company founding (2015-01-01) to today
            from datetime import date
            hire_date = self.faker.date_between(start_date=date(2015, 1, 1), end_date="today")
            
            # 10% chance of being terminated
            termination_date = None
            status = "Active"
            if random.random() < 0.1:
                termination_date = self.faker.date_between(start_date=hire_date, end_date="today")
                status = "Terminated"
            
            # Generate gender first, then name to match
            gender = random.choice(["Male", "Female"])
            
            # Generate names based on gender
            if gender == "Male":
                first_name = self.faker.first_name_male()
                last_name = self.faker.last_name_male()
            else:
                first_name = self.faker.first_name_female()
                last_name = self.faker.last_name_female()
            
            # Generate employment type based on hire date and job characteristics
            today = datetime.now().date()
            hire_date_obj = hire_date if isinstance(hire_date, date) else hire_date.date()
            months_employed = (today - hire_date_obj).days / 30.44  # Average days per month
            job_title = str(job.get("job_title", ""))
            
            # Employment type logic based on tenure
            if "Intern" in job_title or "Trainee" in job_title:
                employment_type = "Intern"
            elif "Consultant" in job_title or "Advisor" in job_title:
                employment_type = "Consultant"
            elif months_employed <= 6:
                # Hired in past 6 months = Probationary (unless terminated before completing 6 months)
                if termination_date:
                    termination_date_obj = termination_date if isinstance(termination_date, date) else termination_date.date()
                    months_until_termination = (termination_date_obj - hire_date_obj).days / 30.44
                    if months_until_termination <= 6:
                        employment_type = "Probationary"  # Terminated during probation
                    else:
                        employment_type = "Regular"  # Completed probation then terminated
                else:
                    employment_type = "Probationary"  # Currently in probation
            elif "Manager" in job_title or "Director" in job_title or "Executive" in job_title:
                # Management roles are typically Regular or Contract
                employment_type = random.choice(["Regular", "Contract"])
            elif "Sales" in job_title and random.random() < 0.3:
                employment_type = "Commission-Based"
            else:
                # For employees with more than 6 months tenure
                # 80% Regular, 15% Contract, 5% others
                employment_type = random.choices(
                    ["Regular", "Contract", "Project-Based", "Part-Time"],
                    weights=[80, 15, 3, 2]
                )[0]
            
            # Work setup logic
            if "Sales" in job_title or "Field" in job_title:
                work_setup = random.choice(["Field-Based", "Hybrid", "On-Site"])
            elif "IT" in job_title or "Developer" in job_title:
                work_setup = random.choice(["Remote", "Hybrid", "On-Site"])
            elif "Driver" in job_title or "Delivery" in job_title:
                work_setup = "Field-Based"
            elif "Manager" in job_title or "Director" in job_title:
                work_setup = random.choice(["Office-Based", "Hybrid"])
            else:
                # 50% On-Site, 25% Hybrid, 15% Remote, 10% others
                work_setup = random.choices(
                    ["On-Site", "Hybrid", "Remote", "Office-Based", "Flexible"],
                    weights=[50, 25, 15, 7, 3]
                )[0]
            
            # Create employee without ID first
            employee = {
                "first_name": first_name,
                "last_name": last_name,
                "gender": gender,
                "email": self.faker.email(),
                "phone": self.faker.basic_phone_number() if hasattr(self.faker, 'basic_phone_number') else f"+63-{random.randint(900000000, 999999999)}",
                "department_id": department["department_id"],
                "job_id": job["job_id"],
                "hire_date": hire_date,
                "termination_date": termination_date,
                "status": status,
                "employment_type": employment_type,
                "work_setup": work_setup,
                "location_id": location["location_id"],
                "bank_id": f"BNK-{random.randint(1, 15):03d}",  # Always assign a bank
                "insurance_id": f"INS-{random.randint(1, 12):03d}",  # Always assign insurance
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
            employees.append(employee)
        
        # Convert to DataFrame and sort by hire date
        employees_df = pd.DataFrame(employees)
        employees_df = employees_df.sort_values('hire_date').reset_index(drop=True)
        
        # Assign IDs in chronological order (Employee 1 = earliest hire)
        for idx, row in employees_df.iterrows():
            employees_df.at[idx, 'employee_id'] = id_generator.generate_id('dim_employees')
        
        return employees_df


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
            
            # Generate realistic pricing for FMCG (target 25-35% gross margin)
            base_price = random.uniform(10, 500)
            cost = base_price * random.uniform(0.65, 0.75)  # Cost is 65-75% of price = 25-35% margin
            
            # Generate launch date from company founding (2015-01-01) to today
            from datetime import date
            launch_date = self.faker.date_between(start_date=date(2015, 1, 1), end_date="today")
            
            product = {
                "product_name": f"{brand['brand_name']} {subcategory['subcategory_name']} {i+1}",
                "sku": f"SKU-{i+1:06d}",
                "category_id": category["category_id"],
                "subcategory_id": subcategory["subcategory_id"],
                "brand_id": brand["brand_id"],
                "unit_price": round(base_price, 2),
                "cost": round(cost, 2),
                "status": random.choice(["Active", "Discontinued", "Pending"]),
                "launch_date": launch_date,
                "discontinued_date": None,
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
            products.append(product)
        
        # Convert to DataFrame and sort by launch date
        products_df = pd.DataFrame(products)
        products_df = products_df.sort_values('launch_date').reset_index(drop=True)
        
        # Assign IDs in chronological order (Product 1 = earliest launch)
        for idx, row in products_df.iterrows():
            products_df.at[idx, 'product_id'] = id_generator.generate_id('dim_products')
        
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
                "registration_date": self.faker.date_between(start_date="-11y", end_date="today"),
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
            retailers.append(retailer)
        
        return pd.DataFrame(retailers)


class BankGenerator(DataGenerator):
    """Generate bank data"""
    
    def __init__(self, faker: Faker):
        super().__init__(faker)
        self.bank_names = [
            "Banco de Oro", "Metropolitan Bank & Trust Company", "Bank of the Philippine Islands",
            "LandBank of the Philippines", "Philippine National Bank", "Security Bank Corporation",
            "UnionBank of the Philippines", "China Banking Corporation", "Rizal Commercial Banking Corporation",
            "Philippine Bank of Communications", "Asian Development Bank", "Maybank Philippines",
            "Citibank Philippines", "HSBC Philippines", "Standard Chartered Philippines"
        ]
        
        self.bank_codes = [
            "BDO", "MBTC", "BPI", "LANDBANK", "PNB", "SECB", 
            "UBP", "CHINABANK", "RCBC", "PBCOM", "ADB", "MAYBANK",
            "CITIBANK", "HSBC", "SCB"
        ]
        
        self.account_types = ["Savings", "Checking", "Payroll", "Time Deposit"]
    
    def generate_banks(self, count: int = 15) -> pd.DataFrame:
        """Generate bank data"""
        banks = []
        
        for i in range(min(count, len(self.bank_names))):
            bank = {
                "bank_id": f"BNK-{i+1:03d}",
                "bank_name": self.bank_names[i],
                "bank_code": self.bank_codes[i],
                "account_type": random.choice(self.account_types),
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
            banks.append(bank)
        
        return pd.DataFrame(banks)


class InsuranceGenerator(DataGenerator):
    """Generate insurance data"""
    
    def __init__(self, faker: Faker):
        super().__init__(faker)
        self.insurance_companies = [
            "Philippine Health Insurance Corporation", "Social Security System", "Government Service Insurance System",
            "Philam Life", "Sun Life of Canada", "Manulife Philippines", "AXA Philippines",
            "Insular Life", "Prudentialife", "Pacific Cross Insurance", "Chartered Insurance",
            "Malayan Insurance", "Mapfre Insurance", "Generali Philippines", "BPI/MS Insurance"
        ]
        
        self.policy_types = [
            "HMO", "Life Insurance", "Health Insurance", "Accident Insurance", 
            "Disability Insurance", "Retirement Insurance", "Critical Illness Insurance"
        ]
    
    def generate_insurance(self, count: int = 12) -> pd.DataFrame:
        """Generate insurance data"""
        insurance = []
        
        for i in range(min(count, len(self.insurance_companies))):
            policy_type = random.choice(self.policy_types)
            
            # Generate realistic coverage and premium amounts based on policy type
            if policy_type in ["HMO", "Health Insurance"]:
                coverage = random.uniform(50000, 500000)
                premium = random.uniform(1000, 8000)
            elif policy_type == "Life Insurance":
                coverage = random.uniform(500000, 5000000)
                premium = random.uniform(5000, 25000)
            elif policy_type == "Accident Insurance":
                coverage = random.uniform(100000, 1000000)
                premium = random.uniform(2000, 10000)
            else:
                coverage = random.uniform(100000, 2000000)
                premium = random.uniform(3000, 15000)
            
            insurance_record = {
                "insurance_id": f"INS-{i+1:03d}",
                "insurance_name": self.insurance_companies[i],
                "policy_type": policy_type,
                "coverage_amount": round(coverage, 2),
                "premium_amount": round(premium, 2),
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
            insurance.append(insurance_record)
        
        return pd.DataFrame(insurance)


class CampaignGenerator(DataGenerator):
    """Generate marketing campaign data"""
    
    CAMPAIGN_TYPES = [
        "Product Launch", "Seasonal Promotion", "Brand Awareness", 
        "Discount Campaign", "Loyalty Program", "Digital Marketing",
        "In-Store Promotion", "Social Media Campaign"
    ]
    
    def generate_campaigns(self, count: int) -> pd.DataFrame:
        """Generate marketing campaign data spanning from 2015 to present"""
        campaigns = []
        
        # Calculate campaigns per year to distribute evenly
        current_year = datetime.now().year
        years_span = current_year - 2015 + 1  # Include current year
        campaigns_per_year = max(1, count // years_span)
        
        campaign_index = 0
        for year in range(2015, current_year + 1):
            year_campaigns = min(campaigns_per_year, count - campaign_index)
            if year_campaigns <= 0:
                break
                
            for i in range(year_campaigns):
                # Start campaigns throughout the year
                start_month = random.randint(1, 12)
                start_day = random.randint(1, 28)  # Avoid month-end issues
                start_date = datetime(year, start_month, start_day).date()
                
                # Duration of 3-5 months (90-150 days)
                duration = random.randint(90, 150)
                end_date = start_date + timedelta(days=duration)
                
                # Determine status based on dates
                today = datetime.now().date()
                if end_date < today:
                    status = random.choice(["Completed", "Cancelled"])
                elif start_date > today:
                    status = "Planned"
                else:
                    status = "Active"
                
                campaign = {
                    "campaign_name": f"Campaign {campaign_index+1}: {random.choice(self.CAMPAIGN_TYPES)}",
                    "campaign_type": random.choice(self.CAMPAIGN_TYPES),
                    "start_date": start_date,
                    "end_date": end_date,
                    "budget": random.uniform(50000, 500000),
                    "target_audience": random.choice(["All Customers", "Young Adults", "Families", "Business Owners"]),
                    "status": status,
                    "created_at": datetime.now(),
                    "updated_at": datetime.now()
                }
                campaigns.append(campaign)
                campaign_index += 1
        
        # Convert to DataFrame and sort by start date
        campaigns_df = pd.DataFrame(campaigns)
        campaigns_df = campaigns_df.sort_values('start_date').reset_index(drop=True)
        
        # Assign IDs in chronological order (Campaign 1 = earliest start)
        for idx, row in campaigns_df.iterrows():
            campaigns_df.at[idx, 'campaign_id'] = id_generator.generate_id('dim_campaigns')
        
        return campaigns_df
