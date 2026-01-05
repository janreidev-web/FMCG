"""
ID generation utilities for FMCG Data Analytics Platform
"""

import random
from typing import Dict


class IDGenerator:
    """Generates consistent IDs with table prefixes"""
    
    def __init__(self):
        self.counters: Dict[str, int] = {}
    
    def generate_id(self, table_name: str) -> str:
        """
        Generate ID with format: {table_prefix}{15_digit_number}
        Examples: EMP000000000000001, RET000000000000001, PRO000000000000001
        """
        # Map table names to meaningful prefixes
        prefix_mapping = {
            'dim_employees': 'EMP',
            'dim_retailers': 'RET', 
            'dim_products': 'PRO',
            'dim_locations': 'LOC',
            'dim_departments': 'DEP',
            'dim_jobs': 'JOB',
            'dim_campaigns': 'CAM',
            'dim_categories': 'CAT',
            'dim_subcategories': 'SUB',
            'dim_brands': 'BRD',
            'fact_sales': 'SAL',
            'fact_inventory': 'INV',
            'fact_operating_costs': 'COS',
            'fact_marketing_costs': 'MAR'
        }
        
        # Use mapped prefix or default to first 3 letters
        table_prefix = prefix_mapping.get(table_name, table_name.replace('_', '').upper()[:3])
        
        # Get or initialize counter for this table
        if table_name not in self.counters:
            self.counters[table_name] = 1
        else:
            self.counters[table_name] += 1
        
        # Generate 15-digit number with leading zeros
        number_str = str(self.counters[table_name]).zfill(15)
        
        return f"{table_prefix}{number_str}"
    
    def get_next_id(self, table_name: str) -> int:
        """Get the next ID number for a table without prefix"""
        if table_name not in self.counters:
            self.counters[table_name] = 0
        self.counters[table_name] += 1
        return self.counters[table_name]
    
    def reset_counter(self, table_name: str) -> None:
        """Reset counter for a specific table"""
        self.counters[table_name] = 0
    
    def reset_all_counters(self) -> None:
        """Reset all counters"""
        self.counters.clear()


# Global ID generator instance
id_generator = IDGenerator()
