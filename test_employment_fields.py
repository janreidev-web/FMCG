#!/usr/bin/env python3
"""Test script for employment type and work setup fields"""

import pandas as pd
from src.etl.pipeline import ETLPipeline

# Create a mock BigQueryManager for testing
class MockBigQueryManager:
    def __init__(self, project_id, dataset, credentials_path=None):
        self.project_id = project_id
        self.dataset = dataset
        self.credentials_path = credentials_path
    
    def ensure_dataset(self):
        pass
    
    def create_table(self, table_name, schema):
        pass
    
    def load_dataframe(self, df, table_name):
        pass

def main():
    # Test the pipeline
    mock_bq = MockBigQueryManager('test-project', 'test-dataset')
    pipeline = ETLPipeline(bq_manager=mock_bq)

    # Test dimension data generation
    config = {'locations_count': 5, 'initial_employees': 20, 'initial_products': 5, 'initial_retailers': 10, 'initial_campaigns': 3}
    pipeline.generate_dimension_data(config)

    print('=== EMPLOYMENT TYPE & WORK SETUP TEST ===')
    employees_df = pipeline.data_cache['dim_employees']
    
    print(f'Total employees: {len(employees_df)}')
    print(f'Employee columns: {list(employees_df.columns)}')
    
    print('\n=== EMPLOYMENT TYPE DISTRIBUTION ===')
    emp_type_counts = employees_df['employment_type'].value_counts()
    for emp_type, count in emp_type_counts.items():
        print(f'{emp_type}: {count} ({count/len(employees_df)*100:.1f}%)')
    
    print('\n=== WORK SETUP DISTRIBUTION ===')
    work_setup_counts = employees_df['work_setup'].value_counts()
    for work_setup, count in work_setup_counts.items():
        print(f'{work_setup}: {count} ({count/len(employees_df)*100:.1f}%)')
    
    print('\n=== EMPLOYMENT TYPE BY JOB TITLE ===')
    # Show some examples of job titles and their employment types
    sample_employees = employees_df[['job_id', 'employment_type', 'work_setup']].head(10)
    jobs_df = pipeline.data_cache['dim_jobs']
    
    for _, emp in sample_employees.iterrows():
        job_info = jobs_df[jobs_df['job_id'] == emp['job_id']]
        if len(job_info) > 0:
            job_title = job_info.iloc[0]['job_title']
            print(f'{job_title}: {emp["employment_type"]} - {emp["work_setup"]}')
    
    print('\n=== FACT EMPLOYEES COMPENSATION BY EMPLOYMENT TYPE ===')
    # Test fact generation with a small sample
    pipeline.generate_fact_data(config)
    facts_df = pipeline.data_cache['fact_employees']
    
    if len(facts_df) > 0:
        # Join with employee data to see compensation by employment type
        facts_with_emp = facts_df.merge(employees_df[['employee_id', 'employment_type', 'work_setup']], 
                                       on='employee_id', how='left')
        
        print(f'Total fact records: {len(facts_with_emp)}')
        
        # Average compensation by employment type
        avg_by_emp_type = facts_with_emp.groupby('employment_type')['total_compensation'].mean()
        print('\nAverage Total Compensation by Employment Type:')
        for emp_type, avg_comp in avg_by_emp_type.items():
            print(f'{emp_type}: ₱{avg_comp:,.0f}')
        
        # Average compensation by work setup
        avg_by_work_setup = facts_with_emp.groupby('work_setup')['total_compensation'].mean()
        print('\nAverage Total Compensation by Work Setup:')
        for work_setup, avg_comp in avg_by_work_setup.items():
            print(f'{work_setup}: ₱{avg_comp:,.0f}')

if __name__ == '__main__':
    main()
