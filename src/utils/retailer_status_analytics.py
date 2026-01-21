"""
Retailer Status Analytics for Historical Analysis
"""

import pandas as pd
from datetime import datetime, date
from typing import Dict, List, Any

class RetailerStatusAnalytics:
    """Analyze retailer status history and active status by specific time periods"""
    
    def __init__(self, bigquery_client):
        self.bigquery_client = bigquery_client
        self.dataset = bigquery_client.dataset
    
    def get_retailers_by_year(self, target_year: int) -> Dict[str, Any]:
        """Get retailer status distribution for a specific year"""
        query = f"""
        WITH retailer_year_status AS (
            SELECT 
                retailer_id,
                retailer_name,
                retailer_type,
                location_id,
                registration_date,
                status_date,
                deactivation_date,
                status,
                CASE 
                    WHEN registration_date <= DATE('{target_year}-12-31') 
                    AND (r.status_date <= DATE('{target_year}-12-31') OR r.status_date IS NULL)
                    AND (r.deactivation_date > DATE('{target_year}-12-31') OR r.deactivation_date IS NULL)
                    AND r.status = 'Active'
                    THEN 'Active_in_Year'
                    WHEN registration_date <= DATE('{target_year}-12-31') 
                    AND status_date <= DATE('{target_year}-12-31')
                    AND deactivation_date <= DATE('{target_year}-12-31')
                    THEN 'Inactive_in_Year'
                    WHEN registration_date <= DATE('{target_year}-12-31') 
                    AND status_date <= DATE('{target_year}-12-31')
                    AND deactivation_date > DATE('{target_year}-12-31')
                    THEN 'Terminated_in_Year'
                    WHEN registration_date > DATE('{target_year}-12-31')
                    THEN 'Not_Registered_Yet'
                    ELSE 'Never_Active'
                END as year_status
            FROM `{self.dataset}.dim_retailers`
        ),
        location_details AS (
            SELECT r.*, l.region, l.province, l.city
            FROM retailer_year_status r
            JOIN `{self.dataset}.dim_locations` l ON r.location_id = l.location_id
        )
        
        SELECT 
            'Summary' as analysis_type,
            year_status,
            COUNT(*) as retailer_count,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM retailer_year_status), 2) as percentage,
            '{target_year}' as analysis_year
        FROM location_details
        GROUP BY year_status
        
        UNION ALL
        
        SELECT 
            'By_Region' as analysis_type,
            region as area_name,
            COUNT(*) as retailer_count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY region), 2) as percentage,
            year_status
        FROM location_details
        WHERE year_status = 'Active_in_Year'
        GROUP BY region, year_status
        
        UNION ALL
        
        SELECT 
            'By_Type' as analysis_type,
            retailer_type as area_name,
            COUNT(*) as retailer_count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY retailer_type), 2) as percentage,
            year_status
        FROM location_details
        WHERE year_status = 'Active_in_Year'
        GROUP BY retailer_type, year_status
        
        ORDER BY analysis_type, retailer_count DESC
        """
        
        results = self.bigquery_client.execute_query(query)
        return results.to_dict('records')
    
    def get_active_retailers_date_range(self, start_date: date, end_date: date) -> Dict[str, Any]:
        """Get retailers who were active during a specific date range"""
        query = f"""
        WITH active_retailers_period AS (
            SELECT 
                r.retailer_id,
                r.retailer_name,
                r.retailer_type,
                r.registration_date,
                r.status_date,
                r.deactivation_date,
                l.region,
                l.province,
                l.city,
                CASE 
                    WHEN r.registration_date <= '{end_date}' 
                    AND (r.status_date <= '{end_date}' OR r.status_date IS NULL)
                    AND (r.deactivation_date > '{start_date}' OR r.deactivation_date IS NULL)
                    AND r.status = 'Active'
                    THEN 'Active_in_Period'
                    ELSE 'Not_Active'
                END as period_status
            FROM `{self.dataset}.dim_retailers` r
            JOIN `{self.dataset}.dim_locations` l ON r.location_id = l.location_id
        )
        
        SELECT 
            period_status,
            COUNT(*) as total_retailers,
            COUNT(DISTINCT region) as regions_covered,
            COUNT(DISTINCT province) as provinces_covered,
            COUNT(DISTINCT city) as cities_covered,
            COUNT(DISTINCT retailer_type) as types_present,
                        '{start_date}' as period_start,
            '{end_date}' as period_end
        FROM active_retailers_period
        GROUP BY period_status
        
        UNION ALL
        
        SELECT 
            CONCAT(region, ' - ', retailer_type) as period_status,
            COUNT(*) as total_retailers,
            1 as regions_covered,
            COUNT(DISTINCT province) as provinces_covered,
            COUNT(DISTINCT city) as cities_covered,
            1 as types_present,
                        '{start_date}' as period_start,
            '{end_date}' as period_end
        FROM active_retailers_period
        WHERE period_status = 'Active_in_Period'
        GROUP BY region, retailer_type
        ORDER BY total_retailers DESC
        """
        
        results = self.bigquery_client.execute_query(query)
        return results.to_dict('records')
    
    def get_retailer_lifecycle_timeline(self, retailer_id: str = None) -> Dict[str, Any]:
        """Get retailer status changes over time"""
        retailer_filter = f"WHERE r.retailer_id = '{retailer_id}'" if retailer_id else ""
        
        query = f"""
        WITH status_timeline AS (
            SELECT 
                r.retailer_id,
                r.retailer_name,
                r.retailer_type,
                r.registration_date,
                r.status_date,
                r.deactivation_date,
                r.status,
                l.region,
                l.province,
                l.city,
                CASE 
                    WHEN r.registration_date IS NOT NULL THEN 'Registered'
                    WHEN r.status_date IS NOT NULL THEN 'Status_Changed'
                    WHEN r.deactivation_date IS NOT NULL THEN 'Terminated'
                END as event_type,
                COALESCE(r.status_date, r.deactivation_date, r.registration_date) as event_date
            FROM `{self.dataset}.dim_retailers` r
            JOIN `{self.dataset}.dim_locations` l ON r.location_id = l.location_id
            {retailer_filter}
        )
        
        SELECT 
            retailer_id,
            retailer_name,
            retailer_type,
            region,
            province,
            city,
            event_type,
            event_date,
            status,
            LAG(event_date) OVER (PARTITION BY retailer_id ORDER BY event_date) as previous_event_date,
            DATEDIFF(COALESCE(event_date, CURRENT_DATE()), 
                    LAG(event_date) OVER (PARTITION BY retailer_id ORDER BY event_date)) as days_since_previous
        FROM status_timeline
        WHERE event_date IS NOT NULL
        ORDER BY retailer_id, event_date
        """
        
        results = self.bigquery_client.execute_query(query)
        return results.to_dict('records')
    
    def get_year_over_year_activation(self, start_year: int, end_year: int) -> Dict[str, Any]:
        """Compare retailer activation rates across years"""
        query = f"""
        WITH yearly_activations AS (
            SELECT 
                EXTRACT(YEAR FROM registration_date) as registration_year,
                COUNT(*) as new_registrations,
                COUNT(DISTINCT location_id) as new_locations,
                COUNT(DISTINCT retailer_type) as types_added
            FROM `{self.dataset}.dim_retailers`
            WHERE EXTRACT(YEAR FROM registration_date) BETWEEN {start_year} AND {end_year}
            GROUP BY EXTRACT(YEAR FROM registration_date)
        ),
        yearly_deactivations AS (
            SELECT 
                EXTRACT(YEAR FROM deactivation_date) as termination_year,
                COUNT(*) as deactivations
            FROM `{self.dataset}.dim_retailers`
            WHERE deactivation_date IS NOT NULL
            AND EXTRACT(YEAR FROM deactivation_date) BETWEEN {start_year} AND {end_year}
            GROUP BY EXTRACT(YEAR FROM deactivation_date)
        )
        
        SELECT 
            COALESCE(registration_year, termination_year) as year,
            new_registrations,
            COALESCE(deactivations, 0) as deactivations,
            new_locations,
            types_added,
            ROUND((new_registrations - COALESCE(deactivations, 0)) * 100.0 / 
                  NULLIF(new_registrations, 0), 2) as net_growth_percentage,
            LAG(new_registrations) OVER (ORDER BY COALESCE(registration_year, termination_year)) as prev_year_registrations,
            ROUND((new_registrations - LAG(new_registrations) OVER (ORDER BY COALESCE(registration_year, termination_year))) * 100.0 / 
                  NULLIF(LAG(new_registrations) OVER (ORDER BY COALESCE(registration_year, termination_year)), 0), 2) as year_over_year_growth
        FROM yearly_activations ya
        LEFT JOIN yearly_deactivations yd ON ya.registration_year = yd.termination_year
        ORDER BY year
        """
        
        results = self.bigquery_client.execute_query(query)
        return results.to_dict('records')
