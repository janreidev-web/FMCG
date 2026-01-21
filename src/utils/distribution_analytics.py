"""
Distribution Coverage Analytics for FMCG Platform
"""

import pandas as pd
from datetime import datetime, date
from typing import Dict, List, Tuple, Any

class DistributionAnalytics:
    """Compute distribution coverage metrics across geographic dimensions"""
    
    def __init__(self, bigquery_client):
        self.bigquery_client = bigquery_client
        self.dataset = bigquery_client.dataset
    
    def get_distribution_coverage(self, analysis_date: date = None) -> Dict[str, Any]:
        """Calculate comprehensive distribution coverage metrics"""
        if analysis_date is None:
            analysis_date = datetime.now().date()
        
        query = f"""
        WITH 
        active_locations AS (
            SELECT DISTINCT location_id, region, province, city
            FROM `{self.dataset}.dim_locations`
        ),
        
        active_retailers AS (
            SELECT r.location_id, r.retailer_type, r.status, r.registration_date,
                   l.region, l.province, l.city
            FROM `{self.dataset}.dim_retailers` r
            JOIN `{self.dataset}.dim_locations` l ON r.location_id = l.location_id
            WHERE r.status = 'Active'
            AND r.registration_date <= '{analysis_date}'
        ),
        
        location_coverage AS (
            SELECT 
                al.region,
                al.province,
                al.city,
                COUNT(ar.location_id) as retailer_count,
                COUNT(DISTINCT ar.retailer_type) as type_diversity,
                SUM(CASE WHEN ar.status = 'Active' THEN 1 ELSE 0 END) as active_count
            FROM active_locations al
            LEFT JOIN active_retailers ar ON al.location_id = ar.location_id
            GROUP BY al.region, al.province, al.city
        ),
        
        regional_metrics AS (
            SELECT 
                region,
                COUNT(*) as total_locations,
                SUM(CASE WHEN retailer_count > 0 THEN 1 ELSE 0 END) as covered_locations,
                SUM(retailer_count) as total_retailers,
                SUM(active_count) as active_retailers,
                SUM(CASE WHEN retailer_count = 0 THEN 1 ELSE 0 END) as uncovered_locations,
                AVG(CASE WHEN retailer_count > 0 THEN type_diversity ELSE 0 END) as avg_type_diversity,
                SUM(CASE WHEN retailer_count > 0 THEN type_diversity ELSE 0 END) as total_type_diversity
            FROM location_coverage
            GROUP BY region
        )
        
        SELECT 
            'National' as geographic_level,
            'Philippines' as area_name,
            SUM(total_locations) as total_locations,
            SUM(covered_locations) as covered_locations,
            ROUND(SUM(covered_locations) * 100.0 / SUM(total_locations), 2) as coverage_percentage,
            SUM(total_retailers) as total_retailers,
            SUM(active_retailers) as active_retailers,
            ROUND(AVG(avg_type_diversity), 2) as avg_type_diversity,
            '{analysis_date}' as analysis_date
        FROM regional_metrics
        
        UNION ALL
        
        SELECT 
            'Regional' as geographic_level,
            region as area_name,
            total_locations,
            covered_locations,
            ROUND(covered_locations * 100.0 / total_locations, 2) as coverage_percentage,
            total_retailers,
            active_retailers,
            ROUND(avg_type_diversity, 2) as avg_type_diversity,
            '{analysis_date}' as analysis_date
        FROM regional_metrics
        ORDER BY geographic_level, coverage_percentage DESC
        """
        
        results = self.bigquery_client.execute_query(query)
        return results.to_dict('records')
    
    def get_retailer_type_distribution(self, analysis_date: date = None) -> Dict[str, Any]:
        """Get retailer type distribution by region"""
        if analysis_date is None:
            analysis_date = datetime.now().date()
        
        query = f"""
        WITH active_retailers AS (
            SELECT r.retailer_type, l.region, l.province,
                   COUNT(*) as retailer_count
            FROM `{self.dataset}.dim_retailers` r
            JOIN `{self.dataset}.dim_locations` l ON r.location_id = l.location_id
            WHERE r.status = 'Active'
            AND r.registration_date <= '{analysis_date}'
            GROUP BY r.retailer_type, l.region, l.province
        )
        
        SELECT 
            retailer_type,
            region,
            SUM(retailer_count) as total_count,
            ROUND(SUM(retailer_count) * 100.0 / 
                  (SELECT COUNT(*) FROM `{self.dataset}.dim_retailers` 
                   WHERE status = 'Active' 
                   AND registration_date <= '{analysis_date}'), 2) as market_share_percentage,
            COUNT(DISTINCT province) as province_presence,
            '{analysis_date}' as analysis_date
        FROM active_retailers
        GROUP BY retailer_type, region
        ORDER BY retailer_type, market_share_percentage DESC
        """
        
        results = self.bigquery_client.execute_query(query)
        return results.to_dict('records')
    
    def get_coverage_trends(self, start_date: date, end_date: date) -> Dict[str, Any]:
        """Analyze distribution coverage trends over time"""
        query = f"""
        WITH monthly_coverage AS (
            SELECT 
                DATE_TRUNC(DATE_ADD(registration_date, INTERVAL 30 DAY), MONTH) as coverage_month,
                COUNT(DISTINCT location_id) as cumulative_locations,
                COUNT(*) as cumulative_retailers,
                SUM(CASE WHEN status = 'Active' THEN 1 ELSE 0 END) as active_retailers
            FROM `{self.dataset}.dim_retailers`
            WHERE registration_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY DATE_TRUNC(DATE_ADD(registration_date, INTERVAL 30 DAY), MONTH)
            ORDER BY coverage_month
        )
        
        SELECT 
            coverage_month,
            cumulative_locations,
            cumulative_retailers,
            active_retailers,
            ROUND(active_retailers * 100.0 / cumulative_retailers, 2) as activation_rate,
            LAG(cumulative_locations) OVER (ORDER BY coverage_month) as prev_locations,
            LAG(cumulative_retailers) OVER (ORDER BY coverage_month) as prev_retailers
        FROM monthly_coverage
        ORDER BY coverage_month
        """
        
        results = self.bigquery_client.execute_query(query)
        return results.to_dict('records')
    
    def get_market_penetration_metrics(self, analysis_date: date = None) -> Dict[str, Any]:
        """Calculate market penetration by retailer type and geography"""
        if analysis_date is None:
            analysis_date = datetime.now().date()
        
        query = f"""
        WITH location_potential AS (
            SELECT 
                region,
                province,
                city,
                retailer_type,
                CASE 
                    WHEN retailer_type IN ('Supermarket', 'Department Store') THEN 1
                    WHEN retailer_type IN ('Convenience Store', 'Pharmacy') THEN 3
                    WHEN retailer_type = 'Wholesale' THEN 2
                    WHEN retailer_type = 'Sari-Sari Store' THEN 10
                    ELSE 2
                END as potential_capacity
            FROM `{self.dataset}.dim_locations` l
            CROSS JOIN (
                SELECT DISTINCT retailer_type 
                FROM `{self.dataset}.dim_retailers` 
                WHERE status = 'Active'
            ) rt
        ),
        
        actual_presence AS (
            SELECT 
                l.region,
                l.province,
                l.city,
                r.retailer_type,
                COUNT(*) as actual_count
            FROM `{self.dataset}.dim_retailers` r
            JOIN `{self.dataset}.dim_locations` l ON r.location_id = l.location_id
            WHERE r.status = 'Active'
            AND r.registration_date <= '{analysis_date}'
            GROUP BY l.region, l.province, l.city, r.retailer_type
        ),
        
        penetration_analysis AS (
            SELECT 
                lp.region,
                lp.province,
                lp.retailer_type,
                SUM(lp.potential_capacity) as total_potential,
                COALESCE(SUM(ap.actual_count), 0) as actual_presence,
                ROUND(COALESCE(SUM(ap.actual_count), 0) * 100.0 / 
                      SUM(lp.potential_capacity), 2) as penetration_rate
            FROM location_potential lp
            LEFT JOIN actual_presence ap ON lp.region = ap.region 
                                        AND lp.province = ap.province 
                                        AND lp.city = ap.city 
                                        AND lp.retailer_type = ap.retailer_type
            GROUP BY lp.region, lp.province, lp.retailer_type
        )
        
        SELECT 
            region,
            retailer_type,
            SUM(total_potential) as regional_potential,
            SUM(actual_presence) as regional_actual,
            ROUND(SUM(actual_presence) * 100.0 / SUM(total_potential), 2) as regional_penetration,
            COUNT(DISTINCT province) as provinces_covered,
            '{analysis_date}' as analysis_date
        FROM penetration_analysis
        GROUP BY region, retailer_type
        ORDER BY region, regional_penetration DESC
        """
        
        results = self.bigquery_client.execute_query(query)
        return results.to_dict('records')
