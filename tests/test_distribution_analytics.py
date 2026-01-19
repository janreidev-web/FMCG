"""
Tests for Distribution Analytics functionality
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from datetime import datetime, date

from src.utils.distribution_analytics import DistributionAnalytics


class TestDistributionAnalytics(unittest.TestCase):
    """Test cases for DistributionAnalytics class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_bigquery_client = Mock()
        self.mock_bigquery_client.dataset = "test_dataset"
        self.analytics = DistributionAnalytics(self.mock_bigquery_client)
    
    def test_init(self):
        """Test DistributionAnalytics initialization"""
        self.assertEqual(self.analytics.dataset, "test_dataset")
        self.assertEqual(self.analytics.bigquery_client, self.mock_bigquery_client)
    
    @patch('src.utils.distribution_analytics.datetime')
    def test_get_distribution_coverage_default_date(self, mock_datetime):
        """Test get_distribution_coverage with default date"""
        mock_datetime.now.return_value = Mock()
        mock_datetime.now.return_value.date.return_value = date(2026, 1, 20)
        
        # Mock query result
        mock_result_df = pd.DataFrame({
            'geographic_level': ['National', 'Regional'],
            'area_name': ['Philippines', 'Region I'],
            'total_locations': [100, 20],
            'covered_locations': [85, 18],
            'coverage_percentage': [85.0, 90.0],
            'total_retailers': [500, 100],
            'active_retailers': [450, 95],
            'suspended_retailers': [50, 5],
            'avg_type_diversity': [3.2, 2.8],
            'analysis_date': ['2026-01-20', '2026-01-20']
        })
        
        self.mock_bigquery_client.execute_query.return_value = mock_result_df
        
        result = self.analytics.get_distribution_coverage()
        
        # Verify query was called with correct date
        call_args = self.mock_bigquery_client.execute_query.call_args[0][0]
        self.assertIn("2026-01-20", call_args)
        
        # Verify result format
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['coverage_percentage'], 85.0)
    
    def test_get_distribution_coverage_custom_date(self):
        """Test get_distribution_coverage with custom date"""
        custom_date = date(2025, 12, 31)
        
        mock_result_df = pd.DataFrame({
            'geographic_level': ['National'],
            'area_name': ['Philippines'],
            'total_locations': [100],
            'covered_locations': [85],
            'coverage_percentage': [85.0],
            'total_retailers': [500],
            'active_retailers': [450],
            'suspended_retailers': [50],
            'avg_type_diversity': [3.2],
            'analysis_date': ['2025-12-31']
        })
        
        self.mock_bigquery_client.execute_query.return_value = mock_result_df
        
        result = self.analytics.get_distribution_coverage(custom_date)
        
        # Verify query was called with custom date
        call_args = self.mock_bigquery_client.execute_query.call_args[0][0]
        self.assertIn("2025-12-31", call_args)
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['analysis_date'], '2025-12-31')
    
    def test_get_retailer_type_distribution(self):
        """Test retailer type distribution analysis"""
        mock_result_df = pd.DataFrame({
            'retailer_type': ['Sari-Sari Store', 'Supermarket'],
            'region': ['Region I', 'Region I'],
            'total_count': [80, 20],
            'market_share_percentage': [16.0, 4.0],
            'province_presence': [5, 2],
            'analysis_date': ['2026-01-20', '2026-01-20']
        })
        
        self.mock_bigquery_client.execute_query.return_value = mock_result_df
        
        result = self.analytics.get_retailer_type_distribution()
        
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['retailer_type'], 'Sari-Sari Store')
        self.assertEqual(result[0]['market_share_percentage'], 16.0)
    
    def test_get_coverage_trends(self):
        """Test coverage trends analysis"""
        start_date = date(2025, 1, 1)
        end_date = date(2025, 12, 31)
        
        mock_result_df = pd.DataFrame({
            'coverage_month': ['2025-01-01', '2025-02-01'],
            'cumulative_locations': [50, 60],
            'cumulative_retailers': [200, 250],
            'active_retailers': [180, 230],
            'activation_rate': [90.0, 92.0],
            'prev_locations': [None, 50],
            'prev_retailers': [None, 200]
        })
        
        self.mock_bigquery_client.execute_query.return_value = mock_result_df
        
        result = self.analytics.get_coverage_trends(start_date, end_date)
        
        # Verify query parameters
        call_args = self.mock_bigquery_client.execute_query.call_args[0][0]
        self.assertIn("BETWEEN '2025-01-01' AND '2025-12-31'", call_args)
        
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['activation_rate'], 90.0)
        self.assertEqual(result[1]['prev_locations'], 50)
    
    def test_get_market_penetration_metrics(self):
        """Test market penetration metrics"""
        mock_result_df = pd.DataFrame({
            'region': ['Region I', 'Region II'],
            'retailer_type': ['Sari-Sari Store', 'Supermarket'],
            'regional_potential': [100, 50],
            'regional_actual': [75, 40],
            'regional_penetration': [75.0, 80.0],
            'provinces_covered': [5, 3],
            'analysis_date': ['2026-01-20', '2026-01-20']
        })
        
        self.mock_bigquery_client.execute_query.return_value = mock_result_df
        
        result = self.analytics.get_market_penetration_metrics()
        
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['regional_penetration'], 75.0)
        self.assertEqual(result[1]['provinces_covered'], 3)


class TestDistributionAnalyticsIntegration(unittest.TestCase):
    """Integration tests for DistributionAnalytics with realistic data"""
    
    def setUp(self):
        """Set up integration test fixtures"""
        self.mock_bigquery_client = Mock()
        self.mock_bigquery_client.dataset = "fmcg_warehouse"
        self.analytics = DistributionAnalytics(self.mock_bigquery_client)
    
    def test_full_coverage_analysis_workflow(self):
        """Test complete coverage analysis workflow"""
        # Mock realistic data for coverage analysis
        coverage_data = pd.DataFrame({
            'geographic_level': ['National', 'Regional', 'Regional'],
            'area_name': ['Philippines', 'Region I - Ilocos', 'Region II - Cagayan Valley'],
            'total_locations': [81, 16, 5],
            'covered_locations': [65, 14, 4],
            'coverage_percentage': [80.25, 87.5, 80.0],
            'total_retailers': [400, 80, 25],
            'active_retailers': [360, 72, 22],
            'suspended_retailers': [40, 8, 3],
            'avg_type_diversity': [3.1, 3.2, 2.8],
            'analysis_date': ['2026-01-20'] * 3
        })
        
        self.mock_bigquery_client.execute_query.return_value = coverage_data
        
        # Execute coverage analysis
        coverage_result = self.analytics.get_distribution_coverage()
        
        # Verify national level data
        national_data = next(item for item in coverage_result if item['geographic_level'] == 'National')
        self.assertEqual(national_data['coverage_percentage'], 80.25)
        self.assertEqual(national_data['total_retailers'], 400)
        
        # Verify regional data exists
        regional_data = [item for item in coverage_result if item['geographic_level'] == 'Regional']
        self.assertEqual(len(regional_data), 2)
        
        # Verify coverage calculation logic
        for region in regional_data:
            expected_coverage = region['covered_locations'] * 100.0 / region['total_locations']
            self.assertAlmostEqual(region['coverage_percentage'], expected_coverage, places=2)
    
    def test_retailer_type_distribution_analysis(self):
        """Test retailer type distribution with realistic data"""
        retailer_data = pd.DataFrame({
            'retailer_type': ['Sari-Sari Store', 'Supermarket', 'Convenience Store'],
            'region': ['Region I', 'Region I', 'Region I'],
            'total_count': [50, 15, 10],
            'market_share_percentage': [12.5, 3.75, 2.5],
            'province_presence': [4, 2, 3],
            'analysis_date': ['2026-01-20'] * 3
        })
        
        self.mock_bigquery_client.execute_query.return_value = retailer_data
        
        result = self.analytics.get_retailer_type_distribution()
        
        # Verify Sari-Sari Store dominance
        sari_sari_data = next(item for item in result if item['retailer_type'] == 'Sari-Sari Store')
        self.assertEqual(sari_sari_data['total_count'], 50)
        self.assertEqual(sari_sari_data['province_presence'], 4)
        
        # Verify market share calculation
        total_retailers = 400  # Based on mock setup
        for item in result:
            expected_share = item['total_count'] * 100.0 / total_retailers
            self.assertAlmostEqual(item['market_share_percentage'], expected_share, places=2)


if __name__ == '__main__':
    unittest.main()
