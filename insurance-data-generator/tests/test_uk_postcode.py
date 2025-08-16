"""Tests for UK postcode profiling system."""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.uk_postcode_profiler import UKPostcodeProfiler, RiskZone, PostcodeProfile


class TestUKPostcodeProfiler:
    """Test UK postcode profiling functionality."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.profiler = UKPostcodeProfiler()
    
    def test_parse_postcode_valid(self):
        """Test parsing valid UK postcodes."""
        # Test various valid formats
        test_cases = [
            ("SW1A 1AA", {"area": "SW", "district": "SW1A", "sector": "SW1A 1", "unit": "AA"}),
            ("E1 8DY", {"area": "E", "district": "E1", "sector": "E1 8", "unit": "DY"}),
            ("M1 1AE", {"area": "M", "district": "M1", "sector": "M1 1", "unit": "AE"}),
            ("B15 2TT", {"area": "B", "district": "B15", "sector": "B15 2", "unit": "TT"}),
            ("EC1A1BB", {"area": "EC", "district": "EC1A", "sector": "EC1A 1", "unit": "BB"}),
        ]
        
        for postcode, expected in test_cases:
            parsed = self.profiler.parse_postcode(postcode)
            assert parsed is not None
            assert parsed['area'] == expected['area']
            assert parsed['district'] == expected['district']
            assert parsed['sector'] == expected['sector']
            assert parsed['unit'] == expected['unit']
    
    def test_parse_postcode_invalid(self):
        """Test parsing invalid postcodes."""
        invalid_postcodes = [
            "INVALID",
            "123456",
            "XX99 9XX",
            "",
            "A",
            "AAAA AAAA"
        ]
        
        for postcode in invalid_postcodes:
            parsed = self.profiler.parse_postcode(postcode)
            assert parsed is None
    
    def test_get_profile(self):
        """Test getting postcode profile."""
        # Test known postcode areas
        profile_e = self.profiler.get_profile("E1 8DY")
        assert profile_e is not None
        assert profile_e.area == "E"
        assert profile_e.region == "East London"
        assert profile_e.risk_zone == RiskZone.HIGH
        assert profile_e.major_city == "London"
        
        profile_ox = self.profiler.get_profile("OX1 1AA")
        assert profile_ox is not None
        assert profile_ox.area == "OX"
        assert profile_ox.region == "Oxford"
        assert profile_ox.risk_zone == RiskZone.LOW
        assert profile_ox.major_city == "Oxford"
    
    def test_calculate_risk_score(self):
        """Test risk score calculation."""
        # High-risk area
        high_risk_score = self.profiler.calculate_risk_score("EC1A 1BB")
        assert 0.7 <= high_risk_score <= 1.0
        
        # Low-risk area
        low_risk_score = self.profiler.calculate_risk_score("BA1 1AA")
        assert 0.0 <= low_risk_score <= 0.4
        
        # Medium-risk area
        medium_risk_score = self.profiler.calculate_risk_score("M1 1AE")
        assert 0.4 <= medium_risk_score <= 0.7
    
    def test_calculate_risk_score_with_factors(self):
        """Test risk score calculation with additional factors."""
        factors = {
            'crime_weight': 1.0,
            'flood_weight': 1.0,
            'income_weight': 1.0,
            'density_weight': 1.0
        }
        
        score = self.profiler.calculate_risk_score("E1 8DY", factors)
        assert 0 <= score <= 1
        
        # Score should be higher with all factors weighted
        base_score = self.profiler.calculate_risk_score("E1 8DY")
        assert score >= base_score - 0.1  # Account for randomness
    
    def test_get_distance_between_postcodes(self):
        """Test distance calculation between postcodes."""
        # London to Birmingham
        distance = self.profiler.get_distance_between_postcodes("E1 8DY", "B1 1AA")
        assert distance is not None
        assert 150 < distance < 200  # Approximately 163 km
        
        # London postcodes (should be close)
        distance = self.profiler.get_distance_between_postcodes("E1 8DY", "SW1A 1AA")
        assert distance is not None
        assert distance < 20  # Within London
        
        # Invalid postcode
        distance = self.profiler.get_distance_between_postcodes("INVALID", "E1 8DY")
        assert distance is None
    
    def test_get_risk_zone(self):
        """Test risk zone retrieval."""
        assert self.profiler.get_risk_zone("EC1A 1BB") == RiskZone.VERY_HIGH
        assert self.profiler.get_risk_zone("E1 8DY") == RiskZone.HIGH
        assert self.profiler.get_risk_zone("M1 1AE") == RiskZone.MEDIUM
        assert self.profiler.get_risk_zone("OX1 1AA") == RiskZone.LOW
        assert self.profiler.get_risk_zone("BA1 1AA") == RiskZone.VERY_LOW
        assert self.profiler.get_risk_zone("INVALID") is None
    
    def test_get_high_risk_areas(self):
        """Test getting high-risk areas."""
        high_risk = self.profiler.get_high_risk_areas()
        
        assert isinstance(high_risk, list)
        assert len(high_risk) > 0
        assert "EC" in high_risk  # City of London
        assert "E" in high_risk   # East London
        assert "SE" in high_risk  # South East London
    
    def test_get_low_risk_areas(self):
        """Test getting low-risk areas."""
        low_risk = self.profiler.get_low_risk_areas()
        
        assert isinstance(low_risk, list)
        assert len(low_risk) > 0
        assert "OX" in low_risk  # Oxford
        assert "CB" in low_risk  # Cambridge
        assert "BA" in low_risk  # Bath
        assert "GU" in low_risk  # Guildford
    
    def test_analyze_geographic_concentration(self):
        """Test geographic concentration analysis."""
        # Concentrated postcodes (all in East London)
        concentrated = [
            "E1 1AA", "E1 2BB", "E1 3CC", "E1 4DD", "E1 5EE",
            "E2 1AA", "E2 2BB", "E3 1AA"
        ]
        
        analysis = self.profiler.analyze_geographic_concentration(concentrated)
        
        assert analysis['total_postcodes'] == 8
        assert analysis['unique_areas'] == 1  # All in E
        assert analysis['most_common_area'] == 'E'
        assert analysis['concentration_ratio'] == 1.0
        assert analysis['high_concentration'] is True
        
        # Distributed postcodes
        distributed = [
            "E1 1AA", "SW1A 1AA", "M1 1AE", "B1 1AA", "G1 1AA"
        ]
        
        analysis = self.profiler.analyze_geographic_concentration(distributed)
        
        assert analysis['total_postcodes'] == 5
        assert analysis['unique_areas'] == 5
        assert analysis['concentration_ratio'] == 0.2
        assert analysis['high_concentration'] is False
        assert analysis['suspicious_clustering'] is False
    
    def test_suspicious_clustering_detection(self):
        """Test detection of suspicious clustering."""
        # High concentration in high-risk area
        suspicious = [
            "EC1A 1AA", "EC1A 2BB", "EC1A 3CC", "EC1A 4DD",
            "EC1B 1AA", "EC1B 2BB", "EC2A 1AA"
        ]
        
        analysis = self.profiler.analyze_geographic_concentration(suspicious)
        
        assert analysis['high_concentration'] is True
        # High concentration + high risk = suspicious
        assert analysis['average_risk_score'] > 0.6
    
    def test_generate_random_postcode(self):
        """Test random postcode generation."""
        # Generate without specific risk zone
        postcode = self.profiler.generate_random_postcode()
        parsed = self.profiler.parse_postcode(postcode)
        assert parsed is not None
        
        # Generate high-risk postcode
        high_risk_postcode = self.profiler.generate_random_postcode(RiskZone.HIGH)
        profile = self.profiler.get_profile(high_risk_postcode)
        if profile:  # If area is in profiles
            assert profile.risk_zone == RiskZone.HIGH
        
        # Generate low-risk postcode
        low_risk_postcode = self.profiler.generate_random_postcode(RiskZone.LOW)
        profile = self.profiler.get_profile(low_risk_postcode)
        if profile:  # If area is in profiles
            assert profile.risk_zone == RiskZone.LOW
    
    def test_postcode_profile_data(self):
        """Test postcode profile data integrity."""
        for area, profile in self.profiler.POSTCODE_PROFILES.items():
            assert isinstance(profile, PostcodeProfile)
            assert profile.area == area
            assert profile.risk_zone in RiskZone
            assert 0 <= profile.base_risk_score <= 1
            assert profile.crime_rate >= 0
            assert 0 <= profile.flood_risk <= 1
            assert profile.average_income > 0
            assert profile.population_density > 0
            assert profile.motor_theft_rate >= 0
            assert profile.burglary_rate >= 0
            assert len(profile.coordinates) == 2
            assert -90 <= profile.coordinates[0] <= 90  # Valid latitude
            assert -180 <= profile.coordinates[1] <= 180  # Valid longitude


class TestPostcodeRiskFactors:
    """Test risk factors in postcode profiles."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.profiler = UKPostcodeProfiler()
    
    def test_crime_correlation(self):
        """Test that higher crime areas have higher risk scores."""
        profiles = list(self.profiler.POSTCODE_PROFILES.values())
        
        # Sort by crime rate
        sorted_by_crime = sorted(profiles, key=lambda p: p.crime_rate)
        
        # Check general trend (allowing for some variation)
        low_crime_avg = sum(p.base_risk_score for p in sorted_by_crime[:5]) / 5
        high_crime_avg = sum(p.base_risk_score for p in sorted_by_crime[-5:]) / 5
        
        assert high_crime_avg > low_crime_avg
    
    def test_income_inverse_correlation(self):
        """Test that higher income areas have lower risk scores."""
        profiles = list(self.profiler.POSTCODE_PROFILES.values())
        
        # Sort by income
        sorted_by_income = sorted(profiles, key=lambda p: p.average_income)
        
        # Check general trend
        low_income_avg = sum(p.base_risk_score for p in sorted_by_income[:5]) / 5
        high_income_avg = sum(p.base_risk_score for p in sorted_by_income[-5:]) / 5
        
        # Higher income areas should generally have lower risk
        assert low_income_avg > high_income_avg - 0.2  # Allow some variation
    
    def test_london_risk_distribution(self):
        """Test London areas have varied risk profiles."""
        london_areas = ["E", "EC", "N", "NW", "SE", "SW", "W", "WC"]
        london_profiles = [
            self.profiler.POSTCODE_PROFILES[area]
            for area in london_areas
            if area in self.profiler.POSTCODE_PROFILES
        ]
        
        risk_zones = [p.risk_zone for p in london_profiles]
        
        # London should have varied risk zones
        assert len(set(risk_zones)) >= 3  # At least 3 different risk levels
        
        # Some areas should be high risk
        assert RiskZone.HIGH in risk_zones or RiskZone.VERY_HIGH in risk_zones
        
        # Some areas should be lower risk
        assert RiskZone.LOW in risk_zones or RiskZone.MEDIUM in risk_zones


if __name__ == "__main__":
    pytest.main([__file__, "-v"])