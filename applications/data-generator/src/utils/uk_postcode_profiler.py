"""UK postcode risk profiling and geographic analysis."""

import re
import json
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass
from enum import Enum
import random
from geopy.distance import geodesic


class RiskZone(Enum):
    """Risk zone classifications."""
    VERY_LOW = "very_low"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    VERY_HIGH = "very_high"


@dataclass
class PostcodeProfile:
    """Profile data for a UK postcode area."""
    area: str
    region: str
    risk_zone: RiskZone
    base_risk_score: float
    crime_rate: float  # Per 1000 population
    flood_risk: float  # 0-1 scale
    average_income: float  # Annual in GBP
    population_density: float  # Per sq km
    motor_theft_rate: float  # Per 1000 vehicles
    burglary_rate: float  # Per 1000 households
    coordinates: Tuple[float, float]  # (lat, lon)
    major_city: Optional[str]


class UKPostcodeProfiler:
    """Comprehensive UK postcode risk profiling system."""
    
    # Detailed postcode area profiles (realistic approximations)
    POSTCODE_PROFILES = {
        # London Areas
        "E": PostcodeProfile(
            area="E", region="East London", risk_zone=RiskZone.HIGH,
            base_risk_score=0.7, crime_rate=95, flood_risk=0.4,
            average_income=35000, population_density=5500,
            motor_theft_rate=12, burglary_rate=8,
            coordinates=(51.5423, 0.0098), major_city="London"
        ),
        "EC": PostcodeProfile(
            area="EC", region="City of London", risk_zone=RiskZone.VERY_HIGH,
            base_risk_score=0.8, crime_rate=110, flood_risk=0.3,
            average_income=75000, population_density=3000,
            motor_theft_rate=8, burglary_rate=5,
            coordinates=(51.5155, -0.0922), major_city="London"
        ),
        "N": PostcodeProfile(
            area="N", region="North London", risk_zone=RiskZone.MEDIUM,
            base_risk_score=0.6, crime_rate=85, flood_risk=0.2,
            average_income=45000, population_density=7000,
            motor_theft_rate=10, burglary_rate=7,
            coordinates=(51.5654, -0.1062), major_city="London"
        ),
        "NW": PostcodeProfile(
            area="NW", region="North West London", risk_zone=RiskZone.MEDIUM,
            base_risk_score=0.6, crime_rate=80, flood_risk=0.2,
            average_income=48000, population_density=6500,
            motor_theft_rate=9, burglary_rate=6,
            coordinates=(51.5496, -0.1997), major_city="London"
        ),
        "SE": PostcodeProfile(
            area="SE", region="South East London", risk_zone=RiskZone.HIGH,
            base_risk_score=0.65, crime_rate=90, flood_risk=0.35,
            average_income=38000, population_density=5000,
            motor_theft_rate=11, burglary_rate=8,
            coordinates=(51.4759, 0.0585), major_city="London"
        ),
        "SW": PostcodeProfile(
            area="SW", region="South West London", risk_zone=RiskZone.LOW,
            base_risk_score=0.4, crime_rate=55, flood_risk=0.25,
            average_income=65000, population_density=4500,
            motor_theft_rate=5, burglary_rate=4,
            coordinates=(51.4333, -0.1885), major_city="London"
        ),
        "W": PostcodeProfile(
            area="W", region="West London", risk_zone=RiskZone.MEDIUM,
            base_risk_score=0.5, crime_rate=70, flood_risk=0.2,
            average_income=55000, population_density=8000,
            motor_theft_rate=7, burglary_rate=5,
            coordinates=(51.5136, -0.2209), major_city="London"
        ),
        "WC": PostcodeProfile(
            area="WC", region="Central London", risk_zone=RiskZone.HIGH,
            base_risk_score=0.7, crime_rate=100, flood_risk=0.2,
            average_income=60000, population_density=10000,
            motor_theft_rate=6, burglary_rate=5,
            coordinates=(51.5185, -0.1188), major_city="London"
        ),
        
        # Major Cities
        "B": PostcodeProfile(
            area="B", region="Birmingham", risk_zone=RiskZone.MEDIUM,
            base_risk_score=0.55, crime_rate=75, flood_risk=0.15,
            average_income=32000, population_density=4200,
            motor_theft_rate=9, burglary_rate=7,
            coordinates=(52.4862, -1.8904), major_city="Birmingham"
        ),
        "M": PostcodeProfile(
            area="M", region="Manchester", risk_zone=RiskZone.MEDIUM,
            base_risk_score=0.6, crime_rate=80, flood_risk=0.25,
            average_income=34000, population_density=4000,
            motor_theft_rate=10, burglary_rate=7,
            coordinates=(53.4808, -2.2426), major_city="Manchester"
        ),
        "L": PostcodeProfile(
            area="L", region="Liverpool", risk_zone=RiskZone.MEDIUM,
            base_risk_score=0.6, crime_rate=78, flood_risk=0.3,
            average_income=30000, population_density=3500,
            motor_theft_rate=11, burglary_rate=8,
            coordinates=(53.4084, -2.9916), major_city="Liverpool"
        ),
        "G": PostcodeProfile(
            area="G", region="Glasgow", risk_zone=RiskZone.MEDIUM,
            base_risk_score=0.5, crime_rate=70, flood_risk=0.2,
            average_income=32000, population_density=3400,
            motor_theft_rate=8, burglary_rate=6,
            coordinates=(55.8642, -4.2518), major_city="Glasgow"
        ),
        "EH": PostcodeProfile(
            area="EH", region="Edinburgh", risk_zone=RiskZone.LOW,
            base_risk_score=0.4, crime_rate=50, flood_risk=0.15,
            average_income=42000, population_density=1900,
            motor_theft_rate=4, burglary_rate=3,
            coordinates=(55.9533, -3.1883), major_city="Edinburgh"
        ),
        "CF": PostcodeProfile(
            area="CF", region="Cardiff", risk_zone=RiskZone.MEDIUM,
            base_risk_score=0.5, crime_rate=65, flood_risk=0.35,
            average_income=33000, population_density=2500,
            motor_theft_rate=7, burglary_rate=5,
            coordinates=(51.4816, -3.1791), major_city="Cardiff"
        ),
        "BS": PostcodeProfile(
            area="BS", region="Bristol", risk_zone=RiskZone.MEDIUM,
            base_risk_score=0.45, crime_rate=60, flood_risk=0.3,
            average_income=38000, population_density=3800,
            motor_theft_rate=6, burglary_rate=5,
            coordinates=(51.4545, -2.5879), major_city="Bristol"
        ),
        
        # Lower Risk Areas
        "OX": PostcodeProfile(
            area="OX", region="Oxford", risk_zone=RiskZone.LOW,
            base_risk_score=0.3, crime_rate=45, flood_risk=0.25,
            average_income=45000, population_density=2200,
            motor_theft_rate=3, burglary_rate=3,
            coordinates=(51.7520, -1.2577), major_city="Oxford"
        ),
        "CB": PostcodeProfile(
            area="CB", region="Cambridge", risk_zone=RiskZone.LOW,
            base_risk_score=0.3, crime_rate=40, flood_risk=0.2,
            average_income=48000, population_density=2000,
            motor_theft_rate=3, burglary_rate=2,
            coordinates=(52.2053, 0.1218), major_city="Cambridge"
        ),
        "BA": PostcodeProfile(
            area="BA", region="Bath", risk_zone=RiskZone.VERY_LOW,
            base_risk_score=0.25, crime_rate=35, flood_risk=0.2,
            average_income=50000, population_density=1800,
            motor_theft_rate=2, burglary_rate=2,
            coordinates=(51.3758, -2.3599), major_city="Bath"
        ),
        "YO": PostcodeProfile(
            area="YO", region="York", risk_zone=RiskZone.LOW,
            base_risk_score=0.35, crime_rate=48, flood_risk=0.4,
            average_income=36000, population_density=1500,
            motor_theft_rate=4, burglary_rate=3,
            coordinates=(53.9591, -1.0815), major_city="York"
        ),
        "RG": PostcodeProfile(
            area="RG", region="Reading", risk_zone=RiskZone.LOW,
            base_risk_score=0.35, crime_rate=50, flood_risk=0.3,
            average_income=52000, population_density=2800,
            motor_theft_rate=4, burglary_rate=3,
            coordinates=(51.4543, -0.9781), major_city="Reading"
        ),
        "GU": PostcodeProfile(
            area="GU", region="Guildford", risk_zone=RiskZone.VERY_LOW,
            base_risk_score=0.25, crime_rate=30, flood_risk=0.15,
            average_income=58000, population_density=1600,
            motor_theft_rate=2, burglary_rate=2,
            coordinates=(51.2365, -0.5703), major_city="Guildford"
        ),
        
        # Coastal/Other Areas
        "BN": PostcodeProfile(
            area="BN", region="Brighton", risk_zone=RiskZone.MEDIUM,
            base_risk_score=0.45, crime_rate=62, flood_risk=0.25,
            average_income=35000, population_density=3200,
            motor_theft_rate=6, burglary_rate=5,
            coordinates=(50.8225, -0.1372), major_city="Brighton"
        ),
        "SO": PostcodeProfile(
            area="SO", region="Southampton", risk_zone=RiskZone.MEDIUM,
            base_risk_score=0.45, crime_rate=58, flood_risk=0.35,
            average_income=34000, population_density=2800,
            motor_theft_rate=6, burglary_rate=4,
            coordinates=(50.9097, -1.4044), major_city="Southampton"
        ),
        "PO": PostcodeProfile(
            area="PO", region="Portsmouth", risk_zone=RiskZone.MEDIUM,
            base_risk_score=0.5, crime_rate=65, flood_risk=0.4,
            average_income=32000, population_density=3100,
            motor_theft_rate=7, burglary_rate=5,
            coordinates=(50.8198, -1.0880), major_city="Portsmouth"
        )
    }
    
    def __init__(self):
        """Initialize the postcode profiler."""
        self.postcode_pattern = re.compile(
            r'^([A-Z]{1,2})(\d{1,2}[A-Z]?)\s?(\d)([A-Z]{2})$',
            re.IGNORECASE
        )
        
    def parse_postcode(self, postcode: str) -> Optional[Dict[str, str]]:
        """Parse a UK postcode into components.
        
        Args:
            postcode: UK postcode string
            
        Returns:
            Dictionary with postcode components or None if invalid
        """
        cleaned = postcode.upper().replace(' ', '')
        match = self.postcode_pattern.match(cleaned)
        
        if not match:
            return None
            
        return {
            'full': postcode.upper(),
            'area': match.group(1),
            'district': match.group(1) + match.group(2),
            'sector': match.group(1) + match.group(2) + ' ' + match.group(3),
            'unit': match.group(4),
            'outward': match.group(1) + match.group(2),
            'inward': match.group(3) + match.group(4)
        }
    
    def get_profile(self, postcode: str) -> Optional[PostcodeProfile]:
        """Get risk profile for a postcode.
        
        Args:
            postcode: UK postcode
            
        Returns:
            PostcodeProfile or None if not found
        """
        parsed = self.parse_postcode(postcode)
        if not parsed:
            return None
            
        area = parsed['area']
        return self.POSTCODE_PROFILES.get(area)
    
    def calculate_risk_score(
        self,
        postcode: str,
        factors: Optional[Dict[str, float]] = None
    ) -> float:
        """Calculate comprehensive risk score for a postcode.
        
        Args:
            postcode: UK postcode
            factors: Additional risk factors to consider
            
        Returns:
            Risk score between 0 and 1
        """
        profile = self.get_profile(postcode)
        if not profile:
            return 0.5  # Default medium risk
            
        # Start with base risk
        score = profile.base_risk_score
        
        # Apply additional factors if provided
        if factors:
            # Crime factor
            if 'crime_weight' in factors:
                crime_factor = (profile.crime_rate / 100) * factors['crime_weight']
                score += crime_factor * 0.2
                
            # Flood factor
            if 'flood_weight' in factors:
                score += profile.flood_risk * factors['flood_weight'] * 0.15
                
            # Income factor (inverse - lower income = higher risk)
            if 'income_weight' in factors:
                income_factor = 1 - (profile.average_income / 75000)
                score += income_factor * factors['income_weight'] * 0.1
                
            # Density factor
            if 'density_weight' in factors:
                density_factor = min(profile.population_density / 10000, 1)
                score += density_factor * factors['density_weight'] * 0.1
                
        # Add random variation
        score += random.uniform(-0.05, 0.05)
        
        return max(0, min(1, score))
    
    def get_distance_between_postcodes(
        self,
        postcode1: str,
        postcode2: str
    ) -> Optional[float]:
        """Calculate distance between two postcodes in km.
        
        Args:
            postcode1: First UK postcode
            postcode2: Second UK postcode
            
        Returns:
            Distance in kilometers or None if postcodes invalid
        """
        profile1 = self.get_profile(postcode1)
        profile2 = self.get_profile(postcode2)
        
        if not profile1 or not profile2:
            return None
            
        return geodesic(profile1.coordinates, profile2.coordinates).kilometers
    
    def get_risk_zone(self, postcode: str) -> Optional[RiskZone]:
        """Get risk zone classification for a postcode.
        
        Args:
            postcode: UK postcode
            
        Returns:
            RiskZone enum or None
        """
        profile = self.get_profile(postcode)
        return profile.risk_zone if profile else None
    
    def get_high_risk_areas(self) -> List[str]:
        """Get list of high-risk postcode areas.
        
        Returns:
            List of high-risk area codes
        """
        return [
            area for area, profile in self.POSTCODE_PROFILES.items()
            if profile.risk_zone in [RiskZone.HIGH, RiskZone.VERY_HIGH]
        ]
    
    def get_low_risk_areas(self) -> List[str]:
        """Get list of low-risk postcode areas.
        
        Returns:
            List of low-risk area codes
        """
        return [
            area for area, profile in self.POSTCODE_PROFILES.items()
            if profile.risk_zone in [RiskZone.LOW, RiskZone.VERY_LOW]
        ]
    
    def analyze_geographic_concentration(
        self,
        postcodes: List[str]
    ) -> Dict[str, Any]:
        """Analyze geographic concentration of postcodes.
        
        Args:
            postcodes: List of postcodes to analyze
            
        Returns:
            Analysis results including concentration metrics
        """
        area_counts = {}
        profiles = []
        
        for postcode in postcodes:
            parsed = self.parse_postcode(postcode)
            if parsed:
                area = parsed['area']
                area_counts[area] = area_counts.get(area, 0) + 1
                
                profile = self.get_profile(postcode)
                if profile:
                    profiles.append(profile)
        
        # Calculate concentration metrics
        total_postcodes = len(postcodes)
        unique_areas = len(area_counts)
        
        if total_postcodes == 0:
            return {'error': 'No valid postcodes provided'}
        
        # Find most common area
        most_common_area = max(area_counts, key=area_counts.get) if area_counts else None
        concentration_ratio = area_counts.get(most_common_area, 0) / total_postcodes if most_common_area else 0
        
        # Calculate average risk
        avg_risk = sum(p.base_risk_score for p in profiles) / len(profiles) if profiles else 0
        
        # Identify clusters
        clusters = {
            area: count for area, count in area_counts.items()
            if count >= max(3, total_postcodes * 0.1)  # At least 3 or 10% of total
        }
        
        return {
            'total_postcodes': total_postcodes,
            'unique_areas': unique_areas,
            'most_common_area': most_common_area,
            'concentration_ratio': concentration_ratio,
            'average_risk_score': avg_risk,
            'clusters': clusters,
            'high_concentration': concentration_ratio > 0.3,
            'suspicious_clustering': concentration_ratio > 0.5 and avg_risk > 0.6
        }
    
    def generate_random_postcode(self, risk_zone: Optional[RiskZone] = None) -> str:
        """Generate a random UK postcode.
        
        Args:
            risk_zone: Optional risk zone to generate from
            
        Returns:
            Random UK postcode string
        """
        if risk_zone:
            # Filter areas by risk zone
            areas = [
                area for area, profile in self.POSTCODE_PROFILES.items()
                if profile.risk_zone == risk_zone
            ]
            if not areas:
                areas = list(self.POSTCODE_PROFILES.keys())
        else:
            areas = list(self.POSTCODE_PROFILES.keys())
        
        area = random.choice(areas)
        district = random.randint(1, 99)
        sector = random.randint(0, 9)
        unit = ''.join(random.choices('ABCDEFGHJKLMNPQRSTUVWXYZ', k=2))
        
        return f"{area}{district} {sector}{unit}"