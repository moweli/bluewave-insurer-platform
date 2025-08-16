"""Base data generator with UK-specific features."""

import random
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from faker import Faker
from faker.providers import BaseProvider
import sys
sys.path.append('../..')
from src.models import (
    Address, Policyholder, Policy, Claim,
    PolicyType, ClaimStatus, ClaimType, RiskProfile
)


class UKInsuranceProvider(BaseProvider):
    """Custom Faker provider for UK insurance data."""
    
    # Major UK insurance companies
    INSURERS = [
        "Aviva", "AXA UK", "Direct Line", "Admiral Group", "RSA Insurance",
        "Zurich UK", "Allianz UK", "Legal & General", "Churchill", "LV=",
        "NFU Mutual", "Hastings Direct", "esure", "Co-op Insurance", "Saga"
    ]
    
    # UK insurance brokers
    BROKERS = [
        "Marsh McLennan", "Aon UK", "Willis Towers Watson", "Gallagher",
        "Howden", "Lockton", "JLT", "BGL Group", "Arthur J. Gallagher",
        "Towergate", "A-Plan", "Swinton", "Be Wiser", "One Call", None
    ]
    
    # UK cities and regions
    UK_CITIES = [
        "London", "Birmingham", "Manchester", "Glasgow", "Liverpool",
        "Leeds", "Sheffield", "Edinburgh", "Bristol", "Cardiff",
        "Leicester", "Coventry", "Bradford", "Belfast", "Nottingham",
        "Newcastle", "Southampton", "Portsmouth", "Oxford", "Cambridge"
    ]
    
    # UK counties
    UK_COUNTIES = [
        "Greater London", "West Midlands", "Greater Manchester", "West Yorkshire",
        "Kent", "Essex", "Hampshire", "Surrey", "Hertfordshire", "Lancashire",
        "Norfolk", "Suffolk", "Devon", "Somerset", "North Yorkshire",
        "Gloucestershire", "Oxfordshire", "Cambridgeshire", "Warwickshire"
    ]
    
    # UK postcode areas with risk profiles
    POSTCODE_RISK_ZONES = {
        "E": 0.7,   # East London - Higher risk
        "SE": 0.6,  # South East London - Medium-high risk
        "SW": 0.4,  # South West London - Medium risk
        "W": 0.5,   # West London - Medium risk
        "NW": 0.6,  # North West London - Medium-high risk
        "N": 0.6,   # North London - Medium-high risk
        "EC": 0.8,  # City of London - High risk (commercial)
        "WC": 0.7,  # Central London - High risk
        "B": 0.5,   # Birmingham - Medium risk
        "M": 0.6,   # Manchester - Medium-high risk
        "L": 0.6,   # Liverpool - Medium-high risk
        "G": 0.5,   # Glasgow - Medium risk
        "EH": 0.4,  # Edinburgh - Medium-low risk
        "OX": 0.3,  # Oxford - Low risk
        "CB": 0.3,  # Cambridge - Low risk
        "BA": 0.3,  # Bath - Low risk
        "YO": 0.4,  # York - Medium-low risk
        "RG": 0.3,  # Reading - Low risk
        "GU": 0.3,  # Guildford - Low risk
    }
    
    def uk_insurer(self) -> str:
        """Generate a UK insurance company name."""
        return self.random_element(self.INSURERS)
    
    def uk_broker(self) -> Optional[str]:
        """Generate a UK insurance broker name."""
        return self.random_element(self.BROKERS)
    
    def uk_city(self) -> str:
        """Generate a UK city name."""
        return self.random_element(self.UK_CITIES)
    
    def uk_county(self) -> str:
        """Generate a UK county name."""
        return self.random_element(self.UK_COUNTIES)
    
    def uk_postcode(self) -> str:
        """Generate a realistic UK postcode."""
        area = self.random_element(list(self.POSTCODE_RISK_ZONES.keys()))
        district = self.random_int(1, 99)
        sector = self.random_int(0, 9)
        unit = f"{self.random_element('ABCDEFGHJKLMNPQRSTUVWXYZ')}{self.random_element('ABCDEFGHJKLMNPQRSTUVWXYZ')}"
        return f"{area}{district} {sector}{unit}"
    
    def uk_phone(self) -> str:
        """Generate a UK phone number."""
        prefixes = ["020", "0121", "0161", "0141", "0151", "0113", "0114", "0131"]
        prefix = self.random_element(prefixes)
        if prefix == "020":
            return f"{prefix} {self.random_int(7000, 8999)} {self.random_int(1000, 9999)}"
        else:
            return f"{prefix} {self.random_int(100, 999)} {self.random_int(1000, 9999)}"
    
    def uk_mobile(self) -> str:
        """Generate a UK mobile number."""
        return f"07{self.random_int(100000000, 999999999)}"
    
    def policy_number(self, policy_type: PolicyType) -> str:
        """Generate a policy number."""
        prefix = {
            PolicyType.MOTOR: "MOT",
            PolicyType.PROPERTY: "PRO",
            PolicyType.LIABILITY: "LIA",
            PolicyType.PROFESSIONAL_INDEMNITY: "PIN",
            PolicyType.CYBER: "CYB",
            PolicyType.MARINE: "MAR",
            PolicyType.AVIATION: "AVI",
            PolicyType.DIRECTORS_OFFICERS: "DNO"
        }.get(policy_type, "GEN")
        
        return f"{prefix}-{datetime.now().year}-{self.random_int(100000, 999999)}"
    
    def claim_number(self) -> str:
        """Generate a claim number."""
        return f"CLM-{datetime.now().strftime('%Y%m')}-{self.random_int(10000, 99999)}"
    
    def company_registration(self) -> str:
        """Generate a UK company registration number."""
        return f"{self.random_element(['SC', 'NI', ''])}{self.random_int(100000, 999999)}"


class BaseDataGenerator:
    """Base class for generating insurance data."""
    
    def __init__(self, locale: str = "en_GB"):
        """Initialize the generator.
        
        Args:
            locale: Faker locale (default: en_GB for UK)
        """
        self.faker = Faker(locale)
        self.faker.add_provider(UKInsuranceProvider)
        self.created_policyholders: List[Policyholder] = []
        self.created_policies: List[Policy] = []
        self.created_claims: List[Claim] = []
        
    def generate_address(self) -> Address:
        """Generate a UK address."""
        postcode = self.faker.uk_postcode()
        return Address(
            line1=self.faker.street_address(),
            line2=self.faker.secondary_address() if random.random() > 0.5 else None,
            city=self.faker.uk_city(),
            county=self.faker.uk_county(),
            postcode=postcode,
            country="United Kingdom"
        )
    
    def calculate_risk_score(self, address: Address, age: int, is_corporate: bool = False) -> float:
        """Calculate risk score based on various factors.
        
        Args:
            address: Policyholder address
            age: Policyholder age
            is_corporate: Whether it's a corporate policy
            
        Returns:
            Risk score between 0 and 1
        """
        # Base risk from postcode area
        postcode_area = address.postcode.split()[0][:-1] if address.postcode else "XX"
        base_risk = UKInsuranceProvider.POSTCODE_RISK_ZONES.get(postcode_area, 0.5)
        
        # Age factor
        if age < 25:
            age_factor = 0.3
        elif age < 30:
            age_factor = 0.2
        elif age < 60:
            age_factor = 0.0
        else:
            age_factor = 0.1
            
        # Corporate factor
        corporate_factor = -0.1 if is_corporate else 0.0
        
        # Combine factors
        risk_score = base_risk + age_factor + corporate_factor
        
        # Add some randomness
        risk_score += random.uniform(-0.1, 0.1)
        
        # Ensure between 0 and 1
        return max(0.0, min(1.0, risk_score))
    
    def generate_policyholder(self, is_corporate: bool = False) -> Policyholder:
        """Generate a policyholder.
        
        Args:
            is_corporate: Whether to generate a corporate policyholder
            
        Returns:
            Generated Policyholder
        """
        address = self.generate_address()
        dob = self.faker.date_of_birth(minimum_age=18, maximum_age=80)
        # Convert date to datetime for compatibility
        dob_datetime = datetime.combine(dob, datetime.min.time())
        age = (datetime.now() - dob_datetime).days // 365
        
        policyholder = Policyholder(
            first_name=self.faker.first_name(),
            last_name=self.faker.last_name(),
            email=self.faker.email(),
            phone=self.faker.uk_mobile() if random.random() > 0.3 else self.faker.uk_phone(),
            date_of_birth=dob_datetime,
            address=address,
            risk_score=self.calculate_risk_score(address, age, is_corporate),
            is_corporate=is_corporate,
            company_name=self.faker.company() if is_corporate else None,
            company_registration=self.faker.company_registration() if is_corporate else None
        )
        
        self.created_policyholders.append(policyholder)
        return policyholder
    
    def get_risk_profile(self, risk_score: float) -> RiskProfile:
        """Convert risk score to risk profile.
        
        Args:
            risk_score: Numeric risk score (0-1)
            
        Returns:
            RiskProfile enum
        """
        if risk_score < 0.3:
            return RiskProfile.LOW
        elif risk_score < 0.6:
            return RiskProfile.MEDIUM
        elif risk_score < 0.8:
            return RiskProfile.HIGH
        else:
            return RiskProfile.VERY_HIGH