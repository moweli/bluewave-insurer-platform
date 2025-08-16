"""Tests for data generators."""

import pytest
from datetime import datetime, timedelta
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import PolicyType, ClaimType, ClaimStatus, RiskProfile, FraudIndicator
from src.generators.base_generator import BaseDataGenerator
from src.generators.policy_generator import PolicyGenerator
from src.generators.claim_generator import ClaimGenerator


class TestBaseDataGenerator:
    """Test base data generator functionality."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.generator = BaseDataGenerator()
    
    def test_generate_address(self):
        """Test UK address generation."""
        address = self.generator.generate_address()
        
        assert address.line1 is not None
        assert address.city is not None
        assert address.postcode is not None
        assert address.country == "United Kingdom"
        
        # Test postcode format
        assert len(address.postcode.replace(' ', '')) >= 5
        assert address.postcode[0].isalpha()
    
    def test_generate_policyholder(self):
        """Test policyholder generation."""
        policyholder = self.generator.generate_policyholder()
        
        assert policyholder.id is not None
        assert policyholder.first_name is not None
        assert policyholder.last_name is not None
        assert policyholder.email is not None
        assert '@' in policyholder.email
        assert policyholder.phone is not None
        assert 0 <= policyholder.risk_score <= 1
        assert policyholder.age >= 18
    
    def test_generate_corporate_policyholder(self):
        """Test corporate policyholder generation."""
        policyholder = self.generator.generate_policyholder(is_corporate=True)
        
        assert policyholder.is_corporate is True
        assert policyholder.company_name is not None
        assert policyholder.company_registration is not None
    
    def test_risk_score_calculation(self):
        """Test risk score calculation."""
        address = self.generator.generate_address()
        
        # Young age should increase risk
        young_risk = self.generator.calculate_risk_score(address, 22, False)
        middle_risk = self.generator.calculate_risk_score(address, 40, False)
        
        assert 0 <= young_risk <= 1
        assert 0 <= middle_risk <= 1
        # Young drivers typically have higher risk
        assert young_risk > middle_risk - 0.2  # Account for randomness
    
    def test_risk_profile_conversion(self):
        """Test risk score to profile conversion."""
        assert self.generator.get_risk_profile(0.2) == RiskProfile.LOW
        assert self.generator.get_risk_profile(0.5) == RiskProfile.MEDIUM
        assert self.generator.get_risk_profile(0.7) == RiskProfile.HIGH
        assert self.generator.get_risk_profile(0.9) == RiskProfile.VERY_HIGH


class TestPolicyGenerator:
    """Test policy generator functionality."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.generator = PolicyGenerator()
    
    def test_generate_policy(self):
        """Test policy generation."""
        policy = self.generator.generate_policy()
        
        assert policy.id is not None
        assert policy.policy_number is not None
        assert policy.policyholder_id is not None
        assert policy.policy_type in PolicyType
        assert policy.premium_amount > 0
        assert policy.sum_insured > 0
        assert policy.deductible >= 0
        assert policy.end_date > policy.start_date
    
    def test_policy_number_format(self):
        """Test policy number format."""
        policy = self.generator.generate_policy(policy_type=PolicyType.MOTOR)
        
        assert policy.policy_number.startswith("MOT-")
        assert str(datetime.now().year) in policy.policy_number
    
    def test_premium_calculation(self):
        """Test premium calculation logic."""
        premium_low = self.generator.calculate_premium(PolicyType.MOTOR, 0.2, False)
        premium_high = self.generator.calculate_premium(PolicyType.MOTOR, 0.8, False)
        
        assert premium_low > 0
        assert premium_high > 0
        assert premium_high > premium_low  # Higher risk = higher premium
    
    def test_corporate_policy(self):
        """Test corporate policy generation."""
        policyholder = self.generator.generate_policyholder(is_corporate=True)
        policy = self.generator.generate_policy(policyholder=policyholder)
        
        assert policy.policyholder_id == policyholder.id
        # Corporate policies typically have higher premiums
        assert policy.premium_amount > 0
    
    def test_policy_renewal(self):
        """Test policy renewal generation."""
        original = self.generator.generate_policy()
        renewal = self.generator.generate_renewal(original)
        
        assert renewal.policy_number.endswith(f"-R{datetime.now().year}")
        assert renewal.policyholder_id == original.policyholder_id
        assert renewal.policy_type == original.policy_type
        assert renewal.start_date == original.end_date
        assert renewal.is_active is True


class TestClaimGenerator:
    """Test claim generator functionality."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.claim_generator = ClaimGenerator(fraud_rate=0.1)
        self.policy_generator = PolicyGenerator()
    
    def test_generate_claim(self):
        """Test basic claim generation."""
        policy = self.policy_generator.generate_policy()
        claim = self.claim_generator.generate_claim(policy)
        
        assert claim.id is not None
        assert claim.claim_number is not None
        assert claim.policy_id == policy.id
        assert claim.policyholder_id == policy.policyholder_id
        assert claim.claim_type in ClaimType
        assert claim.status in ClaimStatus
        assert claim.claim_amount > 0
        assert claim.incident_date <= claim.reported_date
        assert 0 <= claim.fraud_score <= 1
    
    def test_claim_number_format(self):
        """Test claim number format."""
        policy = self.policy_generator.generate_policy()
        claim = self.claim_generator.generate_claim(policy)
        
        assert claim.claim_number.startswith("CLM-")
        assert datetime.now().strftime("%Y%m") in claim.claim_number
    
    def test_fraud_injection(self):
        """Test fraud pattern injection."""
        policy = self.policy_generator.generate_policy()
        policyholder = self.policy_generator.generate_policyholder()
        
        # Force fraud injection
        claim = self.claim_generator.generate_claim(
            policy=policy,
            policyholder=policyholder,
            is_fraud=True
        )
        
        assert claim.fraud_score > 0
        assert len(claim.fraud_indicators) > 0 or claim.fraud_score > 0.5
    
    def test_claim_amount_calculation(self):
        """Test claim amount calculation."""
        policy = self.policy_generator.generate_policy()
        policy.sum_insured = 10000
        
        amount_normal = self.claim_generator.calculate_claim_amount(
            ClaimType.COLLISION, policy.sum_insured, is_fraud=False
        )
        amount_fraud = self.claim_generator.calculate_claim_amount(
            ClaimType.COLLISION, policy.sum_insured, is_fraud=True
        )
        
        assert 0 < amount_normal <= policy.sum_insured
        assert 0 < amount_fraud <= policy.sum_insured
    
    def test_fraud_patterns(self):
        """Test various fraud patterns."""
        # Early claim fraud
        policy = self.policy_generator.generate_policy()
        policy.start_date = datetime.now() - timedelta(days=10)
        policyholder = self.policy_generator.generate_policyholder()
        
        claim = self.claim_generator.generate_claim(
            policy=policy,
            policyholder=policyholder,
            is_fraud=True,
            incident_date=policy.start_date + timedelta(days=5)
        )
        
        claim = self.claim_generator.inject_fraud_patterns(claim, policy, policyholder)
        
        assert FraudIndicator.SUSPICIOUS_TIMING in claim.fraud_indicators
        assert claim.fraud_score > 0.3
    
    def test_fraud_ring_generation(self):
        """Test fraud ring generation."""
        fraud_ring = self.claim_generator.generate_fraud_ring(size=3)
        
        assert len(fraud_ring) == 3
        
        # All claims should be flagged
        for claim in fraud_ring:
            assert claim.is_flagged is True
            assert FraudIndicator.NETWORK_FRAUD_RING in claim.fraud_indicators
            assert claim.fraud_score > 0.5
        
        # Claims should have similar characteristics
        claim_types = [c.claim_type for c in fraud_ring]
        assert len(set(claim_types)) <= 2  # Similar claim types


class TestClaimTypeMapping:
    """Test claim type mapping by policy type."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.claim_generator = ClaimGenerator()
        self.policy_generator = PolicyGenerator()
    
    def test_motor_policy_claims(self):
        """Test motor policy claim types."""
        policy = self.policy_generator.generate_policy(policy_type=PolicyType.MOTOR)
        claim = self.claim_generator.generate_claim(policy)
        
        valid_types = [
            ClaimType.COLLISION,
            ClaimType.THEFT,
            ClaimType.LIABILITY_INJURY,
            ClaimType.LIABILITY_DAMAGE,
            ClaimType.OTHER
        ]
        assert claim.claim_type in valid_types
    
    def test_property_policy_claims(self):
        """Test property policy claim types."""
        policy = self.policy_generator.generate_policy(policy_type=PolicyType.PROPERTY)
        claim = self.claim_generator.generate_claim(policy)
        
        valid_types = [
            ClaimType.FIRE,
            ClaimType.FLOOD,
            ClaimType.STORM,
            ClaimType.THEFT,
            ClaimType.LIABILITY_DAMAGE,
            ClaimType.OTHER
        ]
        assert claim.claim_type in valid_types
    
    def test_cyber_policy_claims(self):
        """Test cyber policy claim types."""
        policy = self.policy_generator.generate_policy(policy_type=PolicyType.CYBER)
        claim = self.claim_generator.generate_claim(policy)
        
        valid_types = [
            ClaimType.CYBER_BREACH,
            ClaimType.LIABILITY_DAMAGE,
            ClaimType.OTHER
        ]
        assert claim.claim_type in valid_types


if __name__ == "__main__":
    pytest.main([__file__, "-v"])