"""Tests for fraud detection system."""

import pytest
from datetime import datetime, timedelta
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import Claim, Policy, Policyholder, ClaimType, PolicyType, ClaimStatus, FraudIndicator, RiskProfile
from src.fraud.fraud_detector import FraudDetector
from src.fraud.fraud_scenarios import FraudScenarioGenerator
from src.generators.base_generator import BaseDataGenerator


class TestFraudDetector:
    """Test fraud detection functionality."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.detector = FraudDetector()
        self.generator = BaseDataGenerator()
        
        # Create test data
        self.policyholder = self.generator.generate_policyholder()
        self.policy = self._create_test_policy()
        self.claim = self._create_test_claim()
    
    def _create_test_policy(self):
        """Create a test policy."""
        return Policy(
            policy_number="TEST-2024-001",
            policyholder_id=self.policyholder.id,
            policy_type=PolicyType.MOTOR,
            start_date=datetime.now() - timedelta(days=180),
            end_date=datetime.now() + timedelta(days=185),
            premium_amount=1000,
            sum_insured=20000,
            deductible=500,
            is_active=True,
            risk_profile=RiskProfile.MEDIUM,
            underwriter="Test Insurer"
        )
    
    def _create_test_claim(self):
        """Create a test claim."""
        return Claim(
            claim_number="CLM-TEST-001",
            policy_id=self.policy.id,
            policyholder_id=self.policyholder.id,
            claim_type=ClaimType.COLLISION,
            status=ClaimStatus.SUBMITTED,
            incident_date=datetime.now() - timedelta(days=10),
            reported_date=datetime.now() - timedelta(days=8),
            claim_amount=5000,
            description="Test collision claim",
            location=self.generator.generate_address()
        )
    
    def test_analyze_claim_basic(self):
        """Test basic claim analysis."""
        analysis = self.detector.analyze_claim(
            self.claim,
            self.policy,
            self.policyholder
        )
        
        assert 'fraud_score' in analysis
        assert 'risk_level' in analysis
        assert 'fraud_indicators' in analysis
        assert 'signals' in analysis
        assert 'is_flagged' in analysis
        assert 'confidence' in analysis
        assert 'recommended_action' in analysis
        
        assert 0 <= analysis['fraud_score'] <= 1
        assert analysis['risk_level'] in ['MINIMAL', 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL']
    
    def test_early_claim_detection(self):
        """Test early claim fraud detection."""
        # Create early claim (within 30 days of policy start)
        self.policy.start_date = datetime.now() - timedelta(days=20)
        self.claim.incident_date = self.policy.start_date + timedelta(days=10)
        self.claim.reported_date = self.claim.incident_date + timedelta(days=1)
        
        analysis = self.detector.analyze_claim(
            self.claim,
            self.policy,
            self.policyholder
        )
        
        assert analysis['signals']['timing'] > 0.5
        assert FraudIndicator.SUSPICIOUS_TIMING in analysis['fraud_indicators']
    
    def test_inflated_amount_detection(self):
        """Test inflated claim amount detection."""
        # Set claim amount to 90% of sum insured
        self.claim.claim_amount = self.policy.sum_insured * 0.9
        
        analysis = self.detector.analyze_claim(
            self.claim,
            self.policy,
            self.policyholder
        )
        
        assert analysis['signals']['amount'] > 0.4
        assert FraudIndicator.INFLATED_AMOUNT in analysis['fraud_indicators']
    
    def test_frequency_analysis(self):
        """Test claim frequency analysis."""
        # Create historical claims
        historical_claims = []
        for i in range(4):
            claim = self._create_test_claim()
            claim.incident_date = datetime.now() - timedelta(days=30 * i)
            historical_claims.append(claim)
        
        analysis = self.detector.analyze_claim(
            self.claim,
            self.policy,
            self.policyholder,
            historical_claims
        )
        
        assert 'frequency' in analysis['signals']
        assert analysis['signals']['frequency'] > 0.3
        assert FraudIndicator.UNUSUAL_FREQUENCY in analysis['fraud_indicators']
    
    def test_blacklist_detection(self):
        """Test blacklist functionality."""
        # Add policyholder to blacklist
        self.detector.blacklist.add(self.policyholder.id)
        
        analysis = self.detector.analyze_claim(
            self.claim,
            self.policy,
            self.policyholder
        )
        
        assert FraudIndicator.BLACKLISTED_ENTITY in analysis['fraud_indicators']
        assert analysis['fraud_score'] > 0.5
        assert analysis['signals'].get('blacklisted') is True
    
    def test_network_analysis(self):
        """Test network fraud detection."""
        # Create network connections
        connected_ids = ["ph_001", "ph_002", "ph_003"]
        self.detector.add_to_network(self.policyholder.id, connected_ids)
        
        # Mark connected policyholders as fraudulent
        for conn_id in connected_ids:
            self.detector.policyholder_scores[conn_id] = [0.8, 0.9, 0.85]
        
        analysis = self.detector.analyze_claim(
            self.claim,
            self.policy,
            self.policyholder
        )
        
        assert analysis['signals']['network'] > 0.3
    
    def test_risk_level_calculation(self):
        """Test risk level calculation."""
        assert self.detector._get_risk_level(0.1) == 'MINIMAL'
        assert self.detector._get_risk_level(0.4) == 'LOW'
        assert self.detector._get_risk_level(0.65) == 'MEDIUM'
        assert self.detector._get_risk_level(0.85) == 'HIGH'
        assert self.detector._get_risk_level(0.95) == 'CRITICAL'
    
    def test_recommended_actions(self):
        """Test recommended action generation."""
        assert self.detector._recommend_action(0.95, 'CRITICAL') == 'IMMEDIATE_INVESTIGATION_REQUIRED'
        assert self.detector._recommend_action(0.85, 'HIGH') == 'MANUAL_REVIEW_REQUIRED'
        assert self.detector._recommend_action(0.65, 'MEDIUM') == 'ADDITIONAL_DOCUMENTATION_REQUIRED'
        assert self.detector._recommend_action(0.4, 'LOW') == 'STANDARD_PROCESSING_WITH_MONITORING'
        assert self.detector._recommend_action(0.1, 'MINIMAL') == 'STANDARD_PROCESSING'
    
    def test_statistics_tracking(self):
        """Test statistics tracking."""
        # Analyze multiple claims
        for _ in range(5):
            self.detector.analyze_claim(
                self._create_test_claim(),
                self.policy,
                self.policyholder
            )
        
        stats = self.detector.get_statistics()
        
        assert stats['total_claims_analyzed'] >= 5
        assert stats['unique_policyholders'] >= 1
        assert 'average_fraud_score' in stats
        assert 'blacklisted_entities' in stats


class TestFraudScenarios:
    """Test fraud scenario generation."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.scenario_gen = FraudScenarioGenerator()
    
    def test_early_claim_scenario(self):
        """Test early claim fraud scenario."""
        scenario = self.scenario_gen.generate_early_claim_fraud()
        
        assert scenario['scenario_type'] == 'early_claim_fraud'
        assert 'claim' in scenario
        assert 'policy' in scenario
        assert 'policyholder' in scenario
        assert 'key_indicators' in scenario
        
        claim = scenario['claim']
        policy = scenario['policy']
        
        # Verify early claim characteristics
        days_since_start = (claim.incident_date - policy.start_date).days
        assert days_since_start < 30
        assert claim.fraud_score >= 0.7
        assert claim.is_flagged is True
    
    def test_inflated_claim_scenario(self):
        """Test inflated claim fraud scenario."""
        scenario = self.scenario_gen.generate_inflated_claim_fraud()
        
        assert scenario['scenario_type'] == 'inflated_claim_fraud'
        
        claim = scenario['claim']
        policy = scenario['policy']
        
        # Verify inflated amount
        ratio = claim.claim_amount / policy.sum_insured
        assert ratio > 0.7
        assert FraudIndicator.INFLATED_AMOUNT in claim.fraud_indicators
    
    def test_fraud_ring_scenario(self):
        """Test fraud ring scenario generation."""
        ring_size = 4
        scenario = self.scenario_gen.generate_fraud_ring(size=ring_size)
        
        assert scenario['scenario_type'] == 'fraud_ring'
        assert len(scenario['claims']) == ring_size
        assert len(scenario['policies']) == ring_size
        assert len(scenario['policyholders']) == ring_size
        
        # Verify ring characteristics
        postcodes = [ph.address.postcode[:3] for ph in scenario['policyholders']]
        # Should be from similar areas
        assert len(set(postcodes)) <= 2
        
        for claim in scenario['claims']:
            assert FraudIndicator.NETWORK_FRAUD_RING in claim.fraud_indicators
            assert claim.is_flagged is True
    
    def test_repeat_offender_scenario(self):
        """Test repeat offender scenario."""
        claim_count = 3
        scenario = self.scenario_gen.generate_repeat_offender(claim_count=claim_count)
        
        assert scenario['scenario_type'] == 'repeat_offender'
        assert len(scenario['claims']) == claim_count
        assert len(scenario['policies']) == claim_count
        
        # All claims should be from same policyholder
        policyholder_ids = [c.policyholder_id for c in scenario['claims']]
        assert len(set(policyholder_ids)) == 1
        
        for claim in scenario['claims']:
            assert claim.fraud_score >= 0.75
    
    def test_phantom_claim_scenario(self):
        """Test phantom claim scenario."""
        scenario = self.scenario_gen.generate_phantom_claim()
        
        assert scenario['scenario_type'] == 'phantom_claim'
        
        claim = scenario['claim']
        
        # Phantom claims typically have late reporting
        assert claim.reporting_delay_days >= 30
        assert claim.claim_type == ClaimType.THEFT
        assert claim.fraud_score >= 0.85
        assert FraudIndicator.SUSPICIOUS_TIMING in claim.fraud_indicators
    
    def test_opportunistic_fraud_scenario(self):
        """Test opportunistic fraud scenario."""
        scenario = self.scenario_gen.generate_opportunistic_fraud()
        
        assert scenario['scenario_type'] == 'opportunistic_fraud'
        assert 'actual_damage' in scenario
        assert 'claimed_damage' in scenario
        
        claim = scenario['claim']
        
        # Claimed amount should be inflated
        inflation_ratio = scenario['claimed_damage'] / scenario['actual_damage']
        assert inflation_ratio >= 2.0
        assert FraudIndicator.INFLATED_AMOUNT in claim.fraud_indicators
    
    def test_scenario_summary_generation(self):
        """Test scenario summary generation."""
        scenario = self.scenario_gen.generate_early_claim_fraud()
        summary = self.scenario_gen.get_scenario_summary(scenario)
        
        assert 'Fraud Scenario:' in summary
        assert 'Key Indicators:' in summary
        assert 'Claim amount:' in summary
        assert 'Fraud score:' in summary


if __name__ == "__main__":
    pytest.main([__file__, "-v"])