"""Predefined fraud scenarios for testing and demonstration."""

from datetime import datetime, timedelta
from typing import List, Dict, Any
import random
import sys
sys.path.append('../..')
from src.models import (
    Claim, Policy, Policyholder, 
    ClaimType, PolicyType, ClaimStatus, FraudIndicator
)
from src.generators.base_generator import BaseDataGenerator
from src.generators.policy_generator import PolicyGenerator
from src.generators.claim_generator import ClaimGenerator


class FraudScenarioGenerator:
    """Generate specific fraud scenarios for testing."""
    
    def __init__(self):
        """Initialize scenario generator."""
        self.base_generator = BaseDataGenerator()
        self.policy_generator = PolicyGenerator()
        self.claim_generator = ClaimGenerator(fraud_rate=1.0)  # 100% fraud
        
    def generate_early_claim_fraud(self) -> Dict[str, Any]:
        """Generate a claim filed shortly after policy inception.
        
        Returns:
            Scenario data with policyholder, policy, and claim
        """
        # Create policyholder with medium-high risk
        policyholder = self.base_generator.generate_policyholder()
        policyholder.risk_score = random.uniform(0.6, 0.8)
        
        # Create policy that started recently
        start_date = datetime.now() - timedelta(days=random.randint(10, 25))
        policy = self.policy_generator.generate_policy(
            policyholder=policyholder,
            policy_type=PolicyType.MOTOR,
            start_date=start_date
        )
        
        # Create claim shortly after policy start
        incident_date = start_date + timedelta(days=random.randint(5, 20))
        claim = self.claim_generator.generate_claim(
            policy=policy,
            policyholder=policyholder,
            is_fraud=True,
            incident_date=incident_date
        )
        
        # Ensure fraud indicators
        if FraudIndicator.SUSPICIOUS_TIMING not in claim.fraud_indicators:
            claim.fraud_indicators.append(FraudIndicator.SUSPICIOUS_TIMING)
        claim.fraud_score = max(claim.fraud_score, 0.7)
        claim.is_flagged = True
        
        return {
            'scenario_type': 'early_claim_fraud',
            'description': 'Claim filed suspiciously soon after policy inception',
            'policyholder': policyholder,
            'policy': policy,
            'claim': claim,
            'key_indicators': ['Policy age < 30 days', 'High claim amount', 'First claim on policy']
        }
    
    def generate_inflated_claim_fraud(self) -> Dict[str, Any]:
        """Generate a claim with inflated amount.
        
        Returns:
            Scenario data
        """
        policyholder = self.base_generator.generate_policyholder()
        policy = self.policy_generator.generate_policy(
            policyholder=policyholder,
            policy_type=PolicyType.PROPERTY
        )
        
        # Create claim with inflated amount
        claim = self.claim_generator.generate_claim(
            policy=policy,
            policyholder=policyholder,
            is_fraud=True
        )
        
        # Inflate the amount
        claim.claim_amount = policy.sum_insured * random.uniform(0.8, 0.95)
        claim.description += " - Complete loss claimed, minimal evidence provided"
        
        # Add indicators
        if FraudIndicator.INFLATED_AMOUNT not in claim.fraud_indicators:
            claim.fraud_indicators.append(FraudIndicator.INFLATED_AMOUNT)
        claim.fraud_score = max(claim.fraud_score, 0.8)
        claim.is_flagged = True
        
        return {
            'scenario_type': 'inflated_claim_fraud',
            'description': 'Claim amount suspiciously close to policy maximum',
            'policyholder': policyholder,
            'policy': policy,
            'claim': claim,
            'key_indicators': ['Claim > 80% of sum insured', 'Vague description', 'Round number amount']
        }
    
    def generate_fraud_ring(self, size: int = 5) -> Dict[str, Any]:
        """Generate a network of related fraudulent claims.
        
        Args:
            size: Number of related claims
            
        Returns:
            Scenario data with multiple claims
        """
        claims = []
        policyholders = []
        policies = []
        
        # Common characteristics for the ring
        base_postcode = self.base_generator.faker.uk_postcode()
        base_area = base_postcode.split()[0]
        incident_date = datetime.now() - timedelta(days=random.randint(5, 15))
        claim_type = ClaimType.COLLISION
        
        for i in range(size):
            # Create related policyholders (same area)
            policyholder = self.base_generator.generate_policyholder()
            policyholder.address.postcode = f"{base_area} {random.randint(1,9)}XX"
            policyholder.risk_score = random.uniform(0.6, 0.9)
            policyholders.append(policyholder)
            
            # Create motor policies
            policy = self.policy_generator.generate_policy(
                policyholder=policyholder,
                policy_type=PolicyType.MOTOR
            )
            policies.append(policy)
            
            # Create similar claims
            claim = self.claim_generator.generate_claim(
                policy=policy,
                policyholder=policyholder,
                is_fraud=True,
                incident_date=incident_date + timedelta(hours=random.randint(-48, 48))
            )
            
            # Make claims similar
            claim.claim_type = claim_type
            claim.description = f"Multi-vehicle collision on A40, multiple vehicles involved"
            claim.claim_amount = random.uniform(8000, 12000)
            
            # Add network indicator
            claim.fraud_indicators.append(FraudIndicator.NETWORK_FRAUD_RING)
            claim.fraud_score = max(claim.fraud_score, 0.85)
            claim.is_flagged = True
            
            claims.append(claim)
        
        return {
            'scenario_type': 'fraud_ring',
            'description': f'Network of {size} related fraudulent claims',
            'policyholders': policyholders,
            'policies': policies,
            'claims': claims,
            'key_indicators': [
                'Similar incident dates',
                'Same geographic area',
                'Similar claim amounts',
                'Matching incident descriptions',
                f'{size} related claims detected'
            ]
        }
    
    def generate_repeat_offender(self, claim_count: int = 4) -> Dict[str, Any]:
        """Generate multiple suspicious claims from same policyholder.
        
        Args:
            claim_count: Number of claims to generate
            
        Returns:
            Scenario data
        """
        # High-risk policyholder
        policyholder = self.base_generator.generate_policyholder()
        policyholder.risk_score = random.uniform(0.7, 0.9)
        
        # Multiple policies
        policies = []
        claims = []
        
        for i in range(claim_count):
            # Different policy types
            policy_type = random.choice(list(PolicyType))
            policy = self.policy_generator.generate_policy(
                policyholder=policyholder,
                policy_type=policy_type
            )
            policies.append(policy)
            
            # Claims spread over time
            days_ago = random.randint(30 * i, 30 * (i + 2))
            incident_date = datetime.now() - timedelta(days=days_ago)
            
            claim = self.claim_generator.generate_claim(
                policy=policy,
                policyholder=policyholder,
                is_fraud=True,
                incident_date=incident_date
            )
            
            # Add frequency indicator
            if FraudIndicator.UNUSUAL_FREQUENCY not in claim.fraud_indicators:
                claim.fraud_indicators.append(FraudIndicator.UNUSUAL_FREQUENCY)
            claim.fraud_score = max(claim.fraud_score, 0.75)
            claim.is_flagged = True
            
            claims.append(claim)
        
        return {
            'scenario_type': 'repeat_offender',
            'description': f'Policyholder with {claim_count} suspicious claims',
            'policyholder': policyholder,
            'policies': policies,
            'claims': claims,
            'key_indicators': [
                f'{claim_count} claims in short period',
                'Multiple policy types',
                'Consistent high-value claims',
                'Pattern of suspicious behavior'
            ]
        }
    
    def generate_phantom_claim(self) -> Dict[str, Any]:
        """Generate a claim for an incident that likely didn't occur.
        
        Returns:
            Scenario data
        """
        policyholder = self.base_generator.generate_policyholder()
        policy = self.policy_generator.generate_policy(
            policyholder=policyholder,
            policy_type=PolicyType.PROPERTY
        )
        
        # Create phantom theft claim
        claim = self.claim_generator.generate_claim(
            policy=policy,
            policyholder=policyholder,
            is_fraud=True
        )
        
        # Characteristics of phantom claim
        claim.claim_type = ClaimType.THEFT
        claim.description = "Items stolen while away, no witnesses, no CCTV, no forced entry signs"
        claim.reporting_delay_days = random.randint(30, 60)  # Late reporting
        claim.reported_date = claim.incident_date + timedelta(days=claim.reporting_delay_days)
        
        # High-value items
        claim.claim_amount = random.uniform(10000, 25000)
        
        # Add indicators
        claim.fraud_indicators.extend([
            FraudIndicator.SUSPICIOUS_TIMING,
            FraudIndicator.INCONSISTENT_DETAILS
        ])
        claim.fraud_score = max(claim.fraud_score, 0.85)
        claim.is_flagged = True
        
        return {
            'scenario_type': 'phantom_claim',
            'description': 'Claim for incident that likely never occurred',
            'policyholder': policyholder,
            'policy': policy,
            'claim': claim,
            'key_indicators': [
                'No witnesses or evidence',
                'Significant reporting delay',
                'High-value items claimed',
                'Vague or inconsistent details'
            ]
        }
    
    def generate_opportunistic_fraud(self) -> Dict[str, Any]:
        """Generate legitimate claim with inflated damages.
        
        Returns:
            Scenario data
        """
        policyholder = self.base_generator.generate_policyholder()
        policy = self.policy_generator.generate_policy(
            policyholder=policyholder,
            policy_type=PolicyType.MOTOR
        )
        
        claim = self.claim_generator.generate_claim(
            policy=policy,
            policyholder=policyholder,
            is_fraud=False  # Start as legitimate
        )
        
        # Real incident but inflated
        claim.claim_type = ClaimType.COLLISION
        original_amount = random.uniform(2000, 5000)
        claim.claim_amount = original_amount * random.uniform(2.0, 3.5)  # Inflate 2-3.5x
        claim.description = f"Minor collision damage, claiming additional pre-existing damage"
        
        # Add fraud indicators
        claim.fraud_indicators.append(FraudIndicator.INFLATED_AMOUNT)
        claim.fraud_score = random.uniform(0.6, 0.75)
        claim.is_flagged = True
        
        return {
            'scenario_type': 'opportunistic_fraud',
            'description': 'Legitimate incident with inflated claim amount',
            'policyholder': policyholder,
            'policy': policy,
            'claim': claim,
            'key_indicators': [
                'Real incident occurred',
                'Damages inflated 2-3x',
                'Including pre-existing damage',
                'Opportunistic behavior pattern'
            ],
            'actual_damage': original_amount,
            'claimed_damage': claim.claim_amount
        }
    
    def generate_all_scenarios(self) -> List[Dict[str, Any]]:
        """Generate one of each fraud scenario.
        
        Returns:
            List of all scenario types
        """
        scenarios = [
            self.generate_early_claim_fraud(),
            self.generate_inflated_claim_fraud(),
            self.generate_fraud_ring(size=4),
            self.generate_repeat_offender(claim_count=3),
            self.generate_phantom_claim(),
            self.generate_opportunistic_fraud()
        ]
        
        return scenarios
    
    def get_scenario_summary(self, scenario: Dict[str, Any]) -> str:
        """Generate a summary description of a fraud scenario.
        
        Args:
            scenario: Scenario data
            
        Returns:
            Summary string
        """
        summary = f"""
Fraud Scenario: {scenario['scenario_type'].replace('_', ' ').title()}
Description: {scenario['description']}

Key Indicators:
{chr(10).join(f"  • {indicator}" for indicator in scenario['key_indicators'])}

"""
        
        if 'claims' in scenario:
            summary += f"Number of claims: {len(scenario['claims'])}\n"
            total_amount = sum(c.claim_amount for c in scenario['claims'])
            summary += f"Total claim amount: £{total_amount:,.2f}\n"
        elif 'claim' in scenario:
            summary += f"Claim amount: £{scenario['claim'].claim_amount:,.2f}\n"
            summary += f"Fraud score: {scenario['claim'].fraud_score:.2f}\n"
            
        return summary