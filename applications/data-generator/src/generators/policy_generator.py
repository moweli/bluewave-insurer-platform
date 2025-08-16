"""Generate synthetic insurance policies."""

import random
from datetime import datetime, timedelta
from typing import Optional, List
import sys
sys.path.append('../..')
from src.models import Policy, Policyholder, PolicyType, RiskProfile
from src.generators.base_generator import BaseDataGenerator


class PolicyGenerator(BaseDataGenerator):
    """Generator for insurance policies."""
    
    # Premium ranges by policy type (in GBP)
    PREMIUM_RANGES = {
        PolicyType.MOTOR: (300, 5000),
        PolicyType.PROPERTY: (200, 3000),
        PolicyType.LIABILITY: (500, 10000),
        PolicyType.PROFESSIONAL_INDEMNITY: (1000, 50000),
        PolicyType.CYBER: (2000, 100000),
        PolicyType.MARINE: (5000, 500000),
        PolicyType.AVIATION: (10000, 1000000),
        PolicyType.DIRECTORS_OFFICERS: (3000, 200000)
    }
    
    # Sum insured multipliers (relative to premium)
    SUM_INSURED_MULTIPLIERS = {
        PolicyType.MOTOR: (50, 200),
        PolicyType.PROPERTY: (100, 500),
        PolicyType.LIABILITY: (100, 1000),
        PolicyType.PROFESSIONAL_INDEMNITY: (50, 500),
        PolicyType.CYBER: (100, 2000),
        PolicyType.MARINE: (50, 300),
        PolicyType.AVIATION: (100, 500),
        PolicyType.DIRECTORS_OFFICERS: (100, 1000)
    }
    
    # Deductible percentages of sum insured
    DEDUCTIBLE_PERCENTAGES = {
        PolicyType.MOTOR: (0.01, 0.05),
        PolicyType.PROPERTY: (0.005, 0.02),
        PolicyType.LIABILITY: (0.01, 0.05),
        PolicyType.PROFESSIONAL_INDEMNITY: (0.02, 0.10),
        PolicyType.CYBER: (0.05, 0.15),
        PolicyType.MARINE: (0.01, 0.05),
        PolicyType.AVIATION: (0.02, 0.10),
        PolicyType.DIRECTORS_OFFICERS: (0.05, 0.15)
    }
    
    def calculate_premium(
        self,
        policy_type: PolicyType,
        risk_score: float,
        is_corporate: bool = False
    ) -> float:
        """Calculate premium based on policy type and risk.
        
        Args:
            policy_type: Type of policy
            risk_score: Policyholder risk score
            is_corporate: Whether it's a corporate policy
            
        Returns:
            Premium amount in GBP
        """
        min_premium, max_premium = self.PREMIUM_RANGES[policy_type]
        
        # Base premium based on risk score
        base_premium = min_premium + (max_premium - min_premium) * risk_score
        
        # Corporate policies typically have higher premiums
        if is_corporate:
            base_premium *= random.uniform(1.5, 3.0)
            
        # Add some randomness
        premium = base_premium * random.uniform(0.8, 1.2)
        
        return round(premium, 2)
    
    def calculate_sum_insured(
        self,
        policy_type: PolicyType,
        premium: float
    ) -> float:
        """Calculate sum insured based on premium.
        
        Args:
            policy_type: Type of policy
            premium: Premium amount
            
        Returns:
            Sum insured amount in GBP
        """
        min_mult, max_mult = self.SUM_INSURED_MULTIPLIERS[policy_type]
        multiplier = random.uniform(min_mult, max_mult)
        return round(premium * multiplier, 2)
    
    def calculate_deductible(
        self,
        policy_type: PolicyType,
        sum_insured: float
    ) -> float:
        """Calculate deductible based on sum insured.
        
        Args:
            policy_type: Type of policy
            sum_insured: Sum insured amount
            
        Returns:
            Deductible amount in GBP
        """
        min_pct, max_pct = self.DEDUCTIBLE_PERCENTAGES[policy_type]
        percentage = random.uniform(min_pct, max_pct)
        return round(sum_insured * percentage, 2)
    
    def generate_policy(
        self,
        policyholder: Optional[Policyholder] = None,
        policy_type: Optional[PolicyType] = None,
        start_date: Optional[datetime] = None
    ) -> Policy:
        """Generate a policy.
        
        Args:
            policyholder: Policyholder (will generate if not provided)
            policy_type: Type of policy (random if not provided)
            start_date: Policy start date (random if not provided)
            
        Returns:
            Generated Policy
        """
        # Generate or use provided policyholder
        if policyholder is None:
            is_corporate = random.random() > 0.7  # 30% corporate policies
            policyholder = self.generate_policyholder(is_corporate)
            
        # Select policy type
        if policy_type is None:
            # Weight policy types by commonality
            weights = {
                PolicyType.MOTOR: 40,
                PolicyType.PROPERTY: 30,
                PolicyType.LIABILITY: 15,
                PolicyType.PROFESSIONAL_INDEMNITY: 5,
                PolicyType.CYBER: 3,
                PolicyType.MARINE: 2,
                PolicyType.AVIATION: 1,
                PolicyType.DIRECTORS_OFFICERS: 4
            }
            policy_type = random.choices(
                list(weights.keys()),
                weights=list(weights.values())
            )[0]
            
        # Generate dates
        if start_date is None:
            # Random start date in the last 2 years
            days_ago = random.randint(0, 730)
            start_date = datetime.now() - timedelta(days=days_ago)
            
        # Most policies are annual
        policy_duration = random.choices(
            [365, 180, 90, 30],
            weights=[80, 10, 5, 5]
        )[0]
        end_date = start_date + timedelta(days=policy_duration)
        
        # Calculate financial details
        premium = self.calculate_premium(
            policy_type,
            policyholder.risk_score,
            policyholder.is_corporate
        )
        sum_insured = self.calculate_sum_insured(policy_type, premium)
        deductible = self.calculate_deductible(policy_type, sum_insured)
        
        # Create policy
        policy = Policy(
            policy_number=self.faker.policy_number(policy_type),
            policyholder_id=policyholder.id,
            policy_type=policy_type,
            start_date=start_date,
            end_date=end_date,
            premium_amount=premium,
            sum_insured=sum_insured,
            deductible=deductible,
            is_active=end_date > datetime.now(),
            risk_profile=self.get_risk_profile(policyholder.risk_score),
            underwriter=self.faker.uk_insurer(),
            broker=self.faker.uk_broker()
        )
        
        self.created_policies.append(policy)
        return policy
    
    def generate_policies(
        self,
        count: int,
        policyholder: Optional[Policyholder] = None
    ) -> List[Policy]:
        """Generate multiple policies.
        
        Args:
            count: Number of policies to generate
            policyholder: Optional policyholder for all policies
            
        Returns:
            List of generated policies
        """
        policies = []
        
        for _ in range(count):
            # If policyholder provided, 70% chance to use same, 30% new
            if policyholder and random.random() < 0.7:
                policy = self.generate_policy(policyholder=policyholder)
            else:
                policy = self.generate_policy()
                
            policies.append(policy)
            
        return policies
    
    def generate_renewal(self, existing_policy: Policy) -> Policy:
        """Generate a renewal policy.
        
        Args:
            existing_policy: Policy to renew
            
        Returns:
            Renewal policy
        """
        # Find policyholder
        policyholder = next(
            (p for p in self.created_policyholders if p.id == existing_policy.policyholder_id),
            None
        )
        
        if not policyholder:
            # Generate a policyholder if not found
            policyholder = self.generate_policyholder()
            policyholder.id = existing_policy.policyholder_id
            
        # Renewal starts when current ends
        start_date = existing_policy.end_date
        end_date = start_date + timedelta(days=365)
        
        # Premium adjustment (usually increases)
        premium_adjustment = random.uniform(0.95, 1.15)
        new_premium = round(existing_policy.premium_amount * premium_adjustment, 2)
        
        # Create renewal
        renewal = Policy(
            policy_number=f"{existing_policy.policy_number}-R{datetime.now().year}",
            policyholder_id=existing_policy.policyholder_id,
            policy_type=existing_policy.policy_type,
            start_date=start_date,
            end_date=end_date,
            premium_amount=new_premium,
            sum_insured=existing_policy.sum_insured * random.uniform(0.98, 1.05),
            deductible=existing_policy.deductible,
            is_active=True,
            risk_profile=existing_policy.risk_profile,
            underwriter=existing_policy.underwriter,
            broker=existing_policy.broker
        )
        
        self.created_policies.append(renewal)
        return renewal