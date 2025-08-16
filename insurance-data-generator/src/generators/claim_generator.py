"""Generate synthetic insurance claims with fraud patterns."""

import random
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import sys
sys.path.append('../..')
from src.models import (
    Claim, Policy, Policyholder, ClaimType, ClaimStatus,
    FraudIndicator, PolicyType, Address
)
from src.generators.base_generator import BaseDataGenerator


class ClaimGenerator(BaseDataGenerator):
    """Generator for insurance claims with fraud injection."""
    
    # Claim type mapping by policy type
    CLAIM_TYPE_MAPPING = {
        PolicyType.MOTOR: [
            ClaimType.COLLISION,
            ClaimType.THEFT,
            ClaimType.LIABILITY_INJURY,
            ClaimType.LIABILITY_DAMAGE,
            ClaimType.OTHER
        ],
        PolicyType.PROPERTY: [
            ClaimType.FIRE,
            ClaimType.FLOOD,
            ClaimType.STORM,
            ClaimType.THEFT,
            ClaimType.LIABILITY_DAMAGE,
            ClaimType.OTHER
        ],
        PolicyType.LIABILITY: [
            ClaimType.LIABILITY_INJURY,
            ClaimType.LIABILITY_DAMAGE,
            ClaimType.OTHER
        ],
        PolicyType.PROFESSIONAL_INDEMNITY: [
            ClaimType.PROFESSIONAL_ERROR,
            ClaimType.LIABILITY_DAMAGE,
            ClaimType.OTHER
        ],
        PolicyType.CYBER: [
            ClaimType.CYBER_BREACH,
            ClaimType.LIABILITY_DAMAGE,
            ClaimType.OTHER
        ],
        PolicyType.MARINE: [
            ClaimType.COLLISION,
            ClaimType.STORM,
            ClaimType.THEFT,
            ClaimType.OTHER
        ],
        PolicyType.AVIATION: [
            ClaimType.COLLISION,
            ClaimType.LIABILITY_INJURY,
            ClaimType.LIABILITY_DAMAGE,
            ClaimType.OTHER
        ],
        PolicyType.DIRECTORS_OFFICERS: [
            ClaimType.PROFESSIONAL_ERROR,
            ClaimType.LIABILITY_DAMAGE,
            ClaimType.OTHER
        ]
    }
    
    # Claim amount ranges as percentage of sum insured
    CLAIM_AMOUNT_RANGES = {
        ClaimType.COLLISION: (0.1, 0.8),
        ClaimType.THEFT: (0.2, 1.0),
        ClaimType.FIRE: (0.3, 1.0),
        ClaimType.FLOOD: (0.2, 0.9),
        ClaimType.STORM: (0.1, 0.7),
        ClaimType.LIABILITY_INJURY: (0.05, 0.5),
        ClaimType.LIABILITY_DAMAGE: (0.1, 0.6),
        ClaimType.CYBER_BREACH: (0.2, 0.9),
        ClaimType.PROFESSIONAL_ERROR: (0.1, 0.8),
        ClaimType.OTHER: (0.05, 0.4)
    }
    
    # Description templates by claim type
    DESCRIPTION_TEMPLATES = {
        ClaimType.COLLISION: [
            "Vehicle collision at {location} involving {details}",
            "Road traffic accident on {location}, {details}",
            "Multi-vehicle collision, {details} at {location}"
        ],
        ClaimType.THEFT: [
            "Property stolen from {location}, {details}",
            "Burglary at {location}, items taken: {details}",
            "Vehicle theft from {location}, {details}"
        ],
        ClaimType.FIRE: [
            "Fire damage at {location}, cause: {details}",
            "Electrical fire at property, {details}",
            "Kitchen fire spreading to {location}, {details}"
        ],
        ClaimType.FLOOD: [
            "Flood damage from burst pipe at {location}, {details}",
            "Storm flooding affecting {location}, {details}",
            "River flooding damage, {details}"
        ],
        ClaimType.STORM: [
            "Storm damage to property at {location}, {details}",
            "Wind damage to roof, {details}",
            "Tree fallen due to storm, {details} at {location}"
        ],
        ClaimType.LIABILITY_INJURY: [
            "Personal injury claim, {details} at {location}",
            "Slip and fall accident at {location}, {details}",
            "Workplace injury, {details}"
        ],
        ClaimType.LIABILITY_DAMAGE: [
            "Property damage to third party at {location}, {details}",
            "Accidental damage caused, {details}",
            "Negligence claim for damage at {location}, {details}"
        ],
        ClaimType.CYBER_BREACH: [
            "Data breach affecting {details} records",
            "Ransomware attack, {details}",
            "System compromise, {details} at {location}"
        ],
        ClaimType.PROFESSIONAL_ERROR: [
            "Professional negligence claim, {details}",
            "Errors and omissions, {details}",
            "Breach of duty claim, {details}"
        ],
        ClaimType.OTHER: [
            "Miscellaneous claim at {location}, {details}",
            "Other incident type, {details}",
            "General claim for {details}"
        ]
    }
    
    def __init__(self, fraud_rate: float = 0.05):
        """Initialize claim generator.
        
        Args:
            fraud_rate: Percentage of claims to inject with fraud patterns
        """
        super().__init__()
        self.fraud_rate = fraud_rate
        
    def generate_claim_description(
        self,
        claim_type: ClaimType,
        location: str
    ) -> str:
        """Generate a claim description.
        
        Args:
            claim_type: Type of claim
            location: Location of incident
            
        Returns:
            Generated description
        """
        templates = self.DESCRIPTION_TEMPLATES.get(claim_type, ["Claim for {details}"])
        template = random.choice(templates)
        
        details = [
            "minor damage",
            "significant damage",
            "total loss",
            "partial damage",
            "extensive damage",
            "multiple items affected",
            "single item affected"
        ]
        
        return template.format(
            location=location,
            details=random.choice(details)
        )
    
    def calculate_claim_amount(
        self,
        claim_type: ClaimType,
        sum_insured: float,
        is_fraud: bool = False
    ) -> float:
        """Calculate claim amount.
        
        Args:
            claim_type: Type of claim
            sum_insured: Policy sum insured
            is_fraud: Whether this is a fraudulent claim
            
        Returns:
            Claim amount in GBP
        """
        min_pct, max_pct = self.CLAIM_AMOUNT_RANGES.get(
            claim_type,
            (0.1, 0.5)
        )
        
        if is_fraud:
            # Fraudulent claims tend to be inflated
            max_pct = min(max_pct * 1.5, 1.0)
            
        percentage = random.uniform(min_pct, max_pct)
        return round(sum_insured * percentage, 2)
    
    def inject_fraud_patterns(
        self,
        claim: Claim,
        policy: Policy,
        policyholder: Policyholder
    ) -> Claim:
        """Inject fraud patterns into a claim.
        
        Args:
            claim: Claim to modify
            policy: Related policy
            policyholder: Related policyholder
            
        Returns:
            Modified claim with fraud patterns
        """
        fraud_indicators = []
        fraud_score = 0.0
        
        # Pattern 1: Claim shortly after policy start
        days_since_start = (claim.incident_date - policy.start_date).days
        if days_since_start < 30:
            fraud_indicators.append(FraudIndicator.SUSPICIOUS_TIMING)
            fraud_score += 0.3
            
        # Pattern 2: Inflated amount
        expected_max = policy.sum_insured * 0.5
        if claim.claim_amount > expected_max:
            fraud_indicators.append(FraudIndicator.INFLATED_AMOUNT)
            fraud_score += 0.25
            
        # Pattern 3: Long reporting delay
        if claim.reporting_delay_days > 30:
            fraud_indicators.append(FraudIndicator.SUSPICIOUS_TIMING)
            fraud_score += 0.2
            
        # Pattern 4: High-risk policyholder
        if policyholder.risk_score > 0.7:
            fraud_score += 0.15
            
        # Pattern 5: Inconsistent details (random)
        if random.random() < 0.3:
            fraud_indicators.append(FraudIndicator.INCONSISTENT_DETAILS)
            fraud_score += 0.2
            claim.description += " [INCONSISTENCY DETECTED IN STATEMENT]"
            
        # Pattern 6: Network fraud ring (occasional)
        if random.random() < 0.1:
            fraud_indicators.append(FraudIndicator.NETWORK_FRAUD_RING)
            fraud_score += 0.4
            
        # Update claim
        claim.fraud_indicators = fraud_indicators
        claim.fraud_score = min(fraud_score, 1.0)
        claim.is_flagged = fraud_score > 0.6
        
        return claim
    
    def generate_claim(
        self,
        policy: Policy,
        policyholder: Optional[Policyholder] = None,
        is_fraud: bool = False,
        incident_date: Optional[datetime] = None
    ) -> Claim:
        """Generate a claim.
        
        Args:
            policy: Policy for the claim
            policyholder: Policyholder (will generate if not provided)
            is_fraud: Whether to inject fraud patterns
            incident_date: Date of incident (random if not provided)
            
        Returns:
            Generated Claim
        """
        # Get or generate policyholder
        if not policyholder:
            policyholder = self.generate_policyholder()
            policyholder.id = policy.policyholder_id
            
        # Select claim type based on policy type
        claim_types = self.CLAIM_TYPE_MAPPING.get(
            policy.policy_type,
            [ClaimType.OTHER]
        )
        claim_type = random.choice(claim_types)
        
        # Generate incident date
        if not incident_date:
            # Incident must be during policy period
            policy_days = (policy.end_date - policy.start_date).days
            if policy_days > 0:
                days_into_policy = random.randint(0, min(policy_days, 365))
                incident_date = policy.start_date + timedelta(days=days_into_policy)
            else:
                incident_date = policy.start_date
                
        # Reporting delay (most report quickly, some delay)
        reporting_delay = random.choices(
            [0, 1, 2, 3, 7, 14, 30, 60],
            weights=[30, 25, 15, 10, 10, 5, 3, 2]
        )[0]
        reported_date = incident_date + timedelta(days=reporting_delay)
        
        # Generate location
        location = self.generate_address()
        
        # Calculate claim amount
        claim_amount = self.calculate_claim_amount(
            claim_type,
            policy.sum_insured,
            is_fraud
        )
        
        # Generate status
        if reported_date > datetime.now():
            status = ClaimStatus.SUBMITTED
        else:
            status = random.choice(list(ClaimStatus))
            
        # Create claim
        claim = Claim(
            claim_number=self.faker.claim_number(),
            policy_id=policy.id,
            policyholder_id=policy.policyholder_id,
            claim_type=claim_type,
            status=status,
            incident_date=incident_date,
            reported_date=reported_date,
            claim_amount=claim_amount,
            approved_amount=claim_amount * random.uniform(0.7, 1.0) if status == ClaimStatus.APPROVED else None,
            description=self.generate_claim_description(claim_type, location.city),
            location=location,
            adjuster_notes=self.faker.text(max_nb_chars=200) if status != ClaimStatus.SUBMITTED else None
        )
        
        # Calculate processing time for closed claims
        if status in [ClaimStatus.PAID, ClaimStatus.CLOSED, ClaimStatus.REJECTED]:
            claim.processing_time_hours = random.uniform(24, 720)  # 1-30 days
            if status == ClaimStatus.PAID:
                claim.paid_at = reported_date + timedelta(hours=claim.processing_time_hours)
                
        # Inject fraud patterns if requested
        if is_fraud or random.random() < self.fraud_rate:
            claim = self.inject_fraud_patterns(claim, policy, policyholder)
            
        self.created_claims.append(claim)
        return claim
    
    def generate_claims(
        self,
        count: int,
        policies: Optional[List[Policy]] = None
    ) -> List[Claim]:
        """Generate multiple claims.
        
        Args:
            count: Number of claims to generate
            policies: List of policies to use (will generate if not provided)
            
        Returns:
            List of generated claims
        """
        claims = []
        
        for i in range(count):
            # Select or generate a policy
            if policies:
                policy = random.choice(policies)
            else:
                from src.generators.policy_generator import PolicyGenerator
                pg = PolicyGenerator()
                policy = pg.generate_policy()
                
            # Determine if this should be fraudulent
            is_fraud = random.random() < self.fraud_rate
            
            claim = self.generate_claim(policy, is_fraud=is_fraud)
            claims.append(claim)
            
        return claims
    
    def generate_fraud_ring(
        self,
        size: int = 5,
        policy_type: PolicyType = PolicyType.MOTOR
    ) -> List[Claim]:
        """Generate a network of related fraudulent claims.
        
        Args:
            size: Number of claims in the ring
            policy_type: Type of policies involved
            
        Returns:
            List of related fraudulent claims
        """
        claims = []
        
        # Create related policyholders (same postcode area)
        base_address = self.generate_address()
        base_postcode_area = base_address.postcode.split()[0][:-1]
        
        # Similar incident details
        incident_date = datetime.now() - timedelta(days=random.randint(1, 30))
        claim_type = random.choice(self.CLAIM_TYPE_MAPPING[policy_type])
        
        for _ in range(size):
            # Generate related policyholder
            policyholder = self.generate_policyholder()
            address = policyholder.address
            # Same postcode area
            address.postcode = f"{base_postcode_area}{random.randint(1, 9)} {random.randint(0, 9)}AA"
            
            # Generate policy
            from src.generators.policy_generator import PolicyGenerator
            pg = PolicyGenerator()
            policy = pg.generate_policy(policyholder, policy_type)
            
            # Generate claim with similar details
            claim = self.generate_claim(
                policy,
                policyholder,
                is_fraud=True,
                incident_date=incident_date + timedelta(days=random.randint(-2, 2))
            )
            
            # Add network indicator
            claim.fraud_indicators.append(FraudIndicator.NETWORK_FRAUD_RING)
            claim.fraud_score = min(claim.fraud_score + 0.3, 1.0)
            claim.is_flagged = True
            
            claims.append(claim)
            
        return claims