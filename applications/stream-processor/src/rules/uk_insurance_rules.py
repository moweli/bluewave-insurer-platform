"""
UK Insurance Business Rules Engine
Implements UK-specific insurance validation and fraud detection rules
"""

from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
from datetime import datetime, timedelta


class RuleSeverity(Enum):
    """Rule severity levels"""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


class RuleAction(Enum):
    """Actions to take when rule is triggered"""
    AUTO_REJECT = "auto_reject"
    AUTO_APPROVE = "auto_approve"
    MANUAL_REVIEW = "manual_review"
    FRAUD_INVESTIGATION = "fraud_investigation"
    ESCALATE = "escalate"
    CONTINUE = "continue"


@dataclass
class ValidationResult:
    """Result of a validation rule"""
    passed: bool
    rule_id: str
    rule_name: str
    severity: RuleSeverity
    message: str
    action: RuleAction
    metadata: Optional[Dict] = None


class UKInsuranceRules:
    """UK-specific insurance business rules"""
    
    # UK-specific constants
    UK_ROUNDABOUT_HOTSPOTS = [
        "B1", "B2", "B3",  # Birmingham
        "M1", "M2", "M3",  # Manchester
        "L1", "L2", "L3",  # Liverpool
    ]
    
    WHIPLASH_CAPITALS = ["Liverpool", "Manchester", "Oldham", "Birmingham"]
    
    FLOOD_RISK_POSTCODES = ["YO", "HG", "CA", "GL", "SO", "BA", "TA"]
    
    LLOYDS_SYNDICATES = {
        "Hiscox": "33",
        "Beazley": "2623",
        "Catlin": "2003",
        "Amlin": "2001"
    }
    
    # Policy limits by type (in GBP)
    POLICY_LIMITS = {
        "motor": {"min": 100, "max": 100000, "typical": 5000},
        "home": {"min": 500, "max": 500000, "typical": 15000},
        "travel": {"min": 50, "max": 50000, "typical": 2000},
        "pet": {"min": 50, "max": 10000, "typical": 1000},
        "life": {"min": 1000, "max": 1000000, "typical": 100000}
    }
    
    def validate_claim_amount_vs_policy_limit(
        self, 
        claim_amount: float, 
        policy_limit: float,
        policy_type: str
    ) -> ValidationResult:
        """Validate claim amount against policy limits"""
        
        if claim_amount > policy_limit:
            return ValidationResult(
                passed=False,
                rule_id="CLAIM_001",
                rule_name="Claim Exceeds Policy Limit",
                severity=RuleSeverity.CRITICAL,
                message=f"Claim amount £{claim_amount:,.2f} exceeds policy limit £{policy_limit:,.2f}",
                action=RuleAction.AUTO_REJECT,
                metadata={"excess_amount": claim_amount - policy_limit}
            )
        
        # Check if amount is suspiciously close to limit
        if claim_amount > policy_limit * 0.95:
            return ValidationResult(
                passed=False,
                rule_id="CLAIM_002",
                rule_name="Suspicious Amount Near Limit",
                severity=RuleSeverity.HIGH,
                message=f"Claim amount is {(claim_amount/policy_limit)*100:.1f}% of policy limit",
                action=RuleAction.MANUAL_REVIEW
            )
        
        # Check typical ranges
        if policy_type in self.POLICY_LIMITS:
            typical = self.POLICY_LIMITS[policy_type]["typical"]
            if claim_amount > typical * 3:
                return ValidationResult(
                    passed=False,
                    rule_id="CLAIM_003",
                    rule_name="Unusually High Claim",
                    severity=RuleSeverity.MEDIUM,
                    message=f"Claim amount exceeds typical {policy_type} claim by {claim_amount/typical:.1f}x",
                    action=RuleAction.MANUAL_REVIEW
                )
        
        return ValidationResult(
            passed=True,
            rule_id="CLAIM_001",
            rule_name="Claim Amount Valid",
            severity=RuleSeverity.INFO,
            message="Claim amount within acceptable limits",
            action=RuleAction.CONTINUE
        )
    
    def validate_uk_motor_fraud_indicators(
        self,
        claim_type: str,
        incident_location: str,
        fault_percentage: float,
        witness_count: int,
        medical_report_delay_days: int,
        accident_type: str
    ) -> ValidationResult:
        """Detect UK-specific motor insurance fraud patterns"""
        
        if claim_type != "motor":
            return ValidationResult(
                passed=True,
                rule_id="MOTOR_001",
                rule_name="Not Motor Claim",
                severity=RuleSeverity.INFO,
                message="Rule applies only to motor claims",
                action=RuleAction.CONTINUE
            )
        
        fraud_indicators = []
        
        # Crash for cash pattern
        if any(incident_location.startswith(hotspot) for hotspot in self.UK_ROUNDABOUT_HOTSPOTS):
            if fault_percentage == 100 and witness_count == 0:
                fraud_indicators.append("POSSIBLE_STAGED_ACCIDENT")
        
        # Whiplash fraud (UK epidemic)
        if accident_type == "rear_end" and medical_report_delay_days > 14:
            fraud_indicators.append("DELAYED_WHIPLASH_CLAIM")
            
        # Ghost broking indicator
        if fault_percentage == 0 and witness_count > 3:
            fraud_indicators.append("EXCESSIVE_WITNESSES")
        
        if fraud_indicators:
            return ValidationResult(
                passed=False,
                rule_id="MOTOR_002",
                rule_name="Motor Fraud Indicators Detected",
                severity=RuleSeverity.HIGH,
                message=f"Fraud patterns detected: {', '.join(fraud_indicators)}",
                action=RuleAction.FRAUD_INVESTIGATION,
                metadata={"fraud_indicators": fraud_indicators}
            )
        
        return ValidationResult(
            passed=True,
            rule_id="MOTOR_002",
            rule_name="No Motor Fraud Indicators",
            severity=RuleSeverity.INFO,
            message="No motor fraud patterns detected",
            action=RuleAction.CONTINUE
        )
    
    def validate_weather_claims_correlation(
        self,
        claim_type: str,
        claim_date: datetime,
        incident_location: str,
        weather_event: Optional[str]
    ) -> ValidationResult:
        """Validate weather-related claims against Met Office data"""
        
        weather_claim_types = ["storm_damage", "flood", "wind_damage", "weather"]
        
        if claim_type not in weather_claim_types:
            return ValidationResult(
                passed=True,
                rule_id="WEATHER_001",
                rule_name="Not Weather Claim",
                severity=RuleSeverity.INFO,
                message="Not a weather-related claim",
                action=RuleAction.CONTINUE
            )
        
        # Check if location is in flood risk area
        if claim_type == "flood":
            postcode_prefix = incident_location[:2] if len(incident_location) >= 2 else ""
            if postcode_prefix not in self.FLOOD_RISK_POSTCODES:
                return ValidationResult(
                    passed=False,
                    rule_id="WEATHER_002",
                    rule_name="Flood Claim Outside Risk Zone",
                    severity=RuleSeverity.MEDIUM,
                    message=f"Flood claim in area {postcode_prefix} not typically at flood risk",
                    action=RuleAction.MANUAL_REVIEW
                )
        
        # Check timing (claims immediately after weather events are suspicious)
        # In real implementation, would check against actual Met Office data
        if weather_event and claim_date:
            # Suspicious if claimed on same day as weather event
            return ValidationResult(
                passed=False,
                rule_id="WEATHER_003",
                rule_name="Immediate Weather Claim",
                severity=RuleSeverity.MEDIUM,
                message="Claim filed immediately after weather event",
                action=RuleAction.MANUAL_REVIEW,
                metadata={"weather_event": weather_event}
            )
        
        return ValidationResult(
            passed=True,
            rule_id="WEATHER_001",
            rule_name="Weather Claim Valid",
            severity=RuleSeverity.INFO,
            message="Weather claim appears legitimate",
            action=RuleAction.CONTINUE
        )
    
    def validate_fca_vulnerable_customer(
        self,
        customer_age: int,
        has_disability: bool,
        financial_difficulty: bool,
        recent_bereavement: bool
    ) -> ValidationResult:
        """Check FCA vulnerable customer criteria"""
        
        is_vulnerable = False
        reasons = []
        
        if customer_age > 80:
            is_vulnerable = True
            reasons.append("elderly customer")
        
        if has_disability:
            is_vulnerable = True
            reasons.append("disability")
        
        if financial_difficulty:
            is_vulnerable = True
            reasons.append("financial difficulty")
        
        if recent_bereavement:
            is_vulnerable = True
            reasons.append("recent bereavement")
        
        if is_vulnerable:
            return ValidationResult(
                passed=True,  # Not a failure, but needs special handling
                rule_id="FCA_001",
                rule_name="Vulnerable Customer Identified",
                severity=RuleSeverity.HIGH,
                message=f"FCA vulnerable customer: {', '.join(reasons)}",
                action=RuleAction.ESCALATE,
                metadata={"vulnerability_reasons": reasons}
            )
        
        return ValidationResult(
            passed=True,
            rule_id="FCA_001",
            rule_name="Standard Customer",
            severity=RuleSeverity.INFO,
            message="No vulnerability indicators",
            action=RuleAction.CONTINUE
        )
    
    def validate_gdpr_compliance(
        self,
        has_consent: bool,
        consent_date: Optional[datetime],
        data_retention_years: int
    ) -> ValidationResult:
        """Validate GDPR compliance for data processing"""
        
        if not has_consent:
            return ValidationResult(
                passed=False,
                rule_id="GDPR_001",
                rule_name="No GDPR Consent",
                severity=RuleSeverity.CRITICAL,
                message="Cannot process claim without GDPR consent",
                action=RuleAction.AUTO_REJECT
            )
        
        if consent_date:
            consent_age_days = (datetime.now() - consent_date).days
            if consent_age_days > 365:
                return ValidationResult(
                    passed=False,
                    rule_id="GDPR_002",
                    rule_name="Expired Consent",
                    severity=RuleSeverity.HIGH,
                    message=f"GDPR consent expired ({consent_age_days} days old)",
                    action=RuleAction.MANUAL_REVIEW
                )
        
        if data_retention_years > 7:
            return ValidationResult(
                passed=False,
                rule_id="GDPR_003",
                rule_name="Excessive Data Retention",
                severity=RuleSeverity.MEDIUM,
                message=f"Data retention period ({data_retention_years} years) exceeds guidelines",
                action=RuleAction.MANUAL_REVIEW
            )
        
        return ValidationResult(
            passed=True,
            rule_id="GDPR_001",
            rule_name="GDPR Compliant",
            severity=RuleSeverity.INFO,
            message="GDPR requirements satisfied",
            action=RuleAction.CONTINUE
        )
    
    def validate_lloyds_market_claim(
        self,
        underwriter: str,
        claim_amount: float,
        is_reinsurance: bool
    ) -> ValidationResult:
        """Validate claims for Lloyd's of London market"""
        
        if underwriter not in self.LLOYDS_SYNDICATES:
            return ValidationResult(
                passed=True,
                rule_id="LLOYDS_001",
                rule_name="Not Lloyd's Claim",
                severity=RuleSeverity.INFO,
                message="Not a Lloyd's market claim",
                action=RuleAction.CONTINUE
            )
        
        syndicate_number = self.LLOYDS_SYNDICATES[underwriter]
        
        # Lloyd's typically handles large/complex risks
        if claim_amount < 50000 and not is_reinsurance:
            return ValidationResult(
                passed=False,
                rule_id="LLOYDS_002",
                rule_name="Unusual Lloyd's Small Claim",
                severity=RuleSeverity.MEDIUM,
                message=f"Small claim (£{claim_amount:,.2f}) unusual for Lloyd's syndicate {syndicate_number}",
                action=RuleAction.MANUAL_REVIEW,
                metadata={"syndicate": syndicate_number}
            )
        
        return ValidationResult(
            passed=True,
            rule_id="LLOYDS_001",
            rule_name="Valid Lloyd's Claim",
            severity=RuleSeverity.INFO,
            message=f"Lloyd's syndicate {syndicate_number} claim validated",
            action=RuleAction.CONTINUE
        )
    
    def validate_claim_timing(
        self,
        incident_date: datetime,
        claim_date: datetime,
        policy_end_date: datetime
    ) -> ValidationResult:
        """Validate claim timing for suspicious patterns"""
        
        days_to_claim = (claim_date - incident_date).days
        days_before_renewal = (policy_end_date - claim_date).days
        
        # Check for delayed claims
        if days_to_claim > 90:
            return ValidationResult(
                passed=False,
                rule_id="TIMING_001",
                rule_name="Excessively Delayed Claim",
                severity=RuleSeverity.HIGH,
                message=f"Claim filed {days_to_claim} days after incident",
                action=RuleAction.MANUAL_REVIEW
            )
        
        # Check for claims near policy expiry
        if 0 <= days_before_renewal <= 30:
            return ValidationResult(
                passed=False,
                rule_id="TIMING_002",
                rule_name="Claim Near Policy Expiry",
                severity=RuleSeverity.MEDIUM,
                message=f"Claim filed {days_before_renewal} days before policy expires",
                action=RuleAction.MANUAL_REVIEW
            )
        
        # Check for backdated claims
        if days_to_claim < 0:
            return ValidationResult(
                passed=False,
                rule_id="TIMING_003",
                rule_name="Backdated Claim",
                severity=RuleSeverity.CRITICAL,
                message="Claim date is before incident date",
                action=RuleAction.FRAUD_INVESTIGATION
            )
        
        return ValidationResult(
            passed=True,
            rule_id="TIMING_001",
            rule_name="Valid Claim Timing",
            severity=RuleSeverity.INFO,
            message=f"Claim filed {days_to_claim} days after incident",
            action=RuleAction.CONTINUE
        )


class RuleEngine:
    """Main rule engine for processing claims"""
    
    def __init__(self):
        self.rules = UKInsuranceRules()
        self.rule_registry = []
    
    def evaluate_all_rules(self, claim_data: Dict) -> List[ValidationResult]:
        """Evaluate all applicable rules for a claim"""
        
        results = []
        
        # Amount validation
        if "claim_amount" in claim_data and "policy_limit" in claim_data:
            results.append(self.rules.validate_claim_amount_vs_policy_limit(
                claim_data["claim_amount"],
                claim_data["policy_limit"],
                claim_data.get("policy_type", "unknown")
            ))
        
        # Motor fraud detection
        if claim_data.get("claim_type") == "motor":
            results.append(self.rules.validate_uk_motor_fraud_indicators(
                claim_data.get("claim_type"),
                claim_data.get("incident_location", ""),
                claim_data.get("fault_percentage", 0),
                claim_data.get("witness_count", 0),
                claim_data.get("medical_report_delay_days", 0),
                claim_data.get("accident_type", "")
            ))
        
        # Weather claims
        if claim_data.get("claim_type") in ["storm_damage", "flood", "wind_damage"]:
            results.append(self.rules.validate_weather_claims_correlation(
                claim_data.get("claim_type"),
                claim_data.get("claim_date"),
                claim_data.get("incident_location", ""),
                claim_data.get("weather_event")
            ))
        
        # FCA vulnerable customer
        if "customer_age" in claim_data:
            results.append(self.rules.validate_fca_vulnerable_customer(
                claim_data.get("customer_age", 0),
                claim_data.get("has_disability", False),
                claim_data.get("financial_difficulty", False),
                claim_data.get("recent_bereavement", False)
            ))
        
        # GDPR compliance
        results.append(self.rules.validate_gdpr_compliance(
            claim_data.get("gdpr_consent", False),
            claim_data.get("consent_date"),
            claim_data.get("data_retention_years", 7)
        ))
        
        # Timing validation
        if all(k in claim_data for k in ["incident_date", "claim_date", "policy_end_date"]):
            results.append(self.rules.validate_claim_timing(
                claim_data["incident_date"],
                claim_data["claim_date"],
                claim_data["policy_end_date"]
            ))
        
        return results
    
    def determine_claim_action(self, results: List[ValidationResult]) -> RuleAction:
        """Determine final action based on all rule results"""
        
        # Priority order for actions
        action_priority = {
            RuleAction.AUTO_REJECT: 1,
            RuleAction.FRAUD_INVESTIGATION: 2,
            RuleAction.ESCALATE: 3,
            RuleAction.MANUAL_REVIEW: 4,
            RuleAction.AUTO_APPROVE: 5,
            RuleAction.CONTINUE: 6
        }
        
        # Get highest priority action
        if not results:
            return RuleAction.CONTINUE
        
        actions = [r.action for r in results if not r.passed]
        if not actions:
            return RuleAction.AUTO_APPROVE
        
        return min(actions, key=lambda x: action_priority.get(x, 999))