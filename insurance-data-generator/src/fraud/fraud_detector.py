"""Fraud detection and pattern analysis system."""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple, Optional
from collections import defaultdict
import numpy as np
from scipy import stats
import sys
sys.path.append('../..')
from src.models import Claim, Policy, Policyholder, FraudIndicator


logger = logging.getLogger(__name__)


class FraudDetector:
    """Advanced fraud detection system using multiple signals."""
    
    def __init__(self):
        """Initialize the fraud detector."""
        self.claim_history = defaultdict(list)
        self.policyholder_scores = {}
        self.network_map = defaultdict(set)
        self.blacklist = set()
        self.fraud_thresholds = {
            'low': 0.3,
            'medium': 0.6,
            'high': 0.8,
            'critical': 0.9
        }
        
    def analyze_claim(
        self,
        claim: Claim,
        policy: Policy,
        policyholder: Policyholder,
        historical_claims: Optional[List[Claim]] = None
    ) -> Dict[str, Any]:
        """Comprehensive fraud analysis of a claim.
        
        Args:
            claim: Claim to analyze
            policy: Related policy
            policyholder: Related policyholder
            historical_claims: Previous claims by policyholder
            
        Returns:
            Fraud analysis results
        """
        indicators = []
        signals = {}
        base_score = 0.0
        
        # 1. Timing Analysis
        timing_score, timing_indicators = self._analyze_timing(claim, policy)
        base_score += timing_score * 0.25
        indicators.extend(timing_indicators)
        signals['timing'] = timing_score
        
        # 2. Amount Analysis
        amount_score, amount_indicators = self._analyze_amount(claim, policy)
        base_score += amount_score * 0.20
        indicators.extend(amount_indicators)
        signals['amount'] = amount_score
        
        # 3. Frequency Analysis
        if historical_claims:
            freq_score, freq_indicators = self._analyze_frequency(
                claim, historical_claims
            )
            base_score += freq_score * 0.15
            indicators.extend(freq_indicators)
            signals['frequency'] = freq_score
            
        # 4. Geographic Analysis
        geo_score = self._analyze_geographic_patterns(claim, policyholder)
        base_score += geo_score * 0.10
        signals['geographic'] = geo_score
        
        # 5. Policyholder Risk
        risk_score = self._analyze_policyholder_risk(policyholder)
        base_score += risk_score * 0.15
        signals['policyholder_risk'] = risk_score
        
        # 6. Network Analysis
        network_score = self._analyze_network_patterns(policyholder)
        base_score += network_score * 0.10
        signals['network'] = network_score
        
        # 7. Blacklist Check
        if self._check_blacklist(policyholder):
            indicators.append(FraudIndicator.BLACKLISTED_ENTITY)
            base_score += 0.5
            signals['blacklisted'] = True
            
        # 8. Text Analysis (simple keyword detection)
        text_score = self._analyze_claim_text(claim.description)
        base_score += text_score * 0.05
        signals['text_analysis'] = text_score
        
        # Normalize score
        final_score = min(base_score, 1.0)
        
        # Determine risk level
        risk_level = self._get_risk_level(final_score)
        
        # Build analysis result
        analysis = {
            'fraud_score': final_score,
            'risk_level': risk_level,
            'fraud_indicators': list(set(indicators)),
            'signals': signals,
            'is_flagged': final_score > self.fraud_thresholds['medium'],
            'confidence': self._calculate_confidence(signals),
            'recommended_action': self._recommend_action(final_score, risk_level),
            'analysis_timestamp': datetime.utcnow()
        }
        
        # Update internal tracking
        self._update_tracking(claim, policyholder, final_score)
        
        return analysis
    
    def _analyze_timing(
        self,
        claim: Claim,
        policy: Policy
    ) -> Tuple[float, List[FraudIndicator]]:
        """Analyze timing-related fraud signals.
        
        Args:
            claim: Claim to analyze
            policy: Related policy
            
        Returns:
            Timing score and indicators
        """
        score = 0.0
        indicators = []
        
        # Early claim (within first 30 days)
        days_since_start = (claim.incident_date - policy.start_date).days
        if days_since_start < 30:
            score += 0.6
            indicators.append(FraudIndicator.SUSPICIOUS_TIMING)
        elif days_since_start < 60:
            score += 0.3
            
        # Late reporting
        if claim.reporting_delay_days > 30:
            score += 0.3
            indicators.append(FraudIndicator.SUSPICIOUS_TIMING)
        elif claim.reporting_delay_days > 14:
            score += 0.1
            
        # Claim near policy end
        days_to_end = (policy.end_date - claim.incident_date).days
        if days_to_end < 30:
            score += 0.2
            
        # Weekend/holiday claims (simplified)
        if claim.incident_date.weekday() >= 5:  # Weekend
            score += 0.1
            
        return min(score, 1.0), indicators
    
    def _analyze_amount(
        self,
        claim: Claim,
        policy: Policy
    ) -> Tuple[float, List[FraudIndicator]]:
        """Analyze claim amount for fraud signals.
        
        Args:
            claim: Claim to analyze
            policy: Related policy
            
        Returns:
            Amount score and indicators
        """
        score = 0.0
        indicators = []
        
        # Claim to sum insured ratio
        ratio = claim.claim_amount / policy.sum_insured if policy.sum_insured > 0 else 0
        
        if ratio > 0.8:
            score += 0.5
            indicators.append(FraudIndicator.INFLATED_AMOUNT)
        elif ratio > 0.6:
            score += 0.3
        elif ratio > 0.4:
            score += 0.1
            
        # Just below deductible (suspicious pattern)
        if claim.claim_amount < policy.deductible * 1.1:
            score += 0.2
            
        # Round numbers (e.g., £5000, £10000)
        if claim.claim_amount % 1000 == 0:
            score += 0.1
        elif claim.claim_amount % 500 == 0:
            score += 0.05
            
        return min(score, 1.0), indicators
    
    def _analyze_frequency(
        self,
        current_claim: Claim,
        historical_claims: List[Claim]
    ) -> Tuple[float, List[FraudIndicator]]:
        """Analyze claim frequency patterns.
        
        Args:
            current_claim: Current claim
            historical_claims: Historical claims
            
        Returns:
            Frequency score and indicators
        """
        score = 0.0
        indicators = []
        
        # Count claims in last year
        one_year_ago = datetime.now() - timedelta(days=365)
        recent_claims = [
            c for c in historical_claims
            if c.incident_date > one_year_ago
        ]
        
        if len(recent_claims) > 5:
            score += 0.6
            indicators.append(FraudIndicator.UNUSUAL_FREQUENCY)
        elif len(recent_claims) > 3:
            score += 0.3
        elif len(recent_claims) > 2:
            score += 0.1
            
        # Check for duplicate claims
        for claim in recent_claims:
            if (
                claim.claim_type == current_claim.claim_type and
                abs((claim.incident_date - current_claim.incident_date).days) < 7
            ):
                score += 0.5
                indicators.append(FraudIndicator.DUPLICATE_CLAIM)
                break
                
        return min(score, 1.0), indicators
    
    def _analyze_geographic_patterns(
        self,
        claim: Claim,
        policyholder: Policyholder
    ) -> float:
        """Analyze geographic patterns for fraud.
        
        Args:
            claim: Claim to analyze
            policyholder: Related policyholder
            
        Returns:
            Geographic risk score
        """
        score = 0.0
        
        # Check if claim location is far from policyholder address
        claim_postcode = claim.location.postcode[:3] if claim.location.postcode else ""
        holder_postcode = policyholder.address.postcode[:3] if policyholder.address.postcode else ""
        
        if claim_postcode and holder_postcode and claim_postcode != holder_postcode:
            score += 0.3
            
        # High-risk areas (simplified)
        high_risk_areas = ['E', 'SE', 'EC', 'WC', 'B', 'M', 'L']
        if any(claim.location.postcode.startswith(area) for area in high_risk_areas):
            score += 0.2
            
        return min(score, 1.0)
    
    def _analyze_policyholder_risk(self, policyholder: Policyholder) -> float:
        """Analyze policyholder risk profile.
        
        Args:
            policyholder: Policyholder to analyze
            
        Returns:
            Risk score
        """
        base_score = policyholder.risk_score
        
        # Age factor (very young or old)
        age = policyholder.age
        if age < 25:
            base_score += 0.1
        elif age > 75:
            base_score += 0.05
            
        # Previous scores
        if policyholder.id in self.policyholder_scores:
            avg_score = np.mean(self.policyholder_scores[policyholder.id])
            base_score = (base_score + avg_score) / 2
            
        return min(base_score, 1.0)
    
    def _analyze_network_patterns(self, policyholder: Policyholder) -> float:
        """Analyze network connections for fraud rings.
        
        Args:
            policyholder: Policyholder to analyze
            
        Returns:
            Network risk score
        """
        if policyholder.id not in self.network_map:
            return 0.0
            
        connections = self.network_map[policyholder.id]
        
        # Check if connected to known fraudsters
        fraud_connections = sum(
            1 for conn in connections
            if conn in self.policyholder_scores and
            np.mean(self.policyholder_scores[conn]) > 0.7
        )
        
        if fraud_connections > 2:
            return 0.8
        elif fraud_connections > 0:
            return 0.4
            
        return 0.0
    
    def _analyze_claim_text(self, description: str) -> float:
        """Simple text analysis for suspicious patterns.
        
        Args:
            description: Claim description
            
        Returns:
            Text suspicion score
        """
        suspicious_keywords = [
            'total loss', 'complete damage', 'everything destroyed',
            'no witnesses', 'cannot remember', 'stolen', 'disappeared',
            'no evidence', 'all items'
        ]
        
        description_lower = description.lower()
        matches = sum(
            1 for keyword in suspicious_keywords
            if keyword in description_lower
        )
        
        return min(matches * 0.2, 1.0)
    
    def _check_blacklist(self, policyholder: Policyholder) -> bool:
        """Check if policyholder is blacklisted.
        
        Args:
            policyholder: Policyholder to check
            
        Returns:
            True if blacklisted
        """
        return (
            policyholder.id in self.blacklist or
            policyholder.email in self.blacklist or
            (policyholder.company_registration and 
             policyholder.company_registration in self.blacklist)
        )
    
    def _get_risk_level(self, score: float) -> str:
        """Convert score to risk level.
        
        Args:
            score: Fraud score
            
        Returns:
            Risk level string
        """
        if score >= self.fraud_thresholds['critical']:
            return 'CRITICAL'
        elif score >= self.fraud_thresholds['high']:
            return 'HIGH'
        elif score >= self.fraud_thresholds['medium']:
            return 'MEDIUM'
        elif score >= self.fraud_thresholds['low']:
            return 'LOW'
        else:
            return 'MINIMAL'
    
    def _calculate_confidence(self, signals: Dict[str, Any]) -> float:
        """Calculate confidence in fraud detection.
        
        Args:
            signals: Dictionary of signal scores
            
        Returns:
            Confidence score
        """
        # More signals = higher confidence
        active_signals = sum(1 for v in signals.values() if v > 0.3)
        confidence = min(active_signals * 0.2, 1.0)
        
        # Adjust for consistency
        if signals:
            scores = [v for v in signals.values() if isinstance(v, (int, float))]
            if scores:
                std_dev = np.std(scores)
                confidence *= (1 - std_dev)  # Lower std = higher confidence
                
        return confidence
    
    def _recommend_action(self, score: float, risk_level: str) -> str:
        """Recommend action based on fraud analysis.
        
        Args:
            score: Fraud score
            risk_level: Risk level
            
        Returns:
            Recommended action
        """
        if risk_level == 'CRITICAL':
            return 'IMMEDIATE_INVESTIGATION_REQUIRED'
        elif risk_level == 'HIGH':
            return 'MANUAL_REVIEW_REQUIRED'
        elif risk_level == 'MEDIUM':
            return 'ADDITIONAL_DOCUMENTATION_REQUIRED'
        elif risk_level == 'LOW':
            return 'STANDARD_PROCESSING_WITH_MONITORING'
        else:
            return 'STANDARD_PROCESSING'
    
    def _update_tracking(
        self,
        claim: Claim,
        policyholder: Policyholder,
        score: float
    ):
        """Update internal tracking data.
        
        Args:
            claim: Processed claim
            policyholder: Related policyholder
            score: Fraud score
        """
        # Update claim history
        self.claim_history[policyholder.id].append(claim)
        
        # Update policyholder scores
        if policyholder.id not in self.policyholder_scores:
            self.policyholder_scores[policyholder.id] = []
        self.policyholder_scores[policyholder.id].append(score)
        
        # Add to blacklist if consistently fraudulent
        if len(self.policyholder_scores[policyholder.id]) >= 3:
            avg_score = np.mean(self.policyholder_scores[policyholder.id][-3:])
            if avg_score > 0.8:
                self.blacklist.add(policyholder.id)
                logger.warning(f"Policyholder {policyholder.id} added to blacklist")
    
    def add_to_network(self, policyholder_id: str, connected_ids: List[str]):
        """Add network connections for fraud ring detection.
        
        Args:
            policyholder_id: Primary policyholder
            connected_ids: Connected policyholders
        """
        self.network_map[policyholder_id].update(connected_ids)
        for conn_id in connected_ids:
            self.network_map[conn_id].add(policyholder_id)
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get fraud detection statistics.
        
        Returns:
            Statistics dictionary
        """
        total_analyzed = sum(len(scores) for scores in self.policyholder_scores.values())
        high_risk_count = sum(
            1 for scores in self.policyholder_scores.values()
            if scores and np.mean(scores) > self.fraud_thresholds['high']
        )
        
        return {
            'total_claims_analyzed': total_analyzed,
            'unique_policyholders': len(self.policyholder_scores),
            'high_risk_policyholders': high_risk_count,
            'blacklisted_entities': len(self.blacklist),
            'network_connections': sum(len(conns) for conns in self.network_map.values()),
            'average_fraud_score': np.mean([
                score for scores in self.policyholder_scores.values()
                for score in scores
            ]) if total_analyzed > 0 else 0
        }