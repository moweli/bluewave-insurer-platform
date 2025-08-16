#!/usr/bin/env python3
"""Non-interactive demo of fraud detection scenarios."""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.fraud.fraud_scenarios import FraudScenarioGenerator
from src.fraud.fraud_detector import FraudDetector
from src.utils.uk_postcode_profiler import UKPostcodeProfiler


def print_separator(title: str):
    """Print a formatted separator."""
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print('=' * 60)


def run_all_demos():
    """Run all fraud detection demos."""
    print("\n" + "=" * 60)
    print(" " * 10 + "BLUEWAVE INSURANCE FRAUD DETECTION")
    print(" " * 15 + "Automated Demo Suite")
    print("=" * 60)
    
    generator = FraudScenarioGenerator()
    detector = FraudDetector()
    profiler = UKPostcodeProfiler()
    
    # 1. Early Claim Fraud
    print_separator("1. EARLY CLAIM FRAUD")
    scenario = generator.generate_early_claim_fraud()
    claim = scenario['claim']
    policy = scenario['policy']
    policyholder = scenario['policyholder']
    
    days_since_start = (claim.incident_date - policy.start_date).days
    print(f"\n📋 Scenario: {scenario['description']}")
    print(f"⚠️  Claim filed {days_since_start} days after policy inception")
    print(f"💰 Claim amount: £{claim.claim_amount:,.2f}")
    print(f"🎯 Fraud score: {claim.fraud_score:.2%}")
    print(f"📍 Location: {claim.location.city}, {claim.location.postcode}")
    
    # 2. Inflated Claim
    print_separator("2. INFLATED CLAIM FRAUD")
    scenario = generator.generate_inflated_claim_fraud()
    claim = scenario['claim']
    policy = scenario['policy']
    
    ratio = (claim.claim_amount / policy.sum_insured) * 100
    print(f"\n📋 Scenario: {scenario['description']}")
    print(f"💰 Claim amount: £{claim.claim_amount:,.2f}")
    print(f"📊 Claim is {ratio:.1f}% of sum insured (£{policy.sum_insured:,.2f})")
    print(f"🎯 Fraud score: {claim.fraud_score:.2%}")
    
    # 3. Fraud Ring
    print_separator("3. FRAUD RING DETECTION")
    scenario = generator.generate_fraud_ring(size=4)
    claims = scenario['claims']
    policyholders = scenario['policyholders']
    
    print(f"\n📋 Scenario: Network of {len(claims)} related fraudulent claims")
    print("\n🔗 Ring Members:")
    for i, claim in enumerate(claims):
        ph = policyholders[i]
        print(f"  #{i+1}: {ph.first_name} {ph.last_name} - £{claim.claim_amount:,.2f} - {ph.address.postcode}")
    
    postcodes = [p.address.postcode for p in policyholders]
    geo_analysis = profiler.analyze_geographic_concentration(postcodes)
    print(f"\n📍 Geographic Concentration: {geo_analysis['concentration_ratio']:.1%}")
    print(f"⚠️  Suspicious Clustering: {'YES' if geo_analysis['suspicious_clustering'] else 'NO'}")
    
    # 4. Repeat Offender
    print_separator("4. REPEAT OFFENDER")
    scenario = generator.generate_repeat_offender(claim_count=3)
    claims = scenario['claims']
    policyholder = scenario['policyholder']
    
    total_amount = sum(c.claim_amount for c in claims)
    print(f"\n📋 Scenario: {scenario['description']}")
    print(f"👤 Policyholder: {policyholder.first_name} {policyholder.last_name}")
    print(f"📊 Total claims: {len(claims)}")
    print(f"💰 Total claimed: £{total_amount:,.2f}")
    print("\n📅 Claim History:")
    for i, claim in enumerate(claims):
        print(f"  #{i+1}: {claim.incident_date.strftime('%Y-%m-%d')} - {claim.claim_type.value} - £{claim.claim_amount:,.2f}")
    
    # 5. Statistics Summary
    print_separator("DETECTION STATISTICS")
    
    # Analyze a claim with full fraud detection
    test_claim = claims[0]
    test_policy = scenario['policies'][0]
    analysis = detector.analyze_claim(test_claim, test_policy, policyholder)
    
    print("\n📊 Sample Fraud Analysis:")
    print(f"  Overall Score: {analysis['fraud_score']:.2%}")
    print(f"  Risk Level: {analysis['risk_level']}")
    print(f"  Confidence: {analysis['confidence']:.2%}")
    print(f"  Action: {analysis['recommended_action'].replace('_', ' ')}")
    
    print("\n🎯 Signal Breakdown:")
    for signal, score in analysis['signals'].items():
        if isinstance(score, (int, float)):
            print(f"  • {signal}: {score:.2f}")
    
    print("\n" + "=" * 60)
    print("Demo completed successfully! ✅")
    print("=" * 60)


if __name__ == "__main__":
    try:
        run_all_demos()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)