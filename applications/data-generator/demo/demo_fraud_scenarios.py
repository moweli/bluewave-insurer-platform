#!/usr/bin/env python3
"""Demo script for fraud scenario generation and detection."""

import asyncio
import json
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.fraud.fraud_scenarios import FraudScenarioGenerator
from src.fraud.fraud_detector import FraudDetector
from src.utils.uk_postcode_profiler import UKPostcodeProfiler


def print_separator(title: str = ""):
    """Print a formatted separator."""
    if title:
        print(f"\n{'=' * 20} {title} {'=' * 20}")
    else:
        print("=" * 60)


def print_claim_details(claim: Any, policy: Any, policyholder: Any):
    """Print formatted claim details."""
    print(f"""
Claim Details:
  Claim Number: {claim.claim_number}
  Policy Number: {policy.policy_number}
  Policyholder: {policyholder.first_name} {policyholder.last_name}
  Location: {claim.location.city}, {claim.location.postcode}
  
  Incident Date: {claim.incident_date.strftime('%Y-%m-%d')}
  Reported Date: {claim.reported_date.strftime('%Y-%m-%d')}
  Reporting Delay: {claim.reporting_delay_days} days
  
  Claim Type: {claim.claim_type.value}
  Claim Amount: £{claim.claim_amount:,.2f}
  Policy Sum Insured: £{policy.sum_insured:,.2f}
  Claim/Sum Ratio: {(claim.claim_amount/policy.sum_insured)*100:.1f}%
  
  Description: {claim.description}
""")


def print_fraud_analysis(analysis: Dict[str, Any]):
    """Print formatted fraud analysis results."""
    print(f"""
Fraud Analysis Results:
  Overall Fraud Score: {analysis['fraud_score']:.2%}
  Risk Level: {analysis['risk_level']}
  Flagged for Review: {'YES' if analysis['is_flagged'] else 'NO'}
  Confidence: {analysis['confidence']:.2%}
  Recommended Action: {analysis['recommended_action'].replace('_', ' ')}
  
  Fraud Indicators Detected:
""")
    
    if analysis['fraud_indicators']:
        for indicator in analysis['fraud_indicators']:
            print(f"    • {indicator.value.replace('_', ' ').title()}")
    else:
        print("    • None")
    
    print("\n  Signal Scores:")
    for signal, score in analysis['signals'].items():
        if isinstance(score, (int, float)):
            print(f"    • {signal.replace('_', ' ').title()}: {score:.2f}")
        else:
            print(f"    • {signal.replace('_', ' ').title()}: {score}")


def demo_early_claim_fraud():
    """Demonstrate early claim fraud detection."""
    print_separator("EARLY CLAIM FRAUD SCENARIO")
    
    generator = FraudScenarioGenerator()
    detector = FraudDetector()
    
    # Generate scenario
    scenario = generator.generate_early_claim_fraud()
    
    print("\nScenario Description:")
    print(f"  {scenario['description']}")
    
    print("\nKey Characteristics:")
    for indicator in scenario['key_indicators']:
        print(f"  • {indicator}")
    
    # Extract data
    claim = scenario['claim']
    policy = scenario['policy']
    policyholder = scenario['policyholder']
    
    # Print details
    print_claim_details(claim, policy, policyholder)
    
    # Run fraud detection
    analysis = detector.analyze_claim(claim, policy, policyholder)
    print_fraud_analysis(analysis)
    
    # Highlight suspicious elements
    days_since_policy_start = (claim.incident_date - policy.start_date).days
    print(f"\n⚠️  ALERT: Claim filed only {days_since_policy_start} days after policy inception!")


def demo_fraud_ring():
    """Demonstrate fraud ring detection."""
    print_separator("FRAUD RING SCENARIO")
    
    generator = FraudScenarioGenerator()
    detector = FraudDetector()
    profiler = UKPostcodeProfiler()
    
    # Generate fraud ring
    scenario = generator.generate_fraud_ring(size=4)
    
    print("\nScenario Description:")
    print(f"  {scenario['description']}")
    
    print("\nKey Characteristics:")
    for indicator in scenario['key_indicators']:
        print(f"  • {indicator}")
    
    print(f"\nFraud Ring Analysis - {len(scenario['claims'])} Connected Claims:")
    
    # Analyze each claim in the ring
    for i, claim in enumerate(scenario['claims']):
        policy = scenario['policies'][i]
        policyholder = scenario['policyholders'][i]
        
        print(f"\n  Member #{i+1}:")
        print(f"    Policyholder: {policyholder.first_name} {policyholder.last_name}")
        print(f"    Postcode: {policyholder.address.postcode}")
        print(f"    Claim Amount: £{claim.claim_amount:,.2f}")
        print(f"    Incident Date: {claim.incident_date.strftime('%Y-%m-%d')}")
        
        # Add to network
        other_ids = [p.id for j, p in enumerate(scenario['policyholders']) if j != i]
        detector.add_to_network(policyholder.id, other_ids)
    
    # Analyze geographic concentration
    postcodes = [p.address.postcode for p in scenario['policyholders']]
    geo_analysis = profiler.analyze_geographic_concentration(postcodes)
    
    print(f"\n  Geographic Analysis:")
    print(f"    Concentration Ratio: {geo_analysis['concentration_ratio']:.2%}")
    print(f"    Most Common Area: {geo_analysis['most_common_area']}")
    print(f"    Suspicious Clustering: {'YES' if geo_analysis['suspicious_clustering'] else 'NO'}")
    
    # Analyze first claim with network context
    analysis = detector.analyze_claim(
        scenario['claims'][0],
        scenario['policies'][0],
        scenario['policyholders'][0]
    )
    
    print_fraud_analysis(analysis)
    
    print("\n⚠️  ALERT: Network fraud ring detected with geographic clustering!")


def demo_inflated_claim():
    """Demonstrate inflated claim fraud detection."""
    print_separator("INFLATED CLAIM FRAUD SCENARIO")
    
    generator = FraudScenarioGenerator()
    detector = FraudDetector()
    
    # Generate scenario
    scenario = generator.generate_inflated_claim_fraud()
    
    print("\nScenario Description:")
    print(f"  {scenario['description']}")
    
    claim = scenario['claim']
    policy = scenario['policy']
    policyholder = scenario['policyholder']
    
    print_claim_details(claim, policy, policyholder)
    
    # Run fraud detection
    analysis = detector.analyze_claim(claim, policy, policyholder)
    print_fraud_analysis(analysis)
    
    # Show suspicious patterns
    print("\n⚠️  Suspicious Patterns Detected:")
    print(f"  • Claim amount is {(claim.claim_amount/policy.sum_insured)*100:.1f}% of sum insured")
    print(f"  • Significantly above typical claim range for {claim.claim_type.value}")
    if claim.claim_amount % 1000 == 0:
        print(f"  • Round number claim amount (£{claim.claim_amount:,.0f})")


def demo_repeat_offender():
    """Demonstrate repeat offender detection."""
    print_separator("REPEAT OFFENDER SCENARIO")
    
    generator = FraudScenarioGenerator()
    detector = FraudDetector()
    
    # Generate scenario
    scenario = generator.generate_repeat_offender(claim_count=4)
    
    print("\nScenario Description:")
    print(f"  {scenario['description']}")
    
    policyholder = scenario['policyholder']
    print(f"\nPolicyholder Profile:")
    print(f"  Name: {policyholder.first_name} {policyholder.last_name}")
    print(f"  Risk Score: {policyholder.risk_score:.2%}")
    print(f"  Location: {policyholder.address.city}, {policyholder.address.postcode}")
    
    print(f"\nClaim History - {len(scenario['claims'])} Claims:")
    
    total_claimed = 0
    for i, claim in enumerate(scenario['claims']):
        policy = scenario['policies'][i]
        
        print(f"\n  Claim #{i+1}:")
        print(f"    Date: {claim.incident_date.strftime('%Y-%m-%d')}")
        print(f"    Type: {claim.claim_type.value}")
        print(f"    Policy Type: {policy.policy_type.value}")
        print(f"    Amount: £{claim.claim_amount:,.2f}")
        print(f"    Status: {claim.status.value}")
        
        total_claimed += claim.claim_amount
        
        # Analyze with history
        historical = scenario['claims'][:i] if i > 0 else []
        analysis = detector.analyze_claim(claim, policy, policyholder, historical)
    
    print(f"\n  Total Claims Value: £{total_claimed:,.2f}")
    print(f"  Average Claim: £{total_claimed/len(scenario['claims']):,.2f}")
    
    # Show final analysis
    print_fraud_analysis(analysis)
    
    print("\n⚠️  ALERT: Pattern of excessive claiming detected!")


def demo_opportunistic_fraud():
    """Demonstrate opportunistic fraud detection."""
    print_separator("OPPORTUNISTIC FRAUD SCENARIO")
    
    generator = FraudScenarioGenerator()
    detector = FraudDetector()
    
    # Generate scenario
    scenario = generator.generate_opportunistic_fraud()
    
    print("\nScenario Description:")
    print(f"  {scenario['description']}")
    
    claim = scenario['claim']
    policy = scenario['policy']
    policyholder = scenario['policyholder']
    
    print_claim_details(claim, policy, policyholder)
    
    # Show the inflation
    print(f"\n💰 Claim Inflation Analysis:")
    print(f"  Actual Damage Estimate: £{scenario['actual_damage']:,.2f}")
    print(f"  Claimed Amount: £{scenario['claimed_damage']:,.2f}")
    print(f"  Inflation Factor: {scenario['claimed_damage']/scenario['actual_damage']:.1f}x")
    print(f"  Excess Claimed: £{scenario['claimed_damage'] - scenario['actual_damage']:,.2f}")
    
    # Run fraud detection
    analysis = detector.analyze_claim(claim, policy, policyholder)
    print_fraud_analysis(analysis)
    
    print("\n⚠️  Pattern suggests opportunistic inflation of legitimate claim!")


async def demo_streaming_simulation():
    """Simulate real-time fraud detection."""
    print_separator("REAL-TIME FRAUD DETECTION SIMULATION")
    
    print("\n🚀 Starting real-time simulation...")
    print("   Generating mixed claim stream with fraud patterns...\n")
    
    generator = FraudScenarioGenerator()
    detector = FraudDetector()
    
    # Statistics
    stats = {
        'total_claims': 0,
        'fraud_detected': 0,
        'high_risk': 0,
        'total_value': 0
    }
    
    # Generate mix of legitimate and fraudulent claims
    for i in range(10):
        await asyncio.sleep(0.5)  # Simulate streaming delay
        
        # 30% chance of fraud scenario
        if i % 3 == 0:
            # Generate random fraud type
            fraud_types = ['early', 'inflated', 'opportunistic']
            fraud_type = fraud_types[i % len(fraud_types)]
            
            if fraud_type == 'early':
                scenario = generator.generate_early_claim_fraud()
            elif fraud_type == 'inflated':
                scenario = generator.generate_inflated_claim_fraud()
            else:
                scenario = generator.generate_opportunistic_fraud()
            
            claim = scenario['claim']
            policy = scenario['policy']
            policyholder = scenario['policyholder']
            
            print(f"  [{datetime.now().strftime('%H:%M:%S')}] ⚠️  FRAUD PATTERN - ", end="")
        else:
            # Generate legitimate claim
            from src.generators.policy_generator import PolicyGenerator
            from src.generators.claim_generator import ClaimGenerator
            
            pg = PolicyGenerator()
            cg = ClaimGenerator(fraud_rate=0)
            
            policyholder = pg.generate_policyholder()
            policy = pg.generate_policy(policyholder)
            claim = cg.generate_claim(policy, policyholder, is_fraud=False)
            
            print(f"  [{datetime.now().strftime('%H:%M:%S')}] ✓  Normal Claim - ", end="")
        
        # Analyze claim
        analysis = detector.analyze_claim(claim, policy, policyholder)
        
        # Update stats
        stats['total_claims'] += 1
        stats['total_value'] += claim.claim_amount
        
        if analysis['fraud_score'] > 0.7:
            stats['fraud_detected'] += 1
            
        if analysis['risk_level'] in ['HIGH', 'CRITICAL']:
            stats['high_risk'] += 1
        
        # Print summary
        print(f"Claim #{claim.claim_number[-5:]} | "
              f"£{claim.claim_amount:,.0f} | "
              f"Score: {analysis['fraud_score']:.2%} | "
              f"Risk: {analysis['risk_level']}")
    
    # Final statistics
    print(f"\n📊 Simulation Statistics:")
    print(f"  Total Claims Processed: {stats['total_claims']}")
    print(f"  Fraud Cases Detected: {stats['fraud_detected']}")
    print(f"  High Risk Claims: {stats['high_risk']}")
    print(f"  Total Claim Value: £{stats['total_value']:,.2f}")
    print(f"  Detection Rate: {(stats['fraud_detected']/stats['total_claims'])*100:.1f}%")


def main():
    """Run all demo scenarios."""
    print("\n" + "=" * 60)
    print(" " * 10 + "BLUEWAVE INSURANCE FRAUD DETECTION DEMO")
    print("=" * 60)
    
    demos = [
        ("Early Claim Fraud", demo_early_claim_fraud),
        ("Inflated Claim Fraud", demo_inflated_claim),
        ("Fraud Ring Detection", demo_fraud_ring),
        ("Repeat Offender Pattern", demo_repeat_offender),
        ("Opportunistic Fraud", demo_opportunistic_fraud)
    ]
    
    print("\nAvailable Demonstrations:")
    for i, (name, _) in enumerate(demos, 1):
        print(f"  {i}. {name}")
    print(f"  {len(demos)+1}. Real-time Streaming Simulation")
    print(f"  {len(demos)+2}. Run All Demos")
    print("  0. Exit")
    
    while True:
        try:
            choice = input("\nSelect demo (0-7): ").strip()
            
            if choice == '0':
                print("\nExiting demo. Thank you!")
                break
            elif choice == '1':
                demo_early_claim_fraud()
            elif choice == '2':
                demo_inflated_claim()
            elif choice == '3':
                demo_fraud_ring()
            elif choice == '4':
                demo_repeat_offender()
            elif choice == '5':
                demo_opportunistic_fraud()
            elif choice == '6':
                asyncio.run(demo_streaming_simulation())
            elif choice == '7':
                # Run all demos
                for name, demo_func in demos:
                    demo_func()
                    input("\nPress Enter to continue to next demo...")
                asyncio.run(demo_streaming_simulation())
            else:
                print("Invalid choice. Please select 0-7.")
                continue
            
            if choice != '7':
                input("\nPress Enter to return to menu...")
                
        except KeyboardInterrupt:
            print("\n\nDemo interrupted. Goodbye!")
            break
        except Exception as e:
            print(f"\nError running demo: {e}")
            continue


if __name__ == "__main__":
    main()