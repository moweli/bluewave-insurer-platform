#!/usr/bin/env python3
"""Local streaming demo without Azure dependencies."""

import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, Any, List
import random
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import StreamingEvent
from src.generators.policy_generator import PolicyGenerator
from src.generators.claim_generator import ClaimGenerator
from src.fraud.fraud_detector import FraudDetector
from src.fraud.fraud_scenarios import FraudScenarioGenerator

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class LocalStreamingDemo:
    """Local streaming demo that writes to files instead of Event Hubs."""
    
    def __init__(self, output_dir: str = "streaming_output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        self.policy_generator = PolicyGenerator()
        self.claim_generator = ClaimGenerator(fraud_rate=0.05)
        self.fraud_detector = FraudDetector()
        self.fraud_scenario_generator = FraudScenarioGenerator()
        
        self.stats = {
            'policies_generated': 0,
            'claims_generated': 0,
            'fraud_cases_generated': 0,
            'events_generated': 0
        }
        
        # Output files
        self.claims_file = self.output_dir / "claims_stream.jsonl"
        self.policies_file = self.output_dir / "policies_stream.jsonl"
        self.audit_file = self.output_dir / "audit_stream.jsonl"
        
    def write_event(self, file_path: Path, event: StreamingEvent):
        """Write event to file."""
        with open(file_path, 'a') as f:
            f.write(json.dumps(event.dict(), default=str) + '\n')
            
    async def generate_policies(self, count: int = 10):
        """Generate and stream policies."""
        print(f"\n📄 Generating {count} policies...")
        
        for i in range(count):
            # Generate policy
            is_corporate = random.random() < 0.3
            policyholder = self.policy_generator.generate_policyholder(is_corporate)
            policy = self.policy_generator.generate_policy(policyholder)
            
            # Create event
            event = StreamingEvent(
                event_type="policy_created",
                partition_key=policyholder.id,
                data={
                    'policy': policy.dict(),
                    'policyholder': policyholder.dict()
                },
                metadata={
                    'risk_profile': policy.risk_profile.value,
                    'is_corporate': is_corporate,
                    'region': policyholder.address.city
                }
            )
            
            # Write to file
            self.write_event(self.policies_file, event)
            self.stats['policies_generated'] += 1
            self.stats['events_generated'] += 1
            
            # Display progress
            if (i + 1) % 5 == 0:
                print(f"  ✓ Generated {i + 1}/{count} policies")
                
            await asyncio.sleep(0.1)  # Simulate streaming delay
            
    async def generate_claims(self, count: int = 20):
        """Generate and stream claims."""
        print(f"\n📋 Generating {count} claims...")
        
        # Ensure we have policies to claim against
        if not self.policy_generator.created_policies:
            await self.generate_policies(5)
            
        for i in range(count):
            # Select random policy
            policy = random.choice(self.policy_generator.created_policies)
            policyholder = next(
                (p for p in self.policy_generator.created_policyholders 
                 if p.id == policy.policyholder_id),
                None
            )
            
            if not policyholder:
                policyholder = self.policy_generator.generate_policyholder()
                policyholder.id = policy.policyholder_id
                
            # Determine if fraudulent
            is_fraud = random.random() < 0.05
            
            # Generate claim
            claim = self.claim_generator.generate_claim(
                policy=policy,
                policyholder=policyholder,
                is_fraud=is_fraud
            )
            
            # Run fraud detection
            fraud_analysis = self.fraud_detector.analyze_claim(
                claim, policy, policyholder,
                self.claim_generator.created_claims[-5:] if len(self.claim_generator.created_claims) > 5 else []
            )
            
            # Update fraud score
            claim.fraud_score = fraud_analysis['fraud_score']
            claim.is_flagged = fraud_analysis['is_flagged']
            
            # Create event
            event = StreamingEvent(
                event_type="claim_submitted",
                partition_key=claim.policyholder_id,
                data={
                    'claim': claim.dict(),
                    'fraud_analysis': fraud_analysis
                },
                metadata={
                    'claim_type': claim.claim_type.value,
                    'fraud_score': claim.fraud_score,
                    'is_flagged': claim.is_flagged,
                    'region': claim.location.city
                }
            )
            
            # Write to file
            self.write_event(self.claims_file, event)
            self.stats['claims_generated'] += 1
            if is_fraud:
                self.stats['fraud_cases_generated'] += 1
            self.stats['events_generated'] += 1
            
            # Display interesting claims
            if claim.fraud_score > 0.7:
                print(f"  🚨 HIGH RISK: Claim #{claim.claim_number[-5:]} - Score: {claim.fraud_score:.2%} - £{claim.claim_amount:,.2f}")
            elif (i + 1) % 5 == 0:
                print(f"  ✓ Generated {i + 1}/{count} claims")
                
            # Create audit event for high-risk claims
            if claim.fraud_score > 0.7:
                audit_event = StreamingEvent(
                    event_type="high_risk_claim",
                    partition_key=claim.id,
                    data={
                        'claim_id': claim.id,
                        'fraud_score': claim.fraud_score,
                        'amount': claim.claim_amount,
                        'action': fraud_analysis['recommended_action']
                    }
                )
                self.write_event(self.audit_file, audit_event)
                
            await asyncio.sleep(0.05)  # Simulate streaming delay
            
    async def generate_fraud_scenario(self):
        """Generate a fraud scenario."""
        print("\n🎭 Generating fraud scenario...")
        
        scenario_type = random.choice(['early_claim', 'inflated', 'fraud_ring'])
        
        if scenario_type == 'early_claim':
            scenario = self.fraud_scenario_generator.generate_early_claim_fraud()
            print(f"  📍 Early claim fraud: {scenario['claim'].claim_amount:,.2f}")
            
        elif scenario_type == 'inflated':
            scenario = self.fraud_scenario_generator.generate_inflated_claim_fraud()
            print(f"  📍 Inflated claim: {scenario['claim'].claim_amount:,.2f}")
            
        elif scenario_type == 'fraud_ring':
            scenario = self.fraud_scenario_generator.generate_fraud_ring(size=3)
            total = sum(c.claim_amount for c in scenario['claims'])
            print(f"  📍 Fraud ring detected: {len(scenario['claims'])} claims, Total: £{total:,.2f}")
            
            # Write each claim in the ring
            for claim in scenario['claims']:
                event = StreamingEvent(
                    event_type="fraud_ring_member",
                    partition_key=claim.policyholder_id,
                    data={'claim': claim.dict()},
                    metadata={'scenario': 'fraud_ring'}
                )
                self.write_event(self.claims_file, event)
                
        self.stats['fraud_cases_generated'] += 1
        
    def print_summary(self):
        """Print streaming summary."""
        print("\n" + "=" * 60)
        print("📊 STREAMING SUMMARY")
        print("=" * 60)
        print(f"Total Events Generated: {self.stats['events_generated']}")
        print(f"  • Policies: {self.stats['policies_generated']}")
        print(f"  • Claims: {self.stats['claims_generated']}")
        print(f"  • Fraud Cases: {self.stats['fraud_cases_generated']}")
        print(f"\nOutput Files:")
        print(f"  • Claims: {self.claims_file}")
        print(f"  • Policies: {self.policies_file}")
        print(f"  • Audit: {self.audit_file}")
        print("=" * 60)
        
    async def run(self, duration_seconds: int = 30):
        """Run the streaming demo."""
        print("\n" + "=" * 60)
        print("🚀 STARTING LOCAL STREAMING DEMO")
        print(f"Duration: {duration_seconds} seconds")
        print("=" * 60)
        
        start_time = datetime.now()
        
        # Run different generators concurrently
        tasks = []
        
        # Generate initial policies
        tasks.append(self.generate_policies(10))
        
        # Wait for policies to be created
        await asyncio.gather(*tasks)
        
        # Now generate claims and fraud scenarios
        tasks = [
            self.generate_claims(30),
            self.generate_fraud_scenario()
        ]
        
        # Run for specified duration
        try:
            await asyncio.wait_for(
                asyncio.gather(*tasks),
                timeout=duration_seconds
            )
        except asyncio.TimeoutError:
            print(f"\n⏱️ Streaming duration ({duration_seconds}s) reached")
        except KeyboardInterrupt:
            print("\n⚠️ Streaming interrupted by user")
            
        elapsed = (datetime.now() - start_time).total_seconds()
        events_per_second = self.stats['events_generated'] / elapsed if elapsed > 0 else 0
        
        print(f"\n📈 Performance: {events_per_second:.2f} events/second")
        self.print_summary()


async def main():
    """Main entry point."""
    demo = LocalStreamingDemo()
    
    print("\nSelect streaming duration:")
    print("1. Quick demo (10 seconds)")
    print("2. Standard demo (30 seconds)")
    print("3. Extended demo (60 seconds)")
    
    choice = input("\nEnter choice (1-3): ").strip()
    
    duration = {
        '1': 10,
        '2': 30,
        '3': 60
    }.get(choice, 30)
    
    await demo.run(duration)
    
    print("\n✅ Demo completed successfully!")
    print("Check the 'streaming_output' directory for generated events.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nDemo interrupted. Goodbye!")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()