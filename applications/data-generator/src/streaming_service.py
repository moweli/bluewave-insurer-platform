"""Real-time streaming service for insurance data generation."""

import asyncio
import logging
import signal
import sys
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
import random
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import StreamingEvent, PolicyType
from src.streaming.producer import EventHubProducer, MultiEventHubProducer
from src.generators.policy_generator import PolicyGenerator
from src.generators.claim_generator import ClaimGenerator
from src.fraud.fraud_detector import FraudDetector
from src.fraud.fraud_scenarios import FraudScenarioGenerator
from config.settings import get_settings


# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class InsuranceStreamingService:
    """Service for streaming synthetic insurance data to Event Hubs."""
    
    def __init__(self):
        """Initialize the streaming service."""
        self.settings = get_settings()
        self.policy_generator = PolicyGenerator()
        self.claim_generator = ClaimGenerator(fraud_rate=self.settings.fraud_injection_rate)
        self.fraud_detector = FraudDetector()
        self.fraud_scenario_generator = FraudScenarioGenerator()
        
        self.producer: Optional[MultiEventHubProducer] = None
        self.is_running = False
        self.stats = {
            'policies_generated': 0,
            'claims_generated': 0,
            'fraud_cases_generated': 0,
            'events_sent': 0,
            'errors': 0,
            'start_time': None
        }
        
    async def initialize(self):
        """Initialize Event Hub connections."""
        try:
            self.producer = MultiEventHubProducer(
                self.settings.eventhub_connection_string,
                self.settings.batch_size
            )
            
            # Add Event Hubs
            await self.producer.add_hub(self.settings.eventhub_name_claims)
            await self.producer.add_hub(self.settings.eventhub_name_policies)
            await self.producer.add_hub(self.settings.eventhub_name_audit)
            
            logger.info("Streaming service initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize streaming service: {e}")
            raise
            
    async def shutdown(self):
        """Shutdown the streaming service."""
        self.is_running = False
        
        if self.producer:
            await self.producer.close_all()
            
        logger.info("Streaming service shutdown complete")
        self.print_stats()
        
    def print_stats(self):
        """Print service statistics."""
        if self.stats['start_time']:
            runtime = (datetime.utcnow() - self.stats['start_time']).total_seconds()
            events_per_second = self.stats['events_sent'] / runtime if runtime > 0 else 0
            
            print("\n" + "=" * 50)
            print("STREAMING SERVICE STATISTICS")
            print("=" * 50)
            print(f"Runtime: {runtime:.2f} seconds")
            print(f"Policies Generated: {self.stats['policies_generated']}")
            print(f"Claims Generated: {self.stats['claims_generated']}")
            print(f"Fraud Cases: {self.stats['fraud_cases_generated']}")
            print(f"Total Events Sent: {self.stats['events_sent']}")
            print(f"Events per Second: {events_per_second:.2f}")
            print(f"Errors: {self.stats['errors']}")
            print("=" * 50)
            
    async def generate_policy_stream(self):
        """Generate and stream policies."""
        while self.is_running:
            try:
                # Generate a batch of policies
                batch_size = random.randint(5, 15)
                
                for _ in range(batch_size):
                    # Generate policy with random characteristics
                    is_corporate = random.random() < 0.3
                    policyholder = self.policy_generator.generate_policyholder(is_corporate)
                    policy = self.policy_generator.generate_policy(policyholder)
                    
                    # Create streaming event
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
                    
                    # Send to policies stream
                    await self.producer.send_to_hub(
                        self.settings.eventhub_name_policies,
                        event
                    )
                    
                    self.stats['policies_generated'] += 1
                    self.stats['events_sent'] += 1
                    
                    # Create audit event
                    audit_event = StreamingEvent(
                        event_type="audit",
                        partition_key=policy.id,
                        data={
                            'event_type': 'policy_created',
                            'entity_type': 'policy',
                            'entity_id': policy.id,
                            'action': 'CREATE',
                            'details': {
                                'policy_type': policy.policy_type.value,
                                'premium': policy.premium_amount,
                                'policyholder_id': policyholder.id
                            }
                        }
                    )
                    
                    await self.producer.send_to_hub(
                        self.settings.eventhub_name_audit,
                        audit_event
                    )
                    
                    self.stats['events_sent'] += 1
                    
                # Rate limiting
                await asyncio.sleep(random.uniform(2, 5))
                
            except Exception as e:
                logger.error(f"Error in policy stream: {e}")
                self.stats['errors'] += 1
                await asyncio.sleep(5)
                
    async def generate_claim_stream(self):
        """Generate and stream claims."""
        while self.is_running:
            try:
                # Generate claims for existing policies
                if self.policy_generator.created_policies:
                    batch_size = random.randint(3, 10)
                    
                    for _ in range(batch_size):
                        # Select a random policy
                        policy = random.choice(self.policy_generator.created_policies)
                        
                        # Find or generate policyholder
                        policyholder = next(
                            (p for p in self.policy_generator.created_policyholders 
                             if p.id == policy.policyholder_id),
                            None
                        )
                        
                        if not policyholder:
                            policyholder = self.policy_generator.generate_policyholder()
                            policyholder.id = policy.policyholder_id
                            
                        # Determine if this should be fraudulent
                        is_fraud = random.random() < self.settings.fraud_injection_rate
                        
                        # Generate claim
                        claim = self.claim_generator.generate_claim(
                            policy=policy,
                            policyholder=policyholder,
                            is_fraud=is_fraud
                        )
                        
                        # Run fraud detection
                        fraud_analysis = self.fraud_detector.analyze_claim(
                            claim, policy, policyholder,
                            self.claim_generator.created_claims
                        )
                        
                        # Update claim with fraud analysis
                        claim.fraud_score = fraud_analysis['fraud_score']
                        claim.is_flagged = fraud_analysis['is_flagged']
                        
                        # Create streaming event
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
                                'policy_type': policy.policy_type.value,
                                'region': claim.location.city
                            }
                        )
                        
                        # Send to claims stream
                        await self.producer.send_to_hub(
                            self.settings.eventhub_name_claims,
                            event
                        )
                        
                        self.stats['claims_generated'] += 1
                        if is_fraud:
                            self.stats['fraud_cases_generated'] += 1
                        self.stats['events_sent'] += 1
                        
                        # Create audit event for high-risk claims
                        if claim.fraud_score > 0.7:
                            audit_event = StreamingEvent(
                                event_type="audit",
                                partition_key=claim.id,
                                data={
                                    'event_type': 'high_risk_claim_detected',
                                    'entity_type': 'claim',
                                    'entity_id': claim.id,
                                    'action': 'FLAG',
                                    'details': {
                                        'fraud_score': claim.fraud_score,
                                        'fraud_indicators': [i.value for i in claim.fraud_indicators],
                                        'claim_amount': claim.claim_amount,
                                        'recommended_action': fraud_analysis['recommended_action']
                                    }
                                }
                            )
                            
                            await self.producer.send_to_hub(
                                self.settings.eventhub_name_audit,
                                audit_event
                            )
                            
                            self.stats['events_sent'] += 1
                            
                # Rate limiting
                await asyncio.sleep(random.uniform(1, 3))
                
            except Exception as e:
                logger.error(f"Error in claim stream: {e}")
                self.stats['errors'] += 1
                await asyncio.sleep(5)
                
    async def generate_fraud_scenarios(self):
        """Periodically generate fraud scenario bursts."""
        while self.is_running:
            try:
                # Wait before generating fraud scenarios
                await asyncio.sleep(random.uniform(30, 60))
                
                # Select a random fraud scenario
                scenario_type = random.choice([
                    'early_claim',
                    'inflated',
                    'fraud_ring',
                    'repeat_offender',
                    'phantom',
                    'opportunistic'
                ])
                
                logger.info(f"Generating fraud scenario: {scenario_type}")
                
                if scenario_type == 'early_claim':
                    scenario = self.fraud_scenario_generator.generate_early_claim_fraud()
                    await self._stream_single_claim_scenario(scenario)
                    
                elif scenario_type == 'inflated':
                    scenario = self.fraud_scenario_generator.generate_inflated_claim_fraud()
                    await self._stream_single_claim_scenario(scenario)
                    
                elif scenario_type == 'fraud_ring':
                    scenario = self.fraud_scenario_generator.generate_fraud_ring(size=4)
                    await self._stream_fraud_ring_scenario(scenario)
                    
                elif scenario_type == 'repeat_offender':
                    scenario = self.fraud_scenario_generator.generate_repeat_offender(claim_count=3)
                    await self._stream_repeat_offender_scenario(scenario)
                    
                elif scenario_type == 'phantom':
                    scenario = self.fraud_scenario_generator.generate_phantom_claim()
                    await self._stream_single_claim_scenario(scenario)
                    
                elif scenario_type == 'opportunistic':
                    scenario = self.fraud_scenario_generator.generate_opportunistic_fraud()
                    await self._stream_single_claim_scenario(scenario)
                    
            except Exception as e:
                logger.error(f"Error generating fraud scenario: {e}")
                self.stats['errors'] += 1
                
    async def _stream_single_claim_scenario(self, scenario: Dict[str, Any]):
        """Stream a single claim fraud scenario."""
        claim = scenario['claim']
        policy = scenario['policy']
        policyholder = scenario['policyholder']
        
        # Run fraud detection
        fraud_analysis = self.fraud_detector.analyze_claim(
            claim, policy, policyholder
        )
        
        # Create event
        event = StreamingEvent(
            event_type="fraud_scenario",
            partition_key=claim.policyholder_id,
            data={
                'scenario_type': scenario['scenario_type'],
                'claim': claim.dict(),
                'policy': policy.dict(),
                'policyholder': policyholder.dict(),
                'fraud_analysis': fraud_analysis,
                'key_indicators': scenario['key_indicators']
            },
            metadata={
                'scenario': scenario['scenario_type'],
                'fraud_score': claim.fraud_score,
                'description': scenario['description']
            }
        )
        
        await self.producer.send_to_hub(
            self.settings.eventhub_name_claims,
            event
        )
        
        self.stats['fraud_cases_generated'] += 1
        self.stats['events_sent'] += 1
        
    async def _stream_fraud_ring_scenario(self, scenario: Dict[str, Any]):
        """Stream a fraud ring scenario."""
        claims = scenario['claims']
        policies = scenario['policies']
        policyholders = scenario['policyholders']
        
        # Connect policyholders in network
        policyholder_ids = [p.id for p in policyholders]
        for ph_id in policyholder_ids:
            self.fraud_detector.add_to_network(
                ph_id,
                [pid for pid in policyholder_ids if pid != ph_id]
            )
            
        # Stream each claim in the ring
        for i, claim in enumerate(claims):
            policy = policies[i]
            policyholder = policyholders[i]
            
            fraud_analysis = self.fraud_detector.analyze_claim(
                claim, policy, policyholder
            )
            
            event = StreamingEvent(
                event_type="fraud_ring_member",
                partition_key=claim.policyholder_id,
                data={
                    'scenario_type': 'fraud_ring',
                    'ring_size': len(claims),
                    'member_index': i,
                    'claim': claim.dict(),
                    'policy': policy.dict(),
                    'policyholder': policyholder.dict(),
                    'fraud_analysis': fraud_analysis,
                    'connected_policyholders': policyholder_ids
                },
                metadata={
                    'scenario': 'fraud_ring',
                    'fraud_score': claim.fraud_score,
                    'ring_size': len(claims)
                }
            )
            
            await self.producer.send_to_hub(
                self.settings.eventhub_name_claims,
                event
            )
            
            self.stats['fraud_cases_generated'] += 1
            self.stats['events_sent'] += 1
            
            # Small delay between ring members
            await asyncio.sleep(0.5)
            
    async def _stream_repeat_offender_scenario(self, scenario: Dict[str, Any]):
        """Stream a repeat offender scenario."""
        claims = scenario['claims']
        policies = scenario['policies']
        policyholder = scenario['policyholder']
        
        for i, claim in enumerate(claims):
            policy = policies[i]
            
            fraud_analysis = self.fraud_detector.analyze_claim(
                claim, policy, policyholder,
                claims[:i]  # Include previous claims in analysis
            )
            
            event = StreamingEvent(
                event_type="repeat_offender_claim",
                partition_key=policyholder.id,
                data={
                    'scenario_type': 'repeat_offender',
                    'claim_number': i + 1,
                    'total_claims': len(claims),
                    'claim': claim.dict(),
                    'policy': policy.dict(),
                    'policyholder': policyholder.dict(),
                    'fraud_analysis': fraud_analysis,
                    'previous_claims': i
                },
                metadata={
                    'scenario': 'repeat_offender',
                    'fraud_score': claim.fraud_score,
                    'claim_count': len(claims)
                }
            )
            
            await self.producer.send_to_hub(
                self.settings.eventhub_name_claims,
                event
            )
            
            self.stats['fraud_cases_generated'] += 1
            self.stats['events_sent'] += 1
            
            # Delay between claims
            await asyncio.sleep(1)
            
    async def run(self):
        """Run the streaming service."""
        try:
            await self.initialize()
            
            self.is_running = True
            self.stats['start_time'] = datetime.utcnow()
            
            logger.info("Starting insurance data streaming...")
            logger.info(f"Fraud injection rate: {self.settings.fraud_injection_rate * 100}%")
            logger.info(f"Max events per second: {self.settings.max_events_per_second}")
            
            # Create async tasks for different streams
            tasks = [
                asyncio.create_task(self.generate_policy_stream()),
                asyncio.create_task(self.generate_claim_stream()),
                asyncio.create_task(self.generate_fraud_scenarios())
            ]
            
            # Wait for all tasks
            await asyncio.gather(*tasks)
            
        except KeyboardInterrupt:
            logger.info("Streaming service interrupted by user")
        except Exception as e:
            logger.error(f"Streaming service error: {e}")
            raise
        finally:
            await self.shutdown()


async def main():
    """Main entry point."""
    service = InsuranceStreamingService()
    
    # Setup signal handlers
    def signal_handler(sig, frame):
        logger.info("Received shutdown signal")
        service.is_running = False
        
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    await service.run()


if __name__ == "__main__":
    asyncio.run(main())