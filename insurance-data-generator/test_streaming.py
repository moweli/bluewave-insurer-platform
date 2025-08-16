#!/usr/bin/env python3
"""Test streaming to Azure Event Hubs for a short duration."""

import asyncio
import signal
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.streaming_service import InsuranceStreamingService


async def test_streaming(duration: int = 10):
    """Test streaming for a limited duration."""
    service = InsuranceStreamingService()
    
    try:
        await service.initialize()
        
        # Set running flag
        service.is_running = True
        from datetime import datetime
        service.stats['start_time'] = datetime.utcnow()
        
        print(f"🚀 Starting streaming to Azure Event Hubs for {duration} seconds...")
        print(f"   Claims Hub: {service.settings.eventhub_name_claims}")
        print(f"   Policies Hub: {service.settings.eventhub_name_policies}")
        print(f"   Audit Hub: {service.settings.eventhub_name_audit}")
        print("-" * 60)
        
        # Create tasks
        tasks = [
            asyncio.create_task(service.generate_policy_stream()),
            asyncio.create_task(service.generate_claim_stream()),
        ]
        
        # Run for specified duration
        await asyncio.sleep(duration)
        
        # Stop service
        service.is_running = False
        
        # Cancel tasks
        for task in tasks:
            task.cancel()
            
        # Wait a bit for cleanup
        await asyncio.sleep(1)
        
        # Print stats
        service.print_stats()
        
        await service.shutdown()
        
        print("\n✅ Streaming test completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error during streaming: {e}")
        import traceback
        traceback.print_exc()
        await service.shutdown()


if __name__ == "__main__":
    # Run for 10 seconds
    asyncio.run(test_streaming(10))