#!/usr/bin/env python3
"""Test Azure Event Hub connection."""

import asyncio
import sys
from pathlib import Path
from dotenv import load_dotenv
import os

sys.path.insert(0, str(Path(__file__).parent))

from src.streaming.producer import EventHubProducer
from src.models import StreamingEvent


async def test_connection():
    """Test connection to Azure Event Hubs."""
    # Load environment variables
    load_dotenv()
    
    connection_string = os.getenv('EVENTHUB_CONNECTION_STRING')
    claims_hub = os.getenv('EVENTHUB_NAME_CLAIMS')
    
    if not connection_string:
        print("❌ No Event Hub connection string found in .env")
        return False
        
    print("🔌 Testing Azure Event Hub connection...")
    print(f"   Namespace: bw-dev-uks-eventhubs-001")
    print(f"   Event Hub: {claims_hub}")
    
    try:
        # Create producer
        producer = EventHubProducer(
            connection_string=connection_string,
            eventhub_name=claims_hub,
            batch_size=1
        )
        
        # Start producer
        await producer.start()
        print("✅ Successfully connected to Event Hub")
        
        # Send a test event
        test_event = StreamingEvent(
            event_type="connection_test",
            partition_key="test",
            data={"message": "Connection test from BlueWave Insurance Data Generator"},
            metadata={"test": True}
        )
        
        await producer.send_event(test_event)
        await producer._flush_batch()
        
        print(f"✅ Successfully sent test event to {claims_hub}")
        
        # Get stats
        stats = producer.get_stats()
        print(f"📊 Stats: {stats}")
        
        # Close producer
        await producer.stop()
        
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("AZURE EVENT HUB CONNECTION TEST")
    print("=" * 60 + "\n")
    
    success = asyncio.run(test_connection())
    
    if success:
        print("\n✅ Azure Event Hub connection successful!")
        print("You can now run: python src/streaming_service.py")
    else:
        print("\n❌ Connection test failed. Please check your credentials.")
    
    print("\n" + "=" * 60)