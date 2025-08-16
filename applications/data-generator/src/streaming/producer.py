"""Event Hub producer for streaming insurance data."""

import asyncio
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub.exceptions import EventHubError
from tenacity import retry, stop_after_attempt, wait_exponential
import sys
sys.path.append('..')
from ..models import StreamingEvent


logger = logging.getLogger(__name__)


class EventHubProducer:
    """Async Event Hub producer with batching and retry logic."""
    
    def __init__(
        self,
        connection_string: str,
        eventhub_name: str,
        batch_size: int = 100,
        flush_interval: float = 5.0
    ):
        """Initialize the Event Hub producer.
        
        Args:
            connection_string: Event Hub connection string
            eventhub_name: Name of the Event Hub
            batch_size: Maximum batch size before auto-flush
            flush_interval: Maximum seconds before auto-flush
        """
        self.connection_string = connection_string
        self.eventhub_name = eventhub_name
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        
        self.producer: Optional[EventHubProducerClient] = None
        self.event_batch = []
        self.last_flush_time = datetime.utcnow()
        self.total_events_sent = 0
        self.total_batches_sent = 0
        self._running = False
        self._flush_task = None
        
    async def __aenter__(self):
        """Async context manager entry."""
        await self.start()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.stop()
        
    async def start(self):
        """Start the producer and background flush task."""
        try:
            self.producer = EventHubProducerClient.from_connection_string(
                conn_str=self.connection_string,
                eventhub_name=self.eventhub_name
            )
            self._running = True
            self._flush_task = asyncio.create_task(self._periodic_flush())
            logger.info(f"Started Event Hub producer for {self.eventhub_name}")
        except Exception as e:
            logger.error(f"Failed to start producer: {e}")
            raise
            
    async def stop(self):
        """Stop the producer and flush remaining events."""
        self._running = False
        
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
                
        # Flush remaining events
        if self.event_batch:
            await self._flush_batch()
            
        if self.producer:
            await self.producer.close()
            logger.info(f"Stopped Event Hub producer. Total events sent: {self.total_events_sent}")
            
    async def send_event(self, event: StreamingEvent):
        """Send a single event (will be batched).
        
        Args:
            event: StreamingEvent to send
        """
        self.event_batch.append(event)
        
        if len(self.event_batch) >= self.batch_size:
            await self._flush_batch()
            
    async def send_events(self, events: List[StreamingEvent]):
        """Send multiple events.
        
        Args:
            events: List of StreamingEvents to send
        """
        for event in events:
            await self.send_event(event)
            
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    async def _flush_batch(self):
        """Flush the current batch to Event Hub."""
        if not self.event_batch or not self.producer:
            return
            
        try:
            async with self.producer:
                event_data_batch = await self.producer.create_batch()
                
                for event in self.event_batch:
                    event_data = EventData(
                        body=json.dumps(event.dict(), default=str)
                    )
                    event_data.properties = {
                        'event_type': event.event_type,
                        'event_id': event.event_id,
                        'timestamp': event.timestamp.isoformat()
                    }
                    
                    try:
                        event_data_batch.add(event_data)
                    except ValueError:
                        # Batch is full, send it and create a new one
                        await self.producer.send_batch(event_data_batch)
                        event_data_batch = await self.producer.create_batch()
                        event_data_batch.add(event_data)
                        
                # Send the final batch
                await self.producer.send_batch(event_data_batch)
                
                batch_size = len(self.event_batch)
                self.total_events_sent += batch_size
                self.total_batches_sent += 1
                
                logger.debug(f"Sent batch of {batch_size} events to {self.eventhub_name}")
                
                # Clear the batch
                self.event_batch.clear()
                self.last_flush_time = datetime.utcnow()
                
        except EventHubError as e:
            logger.error(f"Failed to send batch to Event Hub: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error sending batch: {e}")
            raise
            
    async def _periodic_flush(self):
        """Background task to periodically flush events."""
        while self._running:
            try:
                await asyncio.sleep(self.flush_interval)
                
                # Check if we need to flush based on time
                time_since_flush = (datetime.utcnow() - self.last_flush_time).total_seconds()
                if time_since_flush >= self.flush_interval and self.event_batch:
                    await self._flush_batch()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic flush: {e}")
                
    def get_stats(self) -> Dict[str, Any]:
        """Get producer statistics.
        
        Returns:
            Dictionary with producer stats
        """
        return {
            'total_events_sent': self.total_events_sent,
            'total_batches_sent': self.total_batches_sent,
            'current_batch_size': len(self.event_batch),
            'avg_batch_size': (
                self.total_events_sent / self.total_batches_sent 
                if self.total_batches_sent > 0 else 0
            )
        }


class MultiEventHubProducer:
    """Producer that can send to multiple Event Hubs."""
    
    def __init__(self, connection_string: str, batch_size: int = 100):
        """Initialize multi-hub producer.
        
        Args:
            connection_string: Event Hub namespace connection string
            batch_size: Default batch size for all producers
        """
        self.connection_string = connection_string
        self.batch_size = batch_size
        self.producers: Dict[str, EventHubProducer] = {}
        
    async def add_hub(self, hub_name: str):
        """Add a new Event Hub to send to.
        
        Args:
            hub_name: Name of the Event Hub
        """
        if hub_name not in self.producers:
            producer = EventHubProducer(
                self.connection_string,
                hub_name,
                self.batch_size
            )
            await producer.start()
            self.producers[hub_name] = producer
            logger.info(f"Added Event Hub: {hub_name}")
            
    async def send_to_hub(self, hub_name: str, event: StreamingEvent):
        """Send event to specific hub.
        
        Args:
            hub_name: Target Event Hub name
            event: Event to send
        """
        if hub_name not in self.producers:
            await self.add_hub(hub_name)
        await self.producers[hub_name].send_event(event)
        
    async def close_all(self):
        """Close all producers."""
        for producer in self.producers.values():
            await producer.stop()
        self.producers.clear()