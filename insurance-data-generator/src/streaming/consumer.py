"""Event Hub consumer for processing insurance data streams."""

import asyncio
import json
import logging
from datetime import datetime
from typing import Callable, Dict, Any, Optional, List
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub import EventData
from azure.eventhub.extensions.checkpointstoreblob import BlobCheckpointStore
from azure.storage.blob.aio import BlobServiceClient
from azure.eventhub.exceptions import EventHubError


logger = logging.getLogger(__name__)


class EventProcessor:
    """Base class for event processors."""
    
    async def process(self, event_data: Dict[str, Any]) -> None:
        """Process a single event.
        
        Args:
            event_data: Deserialized event data
        """
        raise NotImplementedError


class EventHubConsumer:
    """Async Event Hub consumer with checkpointing."""
    
    def __init__(
        self,
        connection_string: str,
        eventhub_name: str,
        consumer_group: str = "$Default",
        storage_connection_string: Optional[str] = None,
        checkpoint_container: Optional[str] = None,
        processor: Optional[EventProcessor] = None
    ):
        """Initialize the Event Hub consumer.
        
        Args:
            connection_string: Event Hub connection string
            eventhub_name: Name of the Event Hub
            consumer_group: Consumer group name
            storage_connection_string: Storage connection for checkpointing
            checkpoint_container: Container name for checkpoints
            processor: Optional event processor
        """
        self.connection_string = connection_string
        self.eventhub_name = eventhub_name
        self.consumer_group = consumer_group
        self.storage_connection_string = storage_connection_string
        self.checkpoint_container = checkpoint_container
        self.processor = processor
        
        self.consumer: Optional[EventHubConsumerClient] = None
        self.checkpoint_store: Optional[BlobCheckpointStore] = None
        self.total_events_received = 0
        self.total_events_processed = 0
        self.total_errors = 0
        self._running = False
        self._handlers: Dict[str, List[Callable]] = {}
        
    async def __aenter__(self):
        """Async context manager entry."""
        await self.start()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.stop()
        
    async def start(self):
        """Start the consumer."""
        try:
            # Setup checkpoint store if storage is configured
            if self.storage_connection_string and self.checkpoint_container:
                blob_client = BlobServiceClient.from_connection_string(
                    self.storage_connection_string
                )
                self.checkpoint_store = BlobCheckpointStore(
                    blob_client=blob_client,
                    container_name=self.checkpoint_container
                )
                
            # Create consumer client
            if self.checkpoint_store:
                self.consumer = EventHubConsumerClient.from_connection_string(
                    conn_str=self.connection_string,
                    consumer_group=self.consumer_group,
                    eventhub_name=self.eventhub_name,
                    checkpoint_store=self.checkpoint_store
                )
            else:
                self.consumer = EventHubConsumerClient.from_connection_string(
                    conn_str=self.connection_string,
                    consumer_group=self.consumer_group,
                    eventhub_name=self.eventhub_name
                )
                
            self._running = True
            logger.info(f"Started Event Hub consumer for {self.eventhub_name}")
            
        except Exception as e:
            logger.error(f"Failed to start consumer: {e}")
            raise
            
    async def stop(self):
        """Stop the consumer."""
        self._running = False
        
        if self.consumer:
            await self.consumer.close()
            logger.info(
                f"Stopped Event Hub consumer. "
                f"Total events: received={self.total_events_received}, "
                f"processed={self.total_events_processed}, errors={self.total_errors}"
            )
            
    def register_handler(self, event_type: str, handler: Callable):
        """Register a handler for specific event types.
        
        Args:
            event_type: Type of event to handle
            handler: Async function to handle the event
        """
        if event_type not in self._handlers:
            self._handlers[event_type] = []
        self._handlers[event_type].append(handler)
        logger.debug(f"Registered handler for event type: {event_type}")
        
    async def on_event(self, partition_context, events):
        """Process received events.
        
        Args:
            partition_context: Partition context for checkpointing
            events: List of received events
        """
        if not events:
            return
            
        for event in events:
            try:
                self.total_events_received += 1
                
                # Parse event body
                event_data = json.loads(event.body_as_str())
                event_type = event_data.get('event_type', 'unknown')
                
                # Log event receipt
                logger.debug(
                    f"Received event: type={event_type}, "
                    f"partition={partition_context.partition_id}, "
                    f"offset={event.offset}, "
                    f"sequence={event.sequence_number}"
                )
                
                # Process with registered processor
                if self.processor:
                    await self.processor.process(event_data)
                    
                # Call type-specific handlers
                if event_type in self._handlers:
                    for handler in self._handlers[event_type]:
                        await handler(event_data)
                        
                # Call wildcard handlers
                if '*' in self._handlers:
                    for handler in self._handlers['*']:
                        await handler(event_data)
                        
                self.total_events_processed += 1
                
                # Update checkpoint every 10 events
                if self.total_events_processed % 10 == 0:
                    await partition_context.update_checkpoint(event)
                    
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse event: {e}")
                self.total_errors += 1
            except Exception as e:
                logger.error(f"Error processing event: {e}")
                self.total_errors += 1
                
        # Final checkpoint for the batch
        try:
            await partition_context.update_checkpoint(events[-1])
        except Exception as e:
            logger.error(f"Failed to update checkpoint: {e}")
            
    async def on_error(self, partition_context, error):
        """Handle errors during event processing.
        
        Args:
            partition_context: Partition context
            error: The error that occurred
        """
        logger.error(
            f"Error on partition {partition_context.partition_id}: {error}"
        )
        self.total_errors += 1
        
    async def consume(self, starting_position: str = "-1"):
        """Start consuming events.
        
        Args:
            starting_position: Starting position ("-1" for beginning, "@latest" for end)
        """
        if not self.consumer:
            await self.start()
            
        try:
            await self.consumer.receive(
                on_event=self.on_event,
                on_error=self.on_error,
                starting_position=starting_position
            )
        except EventHubError as e:
            logger.error(f"Event Hub error during consumption: {e}")
            raise
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        except Exception as e:
            logger.error(f"Unexpected error during consumption: {e}")
            raise
        finally:
            await self.stop()
            
    def get_stats(self) -> Dict[str, Any]:
        """Get consumer statistics.
        
        Returns:
            Dictionary with consumer stats
        """
        return {
            'total_events_received': self.total_events_received,
            'total_events_processed': self.total_events_processed,
            'total_errors': self.total_errors,
            'processing_rate': (
                self.total_events_processed / self.total_events_received
                if self.total_events_received > 0 else 0
            )
        }


class ClaimEventProcessor(EventProcessor):
    """Processor specifically for claim events."""
    
    def __init__(self):
        """Initialize claim processor."""
        self.claims_processed = 0
        self.fraud_detected = 0
        
    async def process(self, event_data: Dict[str, Any]) -> None:
        """Process claim events.
        
        Args:
            event_data: Claim event data
        """
        if event_data.get('event_type') != 'claim':
            return
            
        claim_data = event_data.get('data', {})
        fraud_score = claim_data.get('fraud_score', 0)
        
        self.claims_processed += 1
        
        # Check for potential fraud
        if fraud_score > 0.7:
            self.fraud_detected += 1
            logger.warning(
                f"High fraud score detected: "
                f"claim_id={claim_data.get('id')}, "
                f"score={fraud_score}"
            )
            
        # Log claim processing
        logger.info(
            f"Processed claim: "
            f"id={claim_data.get('id')}, "
            f"amount={claim_data.get('claim_amount')}, "
            f"type={claim_data.get('claim_type')}"
        )