"""Configuration settings for the insurance data generator."""

from pydantic_settings import BaseSettings
from pydantic import Field, field_validator
from typing import Optional
import os
from pathlib import Path


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Azure Event Hub
    eventhub_connection_string: str = Field(..., env='EVENTHUB_CONNECTION_STRING')
    eventhub_name_claims: str = Field('claims-realtime', env='EVENTHUB_NAME_CLAIMS')
    eventhub_name_policies: str = Field('policies-stream', env='EVENTHUB_NAME_POLICIES')
    eventhub_name_audit: str = Field('audit-events', env='EVENTHUB_NAME_AUDIT')
    eventhub_consumer_group: str = Field('$Default', env='EVENTHUB_CONSUMER_GROUP')
    
    # Azure Storage (for checkpointing)
    storage_connection_string: Optional[str] = Field(None, env='STORAGE_CONNECTION_STRING')
    checkpoint_container_name: str = Field('eventhub-checkpoints', env='CHECKPOINT_CONTAINER_NAME')
    
    # Application
    environment: str = Field('development', env='ENVIRONMENT')
    log_level: str = Field('INFO', env='LOG_LEVEL')
    batch_size: int = Field(100, env='BATCH_SIZE')
    flush_interval_seconds: int = Field(5, env='FLUSH_INTERVAL_SECONDS')
    
    # Fraud Detection
    fraud_injection_rate: float = Field(0.05, env='FRAUD_INJECTION_RATE')
    suspicious_pattern_rate: float = Field(0.02, env='SUSPICIOUS_PATTERN_RATE')
    
    # Streaming
    max_events_per_second: int = Field(100, env='MAX_EVENTS_PER_SECOND')
    enable_real_time_streaming: bool = Field(True, env='ENABLE_REAL_TIME_STREAMING')
    
    # Dashboard
    dashboard_port: int = Field(8501, env='DASHBOARD_PORT')
    api_port: int = Field(8000, env='API_PORT')
    
    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'
        case_sensitive = False
    
    @field_validator('fraud_injection_rate', 'suspicious_pattern_rate')
    @classmethod
    def validate_rates(cls, v):
        """Ensure rates are between 0 and 1."""
        if not 0 <= v <= 1:
            raise ValueError('Rate must be between 0 and 1')
        return v


# Singleton instance
_settings = None

def get_settings() -> Settings:
    """Get cached settings instance."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings