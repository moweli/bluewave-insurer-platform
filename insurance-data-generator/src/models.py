"""Data models for insurance entities."""

from datetime import datetime
from typing import Optional, List, Dict, Any
from enum import Enum
from pydantic import BaseModel, Field, field_validator
import uuid


class PolicyType(str, Enum):
    """Insurance policy types."""
    MOTOR = "motor"
    PROPERTY = "property"
    LIABILITY = "liability"
    PROFESSIONAL_INDEMNITY = "professional_indemnity"
    CYBER = "cyber"
    MARINE = "marine"
    AVIATION = "aviation"
    DIRECTORS_OFFICERS = "directors_officers"


class ClaimStatus(str, Enum):
    """Claim status types."""
    SUBMITTED = "submitted"
    UNDER_REVIEW = "under_review"
    INVESTIGATION = "investigation"
    APPROVED = "approved"
    REJECTED = "rejected"
    PAID = "paid"
    CLOSED = "closed"
    REOPENED = "reopened"


class ClaimType(str, Enum):
    """Claim types."""
    COLLISION = "collision"
    THEFT = "theft"
    FIRE = "fire"
    FLOOD = "flood"
    STORM = "storm"
    LIABILITY_INJURY = "liability_injury"
    LIABILITY_DAMAGE = "liability_damage"
    CYBER_BREACH = "cyber_breach"
    PROFESSIONAL_ERROR = "professional_error"
    OTHER = "other"


class FraudIndicator(str, Enum):
    """Fraud indicator types."""
    DUPLICATE_CLAIM = "duplicate_claim"
    SUSPICIOUS_TIMING = "suspicious_timing"
    INFLATED_AMOUNT = "inflated_amount"
    INCONSISTENT_DETAILS = "inconsistent_details"
    BLACKLISTED_ENTITY = "blacklisted_entity"
    UNUSUAL_FREQUENCY = "unusual_frequency"
    NETWORK_FRAUD_RING = "network_fraud_ring"
    DOCUMENT_FORGERY = "document_forgery"


class RiskProfile(str, Enum):
    """Risk profile categories."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    VERY_HIGH = "very_high"


class Address(BaseModel):
    """UK address model."""
    line1: str
    line2: Optional[str] = None
    city: str
    county: Optional[str] = None
    postcode: str
    country: str = "United Kingdom"
    
    @field_validator('postcode')
    @classmethod
    def validate_uk_postcode(cls, v):
        """Basic UK postcode validation."""
        import re
        pattern = r'^[A-Z]{1,2}[0-9][A-Z0-9]? ?[0-9][A-Z]{2}$'
        if not re.match(pattern, v.upper().replace(' ', '')):
            raise ValueError('Invalid UK postcode format')
        return v.upper()


class Policyholder(BaseModel):
    """Policyholder information."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    first_name: str
    last_name: str
    email: str
    phone: str
    date_of_birth: datetime
    address: Address
    risk_score: float = Field(ge=0, le=1)
    is_corporate: bool = False
    company_name: Optional[str] = None
    company_registration: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    @property
    def age(self) -> int:
        """Calculate age from date of birth."""
        today = datetime.utcnow()
        return today.year - self.date_of_birth.year - (
            (today.month, today.day) < (self.date_of_birth.month, self.date_of_birth.day)
        )


class Policy(BaseModel):
    """Insurance policy model."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    policy_number: str
    policyholder_id: str
    policy_type: PolicyType
    start_date: datetime
    end_date: datetime
    premium_amount: float = Field(gt=0)
    sum_insured: float = Field(gt=0)
    deductible: float = Field(ge=0)
    is_active: bool = True
    risk_profile: RiskProfile
    underwriter: str
    broker: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = None
    
    @field_validator('end_date')
    @classmethod
    def validate_end_date(cls, v, info):
        """Ensure end date is after start date."""
        if 'start_date' in info.data and v <= info.data['start_date']:
            raise ValueError('End date must be after start date')
        return v


class Claim(BaseModel):
    """Insurance claim model."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    claim_number: str
    policy_id: str
    policyholder_id: str
    claim_type: ClaimType
    status: ClaimStatus
    incident_date: datetime
    reported_date: datetime
    claim_amount: float = Field(gt=0)
    approved_amount: Optional[float] = None
    description: str
    location: Address
    adjuster_notes: Optional[str] = None
    fraud_score: float = Field(ge=0, le=1, default=0)
    fraud_indicators: List[FraudIndicator] = Field(default_factory=list)
    is_flagged: bool = False
    processing_time_hours: Optional[float] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = None
    paid_at: Optional[datetime] = None
    
    @field_validator('reported_date')
    @classmethod
    def validate_reported_date(cls, v, info):
        """Ensure reported date is after or equal to incident date."""
        if 'incident_date' in info.data and v < info.data['incident_date']:
            raise ValueError('Reported date cannot be before incident date')
        return v
    
    @property
    def reporting_delay_days(self) -> int:
        """Calculate days between incident and reporting."""
        return (self.reported_date - self.incident_date).days


class AuditEvent(BaseModel):
    """Audit event model for compliance tracking."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str
    entity_type: str
    entity_id: str
    user_id: Optional[str] = None
    action: str
    details: Dict[str, Any]
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class StreamingEvent(BaseModel):
    """Wrapper for streaming events to Event Hub."""
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str
    partition_key: str
    data: Dict[str, Any]
    metadata: Dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }