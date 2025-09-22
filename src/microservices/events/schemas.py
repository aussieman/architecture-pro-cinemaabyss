from pydantic import BaseModel, EmailStr
from typing import List, Optional, Any
from datetime import datetime


class MovieEvent(BaseModel):
    movie_id: int
    title: str
    action: str
    user_id: Optional[int] = None
    rating: Optional[float] = None
    genres: Optional[List[str]] = None
    description: Optional[str] = None


class UserEvent(BaseModel):
    user_id: int
    username: Optional[str] = None
    email: Optional[EmailStr] = None
    action: str
    timestamp: datetime


class PaymentEvent(BaseModel):
    payment_id: int
    user_id: int
    amount: float
    status: str
    timestamp: datetime
    method_type: Optional[str] = None


class Event(BaseModel):
    id: str
    type: str
    timestamp: datetime
    payload: Any


class EventResponse(BaseModel):
    status: str
    partition: int
    offset: int
    event: Event


class HealthResponse(BaseModel):
    status: bool
