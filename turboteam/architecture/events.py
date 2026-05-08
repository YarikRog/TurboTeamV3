from dataclasses import dataclass, field
from typing import Any, Dict, Optional


USER_REGISTERED = "USER_REGISTERED"
TRAINING_SELECTED = "TRAINING_SELECTED"
VIDEO_UPLOADED = "VIDEO_UPLOADED"
HP_GRANTED = "HP_GRANTED"
REST_SELECTED = "REST_SELECTED"
SKIP_SELECTED = "SKIP_SELECTED"
PENALTY_APPLIED = "PENALTY_APPLIED"


@dataclass(slots=True)
class EventEnvelope:
    name: str
    user_id: int
    payload: Dict[str, Any] = field(default_factory=dict)
    idempotency_key: Optional[str] = None
