import logging
from collections import defaultdict
from typing import Awaitable, Callable, Dict, List

from cache import KeyManager, acquire_lock
from architecture.events import EventEnvelope

logger = logging.getLogger(__name__)

Subscriber = Callable[[EventEnvelope], Awaitable[object]]


class EventBus:
    def __init__(self) -> None:
        self._subscribers: Dict[str, List[Subscriber]] = defaultdict(list)

    def subscribe(self, event_name: str, handler: Subscriber) -> None:
        self._subscribers[event_name].append(handler)

    async def publish(self, event: EventEnvelope) -> list[object]:
        if event.idempotency_key:
            lock_key = KeyManager.get_event_idempotency_key(event.idempotency_key)
            accepted = await acquire_lock(lock_key, ex=300)
            if not accepted:
                logger.info("[EVENT BUS] Duplicate event skipped: %s", event.idempotency_key)
                return []

        results: list[object] = []
        for handler in self._subscribers.get(event.name, []):
            results.append(await handler(event))
        return results
