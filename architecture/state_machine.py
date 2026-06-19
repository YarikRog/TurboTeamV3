import logging
from typing import Any, Optional

from cache import KeyManager, delete_data, get_data, set_data

logger = logging.getLogger(__name__)


class UserFlowState:
    NEW_USER = "NEW_USER"
    REGISTERED = "REGISTERED"
    TRAINING_STARTED = "TRAINING_STARTED"
    VIDEO_WAITING = "VIDEO_WAITING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    PENALIZED = "PENALIZED"


class StateMachineService:
    _ALLOWED_TRANSITIONS = {
        UserFlowState.NEW_USER: {
            UserFlowState.REGISTERED,
            UserFlowState.TRAINING_STARTED,
            UserFlowState.COMPLETED,
            UserFlowState.PENALIZED,
        },
        UserFlowState.REGISTERED: {UserFlowState.TRAINING_STARTED, UserFlowState.COMPLETED, UserFlowState.PENALIZED},
        UserFlowState.TRAINING_STARTED: {UserFlowState.VIDEO_WAITING},
        UserFlowState.VIDEO_WAITING: {UserFlowState.PROCESSING, UserFlowState.COMPLETED, UserFlowState.PENALIZED},
        UserFlowState.PROCESSING: {UserFlowState.VIDEO_WAITING, UserFlowState.COMPLETED, UserFlowState.PENALIZED},
        UserFlowState.COMPLETED: {UserFlowState.TRAINING_STARTED, UserFlowState.COMPLETED, UserFlowState.PENALIZED},
        UserFlowState.PENALIZED: {UserFlowState.TRAINING_STARTED, UserFlowState.COMPLETED, UserFlowState.PENALIZED},
    }

    async def get_state(self, user_id: int) -> str:
        state = await get_data(KeyManager.get_state_key(user_id))
        return str(state) if state else UserFlowState.NEW_USER

    async def transition(self, user_id: int, new_state: str, ttl: Optional[int] = None) -> bool:
        current_state = await self.get_state(user_id)
        allowed_targets = self._ALLOWED_TRANSITIONS.get(current_state, set())
        if current_state != new_state and new_state not in allowed_targets:
            logger.warning(
                "[STATE] Rejected transition uid=%s %s -> %s",
                user_id,
                current_state,
                new_state,
            )
            return False

        await set_data(KeyManager.get_state_key(user_id), new_state, ex=ttl)
        return True

    async def register_user(self, user_id: int) -> bool:
        return await self.transition(user_id, UserFlowState.REGISTERED)

    async def begin_training(self, user_id: int, action: str, ttl: int = 600) -> bool:
        if not await self.transition(user_id, UserFlowState.TRAINING_STARTED, ttl=ttl):
            return False
        await set_data(KeyManager.get_session_key(user_id), {"action": action}, ex=ttl)
        return await self.transition(user_id, UserFlowState.VIDEO_WAITING, ttl=ttl)

    async def mark_processing(self, user_id: int, ttl: int = 30) -> bool:
        return await self.transition(user_id, UserFlowState.PROCESSING, ttl=ttl)

    async def restore_video_waiting(self, user_id: int, ttl: int = 60) -> bool:
        return await self.transition(user_id, UserFlowState.VIDEO_WAITING, ttl=ttl)

    async def complete(self, user_id: int) -> bool:
        await delete_data(KeyManager.get_session_key(user_id))
        return await self.transition(user_id, UserFlowState.COMPLETED)

    async def penalize(self, user_id: int) -> bool:
        return await self.transition(user_id, UserFlowState.PENALIZED)

    async def get_session(self, user_id: int) -> Optional[dict[str, Any]]:
        session = await get_data(KeyManager.get_session_key(user_id))
        return session if isinstance(session, dict) else None


state_machine = StateMachineService()
