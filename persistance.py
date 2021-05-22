from collections import defaultdict, deque
from functools import reduce
from typing import Dict, Deque, Sequence, Generator, Generic, Type
from uuid import UUID

from domain import Event, TEntity, Entity


class EventStore:
    def __init__(self) -> None:
        self._global_stream: Deque[Event] = deque()
        self._event_streams: Dict[UUID, Deque[Event]] = defaultdict(deque)

    def store(self, producer_id: UUID, events: Sequence[Event]) -> None:
        self._global_stream.extend(events)
        self._event_streams[producer_id].extend(events)

    def all_events_for(self, producer_id: UUID) -> Generator[Event, None, None]:
        return (e for e in self._event_streams[producer_id])

    def all_streams(self) -> Generator[Event, None, None]:
        return (e for e in self._global_stream)


class Repository(Generic[TEntity]):
    def __init__(self, entity_class: Type[Entity], event_store: EventStore) -> None:
        self._entity_class = entity_class
        self._event_store = event_store

    def save(self, account: TEntity) -> None:
        self._event_store.store(account.id, account.uncommitted_changes)

    def _apply_event(self, ag: TEntity, e: Event) -> TEntity:
        ag.hydrate(e)
        return ag

    def get(self, entity_id: UUID) -> TEntity:
        changes = self._event_store.all_events_for(entity_id)
        root: TEntity = self._entity_class.construct()

        final_form: TEntity = reduce(self._apply_event, changes, root)

        return final_form
