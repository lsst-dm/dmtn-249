from __future__ import annotations

__all__ = ("RemovalTransaction",)

import uuid
from typing import TYPE_CHECKING, Self

import pydantic
from lsst.resources import ResourcePath

from .primitives import DatasetRef
from .raw_batch import RawBatch
from .transactions import ArtifactTransaction, ArtifactTransactionHeader

if TYPE_CHECKING:
    from .butler import Datastore


class RemovalTransaction(pydantic.BaseModel, ArtifactTransaction):
    refs: dict[uuid.UUID, DatasetRef]

    @classmethod
    def load(cls, header: ArtifactTransactionHeader, workspace_root: ResourcePath | None) -> Self:
        return cls.model_validate(header.data)

    def abandon(self, datastore: Datastore) -> None:
        datastore.unstore(self.refs.values())

    def commit(self, datastore: Datastore, batch: RawBatch) -> None:
        batch.opaque_table_insertions = datastore.opaque_record_set_type.make_empty()
        for ref in self.refs.values():
            assert ref._opaque_records is not None
            if datastore.verify(ref):
                batch.opaque_table_insertions.update(ref._opaque_records)
