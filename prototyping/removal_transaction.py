from __future__ import annotations

__all__ = ("RemovalTransaction",)

import uuid
import warnings
from collections.abc import Set
from typing import TYPE_CHECKING, Any, Self

import pydantic
from lsst.daf.butler import StoredDatastoreItemInfo
from lsst.resources import ResourcePath

from .aliases import CollectionName, DatastoreTableName
from .artifact_transaction import ArtifactTransaction
from .primitives import DatasetRef
from .raw_batch import RawBatch

if TYPE_CHECKING:
    from .datastore import Datastore


class RemovalTransaction(pydantic.BaseModel, ArtifactTransaction):
    """An artifact transaction implementation that removes datasets."""

    refs: dict[uuid.UUID, DatasetRef]
    purge: bool

    @classmethod
    def from_header_data(
        cls, header_data: Any, workspace_root: ResourcePath | None, datastore: Datastore
    ) -> Self:
        return cls.model_validate(header_data)

    def get_header_data(self, datastore: Datastore) -> Any:
        return self.model_dump()

    def get_operation_name(self) -> str:
        return "remove"

    def get_runs(self) -> Set[CollectionName]:
        return frozenset(ref.run for ref in self.refs.values())

    def get_unstores(self) -> Set[uuid.UUID]:
        return self.refs.keys()

    def commit(
        self, datastore: Datastore
    ) -> tuple[RawBatch, dict[DatastoreTableName, list[StoredDatastoreItemInfo]]]:
        datastore.unstore(self.refs.values())
        batch = RawBatch()
        if self.purge:
            batch.dataset_removals.update(self.refs.keys())
        return batch, {}

    def abandon(
        self, datastore: Datastore
    ) -> tuple[RawBatch, dict[DatastoreTableName, list[StoredDatastoreItemInfo]]]:
        records, present, _, corrupted = self.verify_artifacts(datastore, self.refs)
        for ref in present:
            warnings.warn(f"{ref} was not deleted and will remain stored.")
        for ref in corrupted:
            warnings.warn(f"{ref} was corrupted and will be removed.")
        datastore.unstore(corrupted)
        return RawBatch(), records

    def revert(
        self, datastore: Datastore
    ) -> tuple[RawBatch, dict[DatastoreTableName, list[StoredDatastoreItemInfo]]]:
        records, _, missing, corrupted = self.verify_artifacts(datastore, self.refs)
        if missing or corrupted:
            raise RuntimeError(
                f"{len(missing)} dataset(s) have already been deleted and {len(corrupted)} had missing or "
                f"invalid artifacts; transaction must be committed or abandoned."
            )
        return RawBatch(), records
