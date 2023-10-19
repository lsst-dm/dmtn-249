from __future__ import annotations

__all__ = ("PutTransaction",)

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
from .raw_batch import DatasetRegistration, RawBatch

if TYPE_CHECKING:
    from .datastore import Datastore


class PutTransaction(pydantic.BaseModel, ArtifactTransaction):
    """An artifact transaction implementation that writes several in-memory
    datasets to the repository.
    """

    refs: dict[uuid.UUID, DatasetRef]

    @classmethod
    def from_header_data(
        cls, header_data: Any, workspace_root: ResourcePath | None, datastore: Datastore
    ) -> Self:
        return cls.model_validate(header_data)

    def get_header_data(self, datastore: Datastore) -> Any:
        return self.model_dump()

    def get_operation_name(self) -> str:
        return "put"

    def get_runs(self) -> Set[CollectionName]:
        return frozenset(ref.run for ref in self.refs.values())

    def get_initial_batch(self) -> RawBatch:
        # We attempt to register all datasets up front, to avoid writing
        # artifacts if there's a constraint violation.
        batch = RawBatch()
        for ref in self.refs.values():
            batch.dataset_registrations.setdefault(ref.run, {}).setdefault(ref.datasetType.name, []).append(
                DatasetRegistration(uuid=ref.id, data_coordinate_values=ref.dataId.values_tuple())
            )
        return batch

    def commit(
        self, datastore: Datastore
    ) -> tuple[RawBatch, dict[DatastoreTableName, list[StoredDatastoreItemInfo]]]:
        records, _, missing, corrupted = self.verify_artifacts(datastore, self.refs)
        if missing or corrupted:
            raise RuntimeError(
                f"{len(missing)} dataset(s) were not written and {len(corrupted)} had missing or "
                f"invalid artifacts; transaction must be reverted or abandoned."
            )
        return RawBatch(), records

    def abandon(
        self, datastore: Datastore
    ) -> tuple[RawBatch, dict[DatastoreTableName, list[StoredDatastoreItemInfo]]]:
        records, _, missing, corrupted = self.verify_artifacts(datastore, self.refs)
        for ref in missing:
            warnings.warn(f"{ref} was not written and will remain unstored.")
        for ref in corrupted:
            warnings.warn(f"{ref} was not fully written and will be unstored.")
        datastore.unstore(corrupted)
        return RawBatch(), records

    def revert(
        self, datastore: Datastore
    ) -> tuple[RawBatch, dict[DatastoreTableName, list[StoredDatastoreItemInfo]]]:
        datastore.unstore(self.refs.values())
        batch = RawBatch()
        batch.dataset_removals.update(self.refs.keys())
        return batch, {}
