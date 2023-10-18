from __future__ import annotations

__all__ = ("PutTransaction",)

import uuid
import warnings
from collections.abc import Mapping, Set
from typing import TYPE_CHECKING, Self, Any

import pydantic
from lsst.daf.butler import StoredDatastoreItemInfo
from lsst.resources import ResourcePath

from .aliases import CollectionName, DatastoreTableName, StorageURI
from .primitives import DatasetRef
from .raw_batch import RawBatch, DatasetInsertion
from .artifact_transaction import ArtifactTransaction

if TYPE_CHECKING:
    from .datastore import Datastore


class PutTransaction(pydantic.BaseModel, ArtifactTransaction):
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

    def get_uris(self, datastore: Datastore) -> list[StorageURI]:
        return datastore.predict_new_uris(self.refs.values())

    def get_initial_batch(self) -> RawBatch:
        # We attempt to register all datasets up front, to avoid writing
        # artifacts if there's a constraint violation.
        batch = RawBatch()
        for ref in self.refs.values():
            batch.dataset_insertions.setdefault(ref.run, {}).setdefault(ref.datasetType.name, []).append(
                DatasetInsertion(uuid=ref.id, data_coordinate_values=ref.dataId.values_tuple())
            )
        return batch

    def commit_phase_one(
        self,
        datastore: Datastore,
        paths: Mapping[StorageURI, ResourcePath],
    ) -> None:
        # Can't do anything on client inside the transaction object because it
        # doesn't have access to the in-memory object(s); we rely on other code
        # calling datastore.put_many after the transaction is opened and before
        # it is committed.
        pass

    def commit_phase_two(
        self,
        datastore: Datastore,
    ) -> tuple[RawBatch, dict[DatastoreTableName, list[StoredDatastoreItemInfo]]]:
        records, _, missing, corrupted = self._verify_artifacts(datastore, self.refs)
        if missing or corrupted:
            raise RuntimeError(
                f"{len(missing)} dataset(s) were not written and {len(corrupted)} had missing or "
                f"invalid artifacts; transaction must be reverted or abandoned."
            )
        return RawBatch(), records

    def abandon_phase_one(
        self,
        datastore: Datastore,
        paths: Mapping[StorageURI, ResourcePath],
    ) -> None:
        pass

    def abandon_phase_two(
        self,
        datastore: Datastore,
    ) -> tuple[RawBatch, dict[DatastoreTableName, list[StoredDatastoreItemInfo]]]:
        records, _, missing, corrupted = self._verify_artifacts(datastore, self.refs)
        for ref in missing:
            warnings.warn(f"{ref} was not written and will remain unstored.")
        for ref in corrupted:
            warnings.warn(f"{ref} was not fully written and will be unstored.")
        datastore.unstore(corrupted)
        return RawBatch(), records

    def revert_phase_one(
        self,
        datastore: Datastore,
        paths: Mapping[StorageURI, ResourcePath],
    ) -> None:
        # Can't do anything on client, since we can't delete through signed
        # URLs.
        pass

    def revert_phase_two(
        self,
        datastore: Datastore,
    ) -> tuple[RawBatch, dict[DatastoreTableName, list[StoredDatastoreItemInfo]]]:
        datastore.unstore(self.refs.values())
        batch = RawBatch()
        batch.dataset_removals.update(self.refs.keys())
        return batch, {}
