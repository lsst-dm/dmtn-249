from __future__ import annotations

__all__ = ("RemovalTransaction",)

import uuid
import warnings
from collections.abc import Mapping, Set
from typing import TYPE_CHECKING, Self, Any

import pydantic
from lsst.daf.butler import StoredDatastoreItemInfo
from lsst.resources import ResourcePath

from .aliases import CollectionName, DatastoreTableName, StorageURI
from .primitives import DatasetRef
from .raw_batch import RawBatch
from .artifact_transaction import ArtifactTransaction

if TYPE_CHECKING:
    from .datastore import Datastore


class RemovalTransaction(pydantic.BaseModel, ArtifactTransaction):
    refs: dict[uuid.UUID, DatasetRef]

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

    def get_uris(self, datastore: Datastore) -> list[StorageURI]:
        return datastore.extract_existing_uris(self.refs.values())

    def commit_phase_one(
        self,
        datastore: Datastore,
        paths: Mapping[StorageURI, ResourcePath],
    ) -> None:
        # Can't do anything on client, since we can't delete through signed
        # URLs.
        pass

    def commit_phase_two(
        self,
        datastore: Datastore,
    ) -> tuple[RawBatch, dict[DatastoreTableName, list[StoredDatastoreItemInfo]]]:
        datastore.unstore(self.refs.values())
        return RawBatch(), {}

    def abandon_phase_one(
        self,
        datastore: Datastore,
        paths: Mapping[StorageURI, ResourcePath],
    ) -> None:
        # When abandoning a removal, all we're doing is verifying that
        # artifacts still exist, and we always do verification on the server,
        # so there's nothing to do on the client.
        pass

    def abandon_phase_two(
        self,
        datastore: Datastore,
    ) -> tuple[RawBatch, dict[DatastoreTableName, list[StoredDatastoreItemInfo]]]:
        records, present, _, corrupted = self._verify_artifacts(datastore, self.refs)
        for ref in present:
            warnings.warn(f"{ref} was not deleted and will remain stored.")
        for ref in corrupted:
            warnings.warn(f"{ref} was corrupted and will be removed.")
        datastore.unstore(corrupted)
        return RawBatch(), records

    def revert_phase_one(
        self,
        datastore: Datastore,
        paths: Mapping[StorageURI, ResourcePath],
    ) -> None:
        # When reverting a removal, all we're doing is verifying that
        # artifacts still exist, and we always do verification on the server,
        # so there's nothing to do on the client.
        pass

    def revert_phase_two(
        self,
        datastore: Datastore,
    ) -> tuple[RawBatch, dict[DatastoreTableName, list[StoredDatastoreItemInfo]]]:
        records, _, missing, corrupted = self._verify_artifacts(datastore, self.refs)
        if missing or corrupted:
            raise RuntimeError(
                f"{len(missing)} dataset(s) have already been deleted and {len(corrupted)} had missing or "
                f"invalid artifacts; transaction must be committed or abandoned."
            )
        return RawBatch(), records
