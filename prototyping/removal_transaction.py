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
        records: dict[DatastoreTableName, list[StoredDatastoreItemInfo]] = {
            table_name: [] for table_name in datastore.tables.keys()
        }
        for dataset_id, present, records_for_dataset in datastore.verify(self.refs.values()):
            ref = self.refs[dataset_id]
            if present:
                for table_name, records_for_table in records_for_dataset.items():
                    # TODO: this extend will lead to duplicates when multiple
                    # datasets are part of one file, e.g. DECam raws.  But
                    # deletion of those is already problematic (in this
                    # prototype) in that we don't have a way to check that all
                    # such datasets are being deleted together.
                    records[table_name].extend(records_for_table)
            else:
                warnings.warn(f"{ref} has already been deleted; transaction cannot be fully abandoned.")
        return RawBatch(), records
