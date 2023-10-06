from __future__ import annotations

__all__ = ("RemovalTransaction",)

import uuid
import warnings
from collections.abc import Mapping, Set
from typing import TYPE_CHECKING, Self, Any

import pydantic
from lsst.daf.butler import StoredDatastoreItemInfo
from lsst.resources import ResourcePath

from .aliases import CollectionName, OpaqueTableName, StorageURI
from .primitives import DatasetRef
from .raw_batch import RawBatch
from .transactions import ArtifactTransaction, ArtifactTransactionResolution

if TYPE_CHECKING:
    from .butler import Datastore


class RemovalTransaction(pydantic.BaseModel, ArtifactTransaction):
    refs: dict[uuid.UUID, DatasetRef]

    @classmethod
    def from_header_data(cls, header_data: Any, workspace_root: ResourcePath | None) -> Self:
        return cls.model_validate(header_data)

    def get_header_data(self) -> Any:
        return self.model_dump()

    def get_operation_name(self) -> str:
        return "remove"

    def get_runs(self) -> Set[CollectionName]:
        return frozenset(ref.run for ref in self.refs.values())

    def get_unstores(self) -> Set[uuid.UUID]:
        return self.refs.keys()

    def get_uris(self, datastore: Datastore) -> list[StorageURI]:
        return datastore.extract_existing_uris(self.refs.values())

    def run_phase_one(
        self,
        resolution: ArtifactTransactionResolution,
        datastore: Datastore,
        paths: Mapping[StorageURI, ResourcePath],
    ) -> dict[OpaqueTableName, list[StoredDatastoreItemInfo]]:
        # Can't do anything on client, since we can't delete through signed
        # URLs.
        return {}

    def run_phase_two(
        self,
        resolution: ArtifactTransactionResolution,
        datastore: Datastore,
        paths: Mapping[StorageURI, ResourcePath],
        records: dict[OpaqueTableName, list[StoredDatastoreItemInfo]],
    ) -> RawBatch:
        match resolution:
            case ArtifactTransactionResolution.COMMIT:
                datastore.unstore(self.refs.values())
            case ArtifactTransactionResolution.ABANDON:
                for ref in self.refs.values():
                    if datastore.verify(ref, paths):
                        assert ref._opaque_records is not None
                        for table_name, records_for_table in ref._opaque_records.items():
                            # TODO: this extend will lead to duplicates when
                            # multiple datasets are part of one file, e.g.
                            # DECam raws.  But deletion of those is already
                            # problematic (in this prototype) in that we don't
                            # have a way to check that all such datasets are
                            # being deleted together.
                            records.setdefault(table_name, []).extend(records_for_table)
                    else:
                        warnings.warn(
                            f"{ref} has already been deleted; transaction cannot be fully abandoned."
                        )
        return RawBatch()
