from __future__ import annotations

__all__ = ("TransferTransaction",)

import dataclasses
import uuid
from collections import defaultdict
from collections.abc import Mapping, Set
from typing import Self, Any

import pydantic
from lsst.daf.butler import StoredDatastoreItemInfo, DatasetId
from lsst.resources import ResourcePath

from .aliases import CollectionName, OpaqueTableName, StorageURI
from .primitives import DatasetRef
from .artifact_transfer import ArtifactTransferManifest
from .raw_batch import RawBatch
from .transactions import ArtifactTransaction, ArtifactTransactionResolution
from .datastore import Datastore
from .persistent_limited_butler import PersistentLimitedButlerConfig


class TransferTransaction(pydantic.BaseModel, ArtifactTransaction):
    manifest: ArtifactTransferManifest
    origin_root: ResourcePath
    origin_config: PersistentLimitedButlerConfig
    refs: dict[DatasetId, DatasetRef] = pydantic.Field(default_factory=dict, exclude=True)

    @classmethod
    def from_header_data(cls, header_data: Any, workspace_root: ResourcePath | None) -> Self:
        result = cls.model_validate(header_data)
        for ref in result.manifest.extract_refs():
            result.refs[ref.id] = dataclasses.replace(ref, _opaque_records=None)
        return result

    def get_header_data(self) -> Any:
        return self.manifest.model_dump()

    def get_operation_name(self) -> str:
        return "transfer"

    def get_runs(self) -> Set[CollectionName]:
        return self.manifest.responses.keys()

    def get_unstores(self) -> Set[uuid.UUID]:
        return frozenset()

    def get_uris(self, datastore: Datastore) -> list[StorageURI]:
        return datastore.predict_new_uris(self.refs.values())

    def run_phase_one(
        self,
        resolution: ArtifactTransactionResolution,
        datastore: Datastore,
        paths: Mapping[StorageURI, ResourcePath],
    ) -> dict[OpaqueTableName, list[StoredDatastoreItemInfo]]:
        match resolution:
            case ArtifactTransactionResolution.COMMIT:
                origin = self.origin_config.make_butler(self.origin_root)
                return datastore.execute_transfer_manifest(self.manifest, paths, origin)
            case ArtifactTransactionResolution.ABANDON:
                # Cannot delete on the client, so nothing to do here.
                return {}

    def run_phase_two(
        self,
        resolution: ArtifactTransactionResolution,
        datastore: Datastore,
        paths: Mapping[StorageURI, ResourcePath],
        records: dict[OpaqueTableName, list[StoredDatastoreItemInfo]],
    ) -> RawBatch:
        records_by_dataset: defaultdict[
            DatasetId, defaultdict[OpaqueTableName, list[StoredDatastoreItemInfo]]
        ] = defaultdict(lambda: defaultdict(list))
        for table_name, records_for_table in records.items():
            for record in records_for_table:
                records_by_dataset[record.dataset_id][table_name].append(record)
        refs = self.refs.copy()
        for dataset_id, records_for_dataset in records_by_dataset.items():
            refs[dataset_id] = dataclasses.replace(self.refs[dataset_id], _opaque_records=records_for_dataset)
        match resolution:
            case ArtifactTransactionResolution.COMMIT:
                for ref in refs.values():
                    if not datastore.verify(ref, paths):
                        raise RuntimeError(f"Artifacts for {ref} were not transferred.")
            case ArtifactTransactionResolution.ABANDON:
                datastore.unstore(refs.values())
        return RawBatch()
