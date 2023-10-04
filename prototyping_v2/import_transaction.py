from __future__ import annotations

__all__ = ("ImportTransaction",)

from typing import TYPE_CHECKING, Self, Any

import pydantic
from lsst.resources import ResourcePath

from .config import DatastoreConfig
from .raw_batch import DatasetInsertion, RawBatch
from .transactions import ArtifactTransaction, ArtifactTransactionHeader

if TYPE_CHECKING:
    from .artifact_transfer import ArtifactTransferManifest
    from .butler import Datastore


class _HeaderData(pydantic.BaseModel):
    batch: RawBatch
    manifest: ArtifactTransferManifest
    origin_root: ResourcePath
    origin_config: DatastoreConfig


class ImportTransaction(ArtifactTransaction):
    def __init__(
        self,
        batch: RawBatch,
        manifest: ArtifactTransferManifest,
        origin: Datastore,
    ):
        self._batch = batch
        self._manifest = manifest
        self._origin = origin

    @classmethod
    def make_header_data(
        cls,
        batch: RawBatch,
        manifest: ArtifactTransferManifest,
        origin_root: ResourcePath,
        origin_config: DatastoreConfig,
    ) -> dict[str, Any]:
        return _HeaderData(
            batch=batch, manifest=manifest, origin_root=origin_root, origin_config=origin_config
        ).model_dump()

    @classmethod
    def load(cls, header: ArtifactTransactionHeader, workspace_root: ResourcePath | None) -> Self:
        validated_data = _HeaderData.model_validate(header.data)
        return cls(
            batch=validated_data.batch,
            manifest=validated_data.manifest,
            origin=validated_data.origin_config.make_datastore(validated_data.origin_root),
        )

    def abandon(self, datastore: Datastore) -> None:
        datastore.abandon_transfer_manifest(self._manifest)

    def commit(self, datastore: Datastore, batch: RawBatch) -> None:
        batch.update(self._batch)
        for ref in self._manifest.extract_refs():
            batch.dataset_insertions.setdefault(ref.run, {}).setdefault(ref.dataset_type.name, []).append(
                DatasetInsertion(uuid=ref.uuid, data_coordinate_values=ref.data_id.values_tuple())
            )
        # Do the actual datastore artifact transfers.
        records = datastore.execute_transfer_manifest(self._manifest, self._origin)
        # Include the records derived from those artifact transfers in the
        # registry batch.
        if batch.opaque_table_insertions is None:
            batch.opaque_table_insertions = records
        else:
            batch.opaque_table_insertions.update(records)
