from __future__ import annotations

__all__ = ("TransferTransaction",)

import dataclasses
import uuid
from collections.abc import Mapping, Set
from typing import Self, Any, TypedDict

import pydantic
from lsst.daf.butler import StoredDatastoreItemInfo, DatasetId
from lsst.resources import ResourcePath

from .aliases import CollectionName, DatastoreTableName, StorageURI
from .primitives import DatasetRef
from .raw_batch import RawBatch
from .artifact_transaction import ArtifactTransaction
from .datastore import Datastore, ArtifactTransferResponse
from .persistent_limited_butler import PersistentLimitedButlerConfig


class SerializedTransfer(TypedDict):
    responses: Any
    origin_root: ResourcePath | None
    origin_config: PersistentLimitedButlerConfig
    refs: list[DatasetRef]


SERIALIZED_TRANSFER_ADAPTER: pydantic.TypeAdapter[SerializedTransfer] = pydantic.TypeAdapter(
    SerializedTransfer
)


@dataclasses.dataclass
class TransferTransaction(ArtifactTransaction):
    responses: list[ArtifactTransferResponse]
    origin_root: ResourcePath | None
    origin_config: PersistentLimitedButlerConfig
    refs: dict[DatasetId, DatasetRef] = pydantic.Field(default_factory=dict, exclude=True)

    @classmethod
    def from_header_data(
        cls, header_data: Any, workspace_root: ResourcePath | None, datastore: Datastore
    ) -> Self:
        validated: SerializedTransfer = SERIALIZED_TRANSFER_ADAPTER.validate_python(header_data)
        return cls(
            responses=datastore.deserialize_transfer_to(validated["responses"]),
            origin_root=validated["origin_root"],
            origin_config=validated["origin_config"],
            refs={ref.id: ref for ref in validated["refs"]},
        )

    def get_header_data(self, datastore: Datastore) -> Any:
        return SERIALIZED_TRANSFER_ADAPTER.dump_python(
            dict(
                responses=datastore.serialize_transfer_to(self.responses),
                origin_root=self.origin_root,
                origin_config=self.origin_config,
                refs=list(self.refs.values()),
            )
        )

    def get_operation_name(self) -> str:
        return "transfer"

    def get_runs(self) -> Set[CollectionName]:
        return {ref.run for ref in self.refs.values()}

    def get_unstores(self) -> Set[uuid.UUID]:
        return frozenset()

    def get_uris(self, datastore: Datastore) -> list[StorageURI]:
        return datastore.predict_new_uris(self.refs.values())

    def commit_phase_one(
        self,
        datastore: Datastore,
        paths: Mapping[StorageURI, ResourcePath],
    ) -> None:
        origin = self.origin_config.make_butler(self.origin_root)
        datastore.execute_transfer_to(self.responses, paths, origin)

    def commit_phase_two(
        self,
        datastore: Datastore,
        paths: Mapping[StorageURI, ResourcePath],
    ) -> tuple[RawBatch, dict[DatastoreTableName, list[StoredDatastoreItemInfo]]]:
        records: dict[DatastoreTableName, list[StoredDatastoreItemInfo]] = {
            table_name: [] for table_name in datastore.tables.keys()
        }
        for ref in self.refs.values():
            valid, new_records_for_ref = datastore.verify(ref, paths)
            if not valid:
                raise RuntimeError(f"Artifacts for {ref} were not transferred.")
            for table_name, records_for_table in new_records_for_ref.items():
                records[table_name].extend(records_for_table)
        return RawBatch(), records

    def abandon_phase_one(
        self,
        datastore: Datastore,
        paths: Mapping[StorageURI, ResourcePath],
    ) -> None:
        # Can't do anything on client, since we can't delete through signed
        # URLs.
        pass

    def abandon_phase_two(
        self,
        datastore: Datastore,
        paths: Mapping[StorageURI, ResourcePath],
    ) -> tuple[RawBatch, dict[DatastoreTableName, list[StoredDatastoreItemInfo]]]:
        datastore.unstore(self.refs.values())
        return RawBatch(), {}
