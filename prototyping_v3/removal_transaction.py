from __future__ import annotations

__all__ = ("RemovalTransaction",)

import uuid
import warnings
from collections.abc import Mapping, Set
from typing import TYPE_CHECKING, Self, Any

import pydantic
from lsst.resources import ResourcePath

from .aliases import CollectionName
from .primitives import Permissions, DatasetRef
from .raw_batch import RawBatch
from .opaque import OpaqueRecordSet
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

    def get_uris(self, resolution: ArtifactTransactionResolution) -> dict[ResourcePath, Permissions]:
        match resolution:
            case ArtifactTransactionResolution.COMMIT:
                permission = Permissions.DELETE
            case ArtifactTransactionResolution.ABANDON:
                permission = Permissions.GET
        result = {}
        for ref in self.refs.values():
            assert ref._opaque_records is not None
            for uri in ref._opaque_records.extract_files():
                result[uri] = permission
        return result

    def run(
        self,
        resolution: ArtifactTransactionResolution,
        datastore: Datastore,
        uris: Mapping[ResourcePath, ResourcePath],
        verify: bool = False,
    ) -> tuple[RawBatch, OpaqueRecordSet, bool]:
        for ref in self.refs.values():
            assert ref._opaque_records is not None
            ref._opaque_records.add_signed_uris(uris)
        match resolution:
            case ArtifactTransactionResolution.COMMIT:
                datastore.unstore(self.refs.values())
                return RawBatch(), datastore.opaque_record_set_type.make_empty(), True
            case ArtifactTransactionResolution.ABANDON:
                records = datastore.opaque_record_set_type.make_empty()
                for ref in self.refs.values():
                    if datastore.verify(ref):
                        records.update(cast(OpaqueRecordSet, ref._opaque_records))
                    else:
                        warnings.warn(
                            f"{ref} has already been deleted; transaction cannot be fully abandoned."
                        )
                return RawBatch(), records, True

    def verify(
        self,
        resolution: ArtifactTransactionResolution,
        datastore: Datastore,
        uris: Mapping[ResourcePath, ResourcePath],
        records: OpaqueRecordSet,
    ) -> None:
        match resolution:
            case ArtifactTransactionResolution.COMMIT:
                if records:
                    raise ValueError("Commit validation failed: records should be empty.")
                for ref in self.refs.values():
                    assert 
                # TODO: check that no artifacts exist and no records were
                # passed.
            case ArtifactTransactionResolution.ABANDON:
                # TODO: check that artifacts exist for the records provided.
                # and do not exist for any other records.
                