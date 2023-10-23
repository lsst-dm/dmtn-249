from __future__ import annotations

__all__ = ("RemovalTransaction",)

import uuid
import warnings
from collections.abc import Set

from .aliases import CollectionName
from .artifact_transaction import (
    ArtifactTransaction,
    ArtifactTransactionCloseContext,
    ArtifactTransactionCommitContext,
    ArtifactTransactionRevertContext,
    ArtifactTransactionOpenContext,
)
from .primitives import DatasetRef


class RemovalTransaction(ArtifactTransaction):
    """An artifact transaction implementation that removes datasets."""

    refs: dict[uuid.UUID, DatasetRef]
    purge: bool

    def get_operation_name(self) -> str:
        return "remove"

    def begin(self, context: ArtifactTransactionOpenContext) -> None:
        context.discard_datastore_records(self.refs.keys())

    def get_insert_only_runs(self) -> Set[CollectionName]:
        return frozenset()

    def get_modified_runs(self) -> Set[CollectionName]:
        return frozenset(ref.run for ref in self.refs.values())

    def commit(self, context: ArtifactTransactionCommitContext) -> None:
        context.datastore.unstore(self.refs.values())
        if self.purge:
            context.discard_datasets(self.refs.keys())

    def revert(self, context: ArtifactTransactionRevertContext) -> None:
        records, _, missing, corrupted = context.verify_artifacts(self.refs)
        if missing or corrupted:
            raise RuntimeError(
                f"{len(missing)} dataset(s) have already been deleted and {len(corrupted)} had missing or "
                f"invalid artifacts; transaction must be committed or abandoned."
            )
        context.insert_datastore_records(records)

    def abandon(self, context: ArtifactTransactionCloseContext) -> None:
        records, present, _, corrupted = context.verify_artifacts(self.refs)
        for ref in present:
            warnings.warn(f"{ref} was not deleted and will remain stored.")
        for ref in corrupted:
            warnings.warn(f"{ref} was partially unstored and will be fully unstored.")
        context.datastore.unstore(corrupted.values())
        context.insert_datastore_records(records)
