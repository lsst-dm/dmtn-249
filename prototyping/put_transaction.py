from __future__ import annotations

__all__ = ("PutTransaction",)

import uuid
import warnings
from collections.abc import Set

from .aliases import CollectionName
from .artifact_transaction import (
    ArtifactTransaction,
    ArtifactTransactionCloseContext,
    ArtifactTransactionRevertContext,
    ArtifactTransactionCommitContext,
    ArtifactTransactionOpenContext,
)
from .primitives import DatasetRef


class PutTransaction(ArtifactTransaction):
    """An artifact transaction implementation that writes several in-memory
    datasets to the repository.
    """

    refs: dict[uuid.UUID, DatasetRef]

    def get_operation_name(self) -> str:
        return "put"

    def get_insert_only_runs(self) -> Set[CollectionName]:
        return frozenset(ref.run for ref in self.refs.values())

    def get_modified_runs(self) -> Set[CollectionName]:
        return frozenset()

    def begin(self, context: ArtifactTransactionOpenContext) -> None:
        context.insert_new_datasets(self.refs.values())

    def commit(self, context: ArtifactTransactionCommitContext) -> None:
        records, _, missing, corrupted = context.verify_artifacts(self.refs)
        if missing or corrupted:
            raise RuntimeError(
                f"{len(missing)} dataset(s) were not written and {len(corrupted)} had missing or "
                f"invalid artifacts; transaction must be reverted or abandoned."
            )
        context.insert_datastore_records(records)

    def revert(self, context: ArtifactTransactionRevertContext) -> None:
        context.datastore.unstore(self.refs.values())
        context.discard_datasets(self.refs.keys())

    def abandon(self, context: ArtifactTransactionCloseContext) -> None:
        records, _, missing, corrupted = context.verify_artifacts(self.refs)
        for ref in missing:
            warnings.warn(f"{ref} was not written and will be unstored.")
        for ref in corrupted:
            warnings.warn(f"{ref} was not fully written and will be unstored.")
        context.insert_datastore_records(records)
