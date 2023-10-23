from __future__ import annotations

__all__ = (
    "ArtifactTransaction",
    "ArtifactTransactionName",
    "ArtifactTransactionOpenContext",
    "ArtifactTransactionRevertContext",
    "ArtifactTransactionCloseContext",
    "ArtifactTransactionCommitContext",
)

from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping, Sequence, Set
from typing import Any, TypeAlias

import pydantic
from lsst.daf.butler import CollectionType, DatasetId, StoredDatastoreItemInfo, Timespan

from .aliases import CollectionDocumentation, CollectionName, DatastoreTableName
from .datastore import Datastore
from .primitives import DatasetRef

ArtifactTransactionName: TypeAlias = str
"""Alias for the name a transaction is registered with in the repository
database.
"""


class ArtifactTransaction(pydantic.BaseModel, ABC):
    """An abstraction for operations that modify both butler database and
    datastore state.
    """

    @property
    def is_workspace(self) -> bool:
        """Whether this transaction is associated with a workspace that allows
        content to be added incrementally.
        """
        return False

    def make_workspace_client(self, datastore: Datastore) -> Any:
        return None

    @abstractmethod
    def get_operation_name(self) -> str:
        """Return a human-readable name for the type of transaction.

        This is used to form the user-visible transaction name when no name is
        provided.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_insert_only_runs(self) -> Set[CollectionName]:
        """Return the `~CollectionType.RUN` collections that this transaction
        inserts new datasets into only.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_modified_runs(self) -> Set[CollectionName]:
        """Return the `~CollectionType.RUN` collections that this transaction
        modifies in any way other than insertin new datasets.

        This includes datasets added to the database via
        `ArtifactTransactionOpenContext.ensure_datasets`.
        """
        raise NotImplementedError()

    @abstractmethod
    def begin(self, context: ArtifactTransactionOpenContext) -> None:
        """Open the transaction.

        This method is called within a database transaction with at least
        ``READ COMMITTED`` isolation.  Exceptions raised by this method will
        cause that database transaction to be rolled back, and exceptions that
        originate in a database constraint failure must be re-raised or allowed
        to propagate; these indicate that a rollback is already in progress.

        After this method exits but before the database transaction is
        committed, the database rows representing this transaction will be
        inserted.
        """
        raise NotImplementedError()

    @abstractmethod
    def commit(self, context: ArtifactTransactionCommitContext) -> None:
        """Commit this transaction.

        This method is called within a database transaction with
        ``SERIALIZABLE`` isolation.  Exceptions raised by this method will
        cause that database transaction to be rolled back, and exceptions that
        originate in a database constraint failure must be re-raised or allowed
        to propagate; these indicate that a rollback is already in progress.

        After this method exits but before the database transaction is
        committed, the database rows representing this transaction will be
        deleted.
        """
        raise NotImplementedError()

    @abstractmethod
    def revert(self, context: ArtifactTransactionRevertContext) -> None:
        """Revert this transaction.

        This method is called within a database transaction with
        ``READ COMMITTED`` isolation.  Exceptions raised by this method will
        cause that database transaction to be rolled back, and exceptions that
        originate in a database constraint failure must be re-raised or allowed
        to propagate; these indicate that a rollback is already in progress.

        After this method exits but before the database transaction is
        committed, the database rows representing this transaction will be
        deleted.
        """
        raise NotImplementedError()

    @abstractmethod
    def abandon(self, context: ArtifactTransactionCloseContext) -> None:
        """Abandon this transaction.

        This method is called within a database transaction with at least
        ``READ COMMITTED`` isolation.  Exceptions raised by this method will
        cause that database transaction to be rolled back, and exceptions that
        originate in a database constraint failure must be re-raised or allowed
        to propagate; these indicate that a rollback is already in progress.

        After this method exits but before the database transaction is
        committed, the database rows representing this transaction will be
        deleted.
        """
        raise NotImplementedError()


class ArtifactTransactionOpenContext(ABC):
    """The object passed to `ArtifactTransaction.begin` to allow it to perform
    limited database operations.
    """

    @abstractmethod
    def insert_new_run(self, name: CollectionName, doc: CollectionDocumentation | None = None) -> None:
        """Insert the given `~CollectionType.RUN` collection and raise if it
        already exists.
        """
        raise NotImplementedError()

    @abstractmethod
    def insert_new_datasets(self, refs: Iterable[DatasetRef]) -> None:
        """Insert new datasets, raising if any already exist."""
        raise NotImplementedError()

    def discard_datastore_records(self, dataset_ids: Iterable[DatasetId]) -> None:
        """Discard any datastore records associated with the given datasets,
        ignoring any datasets that do not exist or already do not have
        datastore records.
        """
        raise NotImplementedError()


class ArtifactTransactionCloseContext(ABC):
    """The object passed to `ArtifactTransaction.abandon` to allow it to modify
    the repository.
    """

    @property
    @abstractmethod
    def datastore(self) -> Datastore:
        """The datastore to use for all artifact operations.

        Typically only `Datastore.verify` and `Datastore.unstore` should need
        to be called when a transaction is closed.
        """
        raise NotImplementedError()

    @abstractmethod
    def insert_datastore_records(
        self, records: Mapping[DatastoreTableName, Iterable[StoredDatastoreItemInfo]]
    ) -> None:
        """Insert records into a database table, marking a dataset as being
        stored.
        """
        raise NotImplementedError()

    def verify_artifacts(
        self, refs: Mapping[DatasetId, DatasetRef]
    ) -> tuple[
        dict[DatastoreTableName, list[StoredDatastoreItemInfo]],
        dict[DatasetId, DatasetRef],
        dict[DatasetId, DatasetRef],
        dict[DatasetId, DatasetRef],
    ]:
        """Verify dataset refs using a datastore and classify them.

        This is a convenience method typically called by at least two of
        {`ArtifactTransaction.commit`, `ArtifactTransaction.revert`,
        `ArtifactTransaction.abandon`}.

        Parameters
        ----------
        refs : `~collections.abc.Mapping` [ `~lsst.daf.butler.DatasetId`, \
                `DatasetRef` ]
            Datasets to verify.

        Returns
        -------
        records
            Mapping from datastore table name to the records for that table,
            for all datasets in ``present``.
        present
            Datasets whose artifacts were verified successfully.
        missing
            Datasets whose artifacts were fully absent.
        corrupted
            Datasets whose artifacts were incomplete or invalid.
        """
        records: dict[DatastoreTableName, list[StoredDatastoreItemInfo]] = {
            table_name: [] for table_name in self.datastore.tables.keys()
        }
        present = {}
        missing = {}
        corrupted = {}
        for dataset_id, valid, records_for_dataset in self.datastore.verify(refs.values()):
            ref = refs[dataset_id]
            if valid:
                for table_name, records_for_table in records_for_dataset.items():
                    records[table_name].extend(records_for_table)
                present[ref.id] = ref
            elif valid is None:
                missing[ref.id] = ref
            else:
                corrupted[ref.id] = ref
        return records, present, missing, corrupted


class ArtifactTransactionRevertContext(ArtifactTransactionCloseContext):
    """The object passed to `ArtifactTransaction.revert` to allow it to modify
    the repository.
    """

    @abstractmethod
    def discard_collection(self, name: CollectionName, *, force: bool = False) -> None:
        """Delete any existing collection with the given name.

        If a collection with this name does not exist, do nothing.

        If ``force=False``, the collection must already have no associated
        datasets, parent collections, or child collections.
        If ``force=True``,

        - all datasets in `~CollectionType.RUN` collections are discarded;
        - all dataset-collection associations in `~CollectionType.TAGGED` or
          `~CollectionType.CALIBRATION` collections are discarded (the datasets
          themselves are not discarded);
        - parent and child collection associations are discarded (the parent
          and child collections themselves are not discarded).
        """
        raise NotImplementedError()

    @abstractmethod
    def discard_datasets(self, dataset_ids: Iterable[DatasetId]) -> None:
        """Discard datasets.

        Datasets that do not exist are ignored.

        Datasets must not have any associated datastore records or associations
        with `~CollectionType.TAGGED` or `~CollectionType.CALIBRATION`
        collections.
        """
        raise NotImplementedError()


class ArtifactTransactionCommitContext(ArtifactTransactionRevertContext):
    """The object passed to `ArtifactTransaction.commit` to allow it to modify
    the repository.
    """

    @abstractmethod
    def ensure_collection(
        self, name: CollectionName, type: CollectionType, doc: CollectionDocumentation | None = None
    ) -> None:
        """Insert the given collection if it does not exist.

        If the collection exists with a different type, raise
        `CollectionTypeError`.  Documentation is only used when inserting the
        new collection; documentation conflicts are not checked.
        """
        raise NotImplementedError()

    @abstractmethod
    def set_collection_chain(
        self, parent: CollectionName, children: Sequence[CollectionName], *, flatten: bool = False
    ) -> None:
        """Set the childen of a `~CollectionType.CHAINED` collection."""
        raise NotImplementedError()

    @abstractmethod
    def get_collection_chain(self, parent: CollectionName) -> list[CollectionName]:
        """Get the childen of a `~CollectionType.CHAINED` collection."""
        raise NotImplementedError()

    @abstractmethod
    def ensure_associations(
        self,
        collection: CollectionName,
        dataset_ids: Iterable[DatasetId],
        *,
        replace: bool = False,
        timespan: Timespan | None = None,
    ) -> None:
        """Associate datasets with a `~CollectionType.TAGGED` or
        `~CollectionType.CALIBRATION` collection.
        """
        raise NotImplementedError()

    @abstractmethod
    def discard_associations(
        self,
        collection: CollectionName,
        dataset_ids: Iterable[DatasetId],
        *,
        replace: bool = False,
        timespan: Timespan | None = None,
    ) -> None:
        """Discard associations between datasets and a `~CollectionType.TAGGED`
        or `~CollectionType.CALIBRATION` collection.

        Associations that do not exist are ignored.
        """
        raise NotImplementedError()
