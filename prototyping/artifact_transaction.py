from __future__ import annotations

__all__ = ("ArtifactTransaction", "ArtifactTransactionHeader", "ArtifactTransactionName")

from abc import ABC, abstractmethod
from collections.abc import Mapping, Set
from typing import Any, Self, final, TypeAlias

import pydantic

from lsst.daf.butler import DatasetId, StoredDatastoreItemInfo
from lsst.resources import ResourcePath
from lsst.utils.doImport import doImportType
from lsst.utils.introspection import get_full_type_name

from .aliases import CollectionName, DatastoreTableName, StorageURI
from .datastore import Datastore
from .raw_batch import RawBatch
from .primitives import DatasetRef


ArtifactTransactionName: TypeAlias = str
"""Alias for the name a transaction is registered with in the repository
database.
"""


class ArtifactTransaction(ABC):
    """An abstraction for operations that modify both butler database and
    datastore state.

    `ArtifactTransaction` instances are registered with the database and saved
    to either the database or a `ResourcePath` JSON location by calling
    `Butler.begin_transaction`, and persist until committed or abandoned.
    After a transaction has begun it should only be manipulated by the
    `Butler.commit` and `Butler.abandon` methods.
    """

    @classmethod
    @abstractmethod
    def from_header_data(
        cls, header_data: Any, workspace_root: ResourcePath | None, datastore: Datastore
    ) -> Self:
        """Reconstruct this transaction from its header data and an optional
        workspace root.

        Parameters
        ----------
        header_data
            JSON-compatible data previous obtained by calling `get_header_data`
            on another instance.
        workspace_root : `lsst.resources.ResourcePath` or `None`
            Root directory for additional files the transaction can use to
            store additional data beyond the header.  Files in this directory
            may be read immediately or only when other methods are called. If
            `None`, the butler implementation does not support workspaces and
            transactions that require them are not supported.
        datastore : `Datastore`
            Datastore being written to.  Expected to be used only to
            deserialize objects (e.g. transfer manifests or datastore records).

        Returns
        -------
        transaction
            Reconstructed transaction instance.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_header_data(self, datastore: Datastore) -> Any:
        """Extract JSON-compatible data (e.g. `dict` of built-in types) that
        can be used to reconstruct this transaction via `from_header_data`.

        Parameters
        ----------
        datastore : `Datastore`
            Datastore being written to.  Expected to be used only to serialize
            objects (e.g. transfer manifests or datastore records).
        """
        raise NotImplementedError()

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
    def get_runs(self) -> Set[CollectionName]:
        """Return the `~CollectionType.RUN` collections whose dataset artifacts
        may be modified by this transaction.
        """
        raise NotImplementedError()

    def get_initial_batch(self) -> RawBatch:
        """Return a batch of registry operations to perform when beginning the
        transaction.
        """
        raise NotImplementedError()

    def get_unstores(self) -> Set[DatasetId]:
        """Return the IDs of datasets whose artifacts may be removed by this
        transaction.

        Since datastore record presence implies artifact existence, datastore
        records need to be removed at the start of an artifact transaction and
        re-inserted only if the transaction is *successfully* abandoned (which
        may not always be possible, e.g. if artifacts have already been
        irreversibly deleted).

        TODO: Just returning a set of IDs here is not sufficient if we want
        to be able to safely delete artifacts that hold multiple datasets
        (e.g. DECam raws); only the datastore can know how to do the self-join
        (e.g. on URI) that identifies those artifacts and tests whether all
        datasets are being deleted or not.  We need to come up with a way to
        abstract and probably serialize that information.
        """
        return frozenset()

    @abstractmethod
    def get_uris(self, datastore: Datastore) -> list[StorageURI]:
        """Return possibly-relative URIs that need to be turned into
        possibly-signed and definitely absolute URLs to pass to commit and
        abandon methods.
        """
        raise NotImplementedError()

    @abstractmethod
    def commit_phase_one(
        self,
        datastore: Datastore,
        paths: Mapping[StorageURI, ResourcePath],
    ) -> None:
        """Begin to commit this transaction.

        This method will always be called by the client.

        Parameters
        ----------
        datastore : `Datastore`
            Datastore client for this data repository.
        paths : `~collections.abc.Mapping` [ `str`, \
                `~lsst.resources.ResourcePath` ]
            Mapping from unsigned possibly-relative URI to absolute
            possibly-signed URL.  Keys are the same as those returned by
            `get_uris` for the resolution.
        """
        raise NotImplementedError()

    @abstractmethod
    def commit_phase_two(
        self,
        datastore: Datastore,
    ) -> tuple[RawBatch, dict[DatastoreTableName, list[StoredDatastoreItemInfo]]]:
        """Finish committing this transaction.

        This method will be called on the server in `RemoteButler`, and will
        only be called after `commit_phase_one` has been called.

        Parameters
        ----------
        datastore : `Datastore`
            Datastore client for this data repository.

        Returns
        -------
        final_batch
            Batch of database-only modifications to execute when the
            transaction is closed in the database.
        records
            Datastore records to insert into the database.
        """
        raise NotImplementedError()

    @abstractmethod
    def revert_phase_one(
        self,
        datastore: Datastore,
        paths: Mapping[StorageURI, ResourcePath],
    ) -> None:
        """Begin to revert this transaction.

        This method will always be called by the client.

        Parameters
        ----------
        datastore : `Datastore`
            Datastore client for this data repository.
        paths : `~collections.abc.Mapping` [ `str`, \
                `~lsst.resources.ResourcePath` ]
            Mapping from unsigned possibly-relative URI to absolute
            possibly-signed URL.  Keys are the same as those returned by
            `get_uris` for the resolution.
        """
        raise NotImplementedError()

    @abstractmethod
    def revert_phase_two(
        self,
        datastore: Datastore,
    ) -> tuple[RawBatch, dict[DatastoreTableName, list[StoredDatastoreItemInfo]]]:
        """Finish reverting this transaction.

        This method will be called on the server in `RemoteButler`, and will
        only be called after `commit_phase_one` has been called.

        Parameters
        ----------
        datastore : `Datastore`
            Datastore client for this data repository.

        Returns
        -------
        final_batch
            Batch of database-only modifications to execute when the
            transaction is closed in the database.
        records
            Datastore records to insert into the database.
        """
        raise NotImplementedError()

    @abstractmethod
    def abandon_phase_one(
        self,
        datastore: Datastore,
        paths: Mapping[StorageURI, ResourcePath],
    ) -> None:
        """Begin to abandon this transaction.

        This method will always be called by the client.

        Parameters
        ----------
        datastore : `Datastore`
            Datastore client for this data repository.
        paths : `~collections.abc.Mapping` [ `str`, \
                `~lsst.resources.ResourcePath` ]
            Mapping from unsigned possibly-relative URI to absolute
            possibly-signed URL.  Keys are the same as those returned by
            `get_uris` for the resolution.
        """
        raise NotImplementedError()

    @abstractmethod
    def abandon_phase_two(
        self,
        datastore: Datastore,
    ) -> tuple[RawBatch, dict[DatastoreTableName, list[StoredDatastoreItemInfo]]]:
        """Finish abandoning this transaction.

        This method will be called on the server in `RemoteButler`, and will
        only be called after `commit_phase_one` has been called.

        Parameters
        ----------
        datastore : `Datastore`
            Datastore client for this data repository.

        Returns
        -------
        final_batch
            Batch of database-only modifications to execute when the
            transaction is closed in the database.
        records
            Datastore records to insert into the database.
        """
        raise NotImplementedError()

    def verify_artifacts(
        self,
        datastore: Datastore,
        refs: Mapping[DatasetId, DatasetRef],
    ) -> tuple[
        dict[DatastoreTableName, list[StoredDatastoreItemInfo]],
        list[DatasetRef],
        list[DatasetRef],
        list[DatasetRef],
    ]:
        """Verify dataset refs using a datastore and classify them.

        This is a convenience method typically called by at least two of a
        transaction's (`commit_phase_two`, `revert_phase_two`,
        `abandon_phase_two`).

        Parameters
        ----------
        datastore : `Datastore`
            Datastore to call `Datastore.verify` on.
        refs : `~collections.abc.Mapping` [ `~lsst.daf.butler.DatasetId`, \
                `DatasetRef` ]
            Datasets to verify.

        Returns
        -------
        records
            Mapping from datastore table name to the records for that table,
            for all datasets in ``present``.
        present
            List of datasets whose artifacts were verified successfully.
        missing
            List of datasets whose artifacts were fully absent.
        corrupted
            List of datasets whose artifacts were incomplete or invalid.
        """
        records: dict[DatastoreTableName, list[StoredDatastoreItemInfo]] = {
            table_name: [] for table_name in datastore.tables.keys()
        }
        present = []
        missing = []
        corrupted = []
        for dataset_id, valid, records_for_dataset in datastore.verify(refs.values()):
            ref = refs[dataset_id]
            if valid:
                for table_name, records_for_table in records_for_dataset.items():
                    records[table_name].extend(records_for_table)
                present.append(ref)
            elif valid is None:
                missing.append(ref)
            else:
                corrupted.append(ref)
        return records, present, missing, corrupted


@final
class ArtifactTransactionHeader(pydantic.BaseModel):
    """A pydantic model that allows a transaction to be saved as JSON.

    This is called a "header" because some transactions (especially long-lived
    ones) may store additional data in other files.
    """

    name: ArtifactTransactionName
    """Name registered in the database for the transaction."""

    type_name: str
    """Fully-qualified type name of the `ArtifactTransaction` subclass."""

    data: Any
    """JSON-compatible data returned by `ArtifactTransaction.get_header_data`
    and passed to `ArtifactTransaction.from_header_data.
    """

    def make_transaction(
        self, workspace_root: ResourcePath | None, datastore: Datastore
    ) -> ArtifactTransaction:
        """Deserialize a `ArtifactTransaction` instance from this header.

        Parameters
        ----------
        workspace_root : `lsst.resources.ResourcePath` or `None`
            Root directory for additional files the transaction can use to
            store additional data beyond the header.  Files in this directory
            may be read immediately or only when other methods are called. If
            `None`, the butler implementation does not support workspaces and
            transactions that require them are not supported.
        datastore : `Datastore`
            Datastore being written to.  Expected to be used to serialize
            objects or interpret records only here.

        Returns
        -------
        transaction : `ArtifactTransaction`
            Deserialized transaction instance.
        """
        transaction_type: type[ArtifactTransaction] = doImportType(self.type_name)
        return transaction_type.from_header_data(self.data, None, datastore)

    @classmethod
    def from_transaction(
        cls, transaction: ArtifactTransaction, name: ArtifactTransactionName, datastore: Datastore
    ) -> Self:
        """Serialize a transaction to this model.

        Parameters
        ----------
        transaction : `ArtifactTransaction`
            Transaction object to serialize.
        name : `str`
            Name registered in the database for the transaction.
        datastore : `Datastore`
            Datastore being written to.  Expected to be used to deserialize
            objects or interpret records only here.

        Returns
        -------
        header : `ArtifactTransactionHeader`
            Header for the serialized transaction.
        """
        return cls(
            name=name, type_name=get_full_type_name(transaction), data=transaction.get_header_data(datastore)
        )
