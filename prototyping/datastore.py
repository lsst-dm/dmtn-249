from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from contextlib import AbstractContextManager
from typing import Any

from lsst.daf.butler import DatasetRef, DatastoreConfig, FileDataset
from lsst.resources import ResourcePath

from .aliases import GetParameter, InMemoryDataset, OpaqueTableName, OpaqueTableRow
from .raw_batch import OpaqueTableInsertionBatch


class Datastore(ABC):
    """Abstract interface for storage backends.

    This class is fully abstract, but it's easy to imagine an intermediate base
    class that implements the *_transaction methods by delegating to their
    non-transaction variants while taking care of the journal-file management
    for further-derived classes.
    """

    config: DatastoreConfig

    @abstractmethod
    def get_many(
        self,
        arg: Iterable[tuple[DatasetRef, Mapping[GetParameter, Any]]],
        /,
    ) -> Iterable[tuple[DatasetRef, Mapping[GetParameter, Any], InMemoryDataset]]:
        """Load datasets.

        Incoming `DatasetRef` objects will have already been fully expanded to
        include both expanded data IDs and all possibly-relevant opaque table
        records.

        Notes
        -----
        The datasets are not necessarily returned in the order they are passed
        in, to better permit async implementations with lazy first-received
        iterator returns.  Implementations that can guarantee consistent
        ordering might want to explicitly avoid it, to avoid allowing callers
        to grow dependent on that behavior instead of checking the return
        ``DatasetRef`` objects themselves.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_many_uri(self, refs: Iterable[DatasetRef]) -> Iterable[tuple[DatasetRef, ResourcePath]]:
        """Return URIs for the given datasets.

        Incoming `DatasetRef` objects will have already been fully expanded to
        include both expanded data IDs and all possibly-relevant opaque table
        records.

        Notes
        -----

        If a dataset is represented by multiple URIs they should all be
        returned, with component `DatasetRef` objects if appropriate.  If a
        dataset has no URI (e.g. because it is in a Datastore that does not
        use them), it should not be returned.

        I'm not sure if this method should be returning signed URLs or not when
        that's what the Datastore uses.  Returning a URL that isn't signed and
        hence can't be used at all doesn't seem useful, but we probably need
        the user to say what they're going to do with it (read, write, delete)
        in order to sign it.
        """
        raise NotImplementedError()

    @abstractmethod
    def put_many(
        self,
        arg: Iterable[tuple[InMemoryDataset, DatasetRef]],
        /,
    ) -> AbstractContextManager[Iterable[tuple[OpaqueTableName, Iterable[OpaqueTableRow]]]]:
        """Insert new datasets from in-memory objects, assuming some kind of
        datastore transaction (such as a QuantumGraph execution) is already
        under way.

        Full `Butler` should use `put_many_transaction` instead.
        """
        raise NotImplementedError()

    @abstractmethod
    def unstore(
        self, refs: Iterable[DatasetRef]
    ) -> AbstractContextManager[Iterable[tuple[OpaqueTableName, Iterable[uuid.UUID]]]]:
        """Remove datasets, assuming some kind of datastore transaction (such
        as a QuantumGraph exection) is already underway.

        Full `Butler` should use `unstore_transaction` instead.
        """
        raise NotImplementedError()

    @abstractmethod
    def put_many_transaction(
        self,
        arg: Iterable[tuple[InMemoryDataset, DatasetRef]],
        /,
    ) -> AbstractContextManager[Iterable[tuple[OpaqueTableName, Iterable[OpaqueTableRow]]]]:
        """Insert new datasets for in-memory objects within a journal-file
        transaction.

        Notes
        -----
        Simply calling this method performs no write operations of any kind.
        Implementations *may* check or otherwise process inputs at this time,
        including contacting a server to obtain signed URLs.

        Entering the context manager first writes a journal file or otherwise
        persists the list of datasets (at least UUIDs, generally a URI or other
        storage-specific identifier as well) to a location that indicates that
        a fallable write operation is underway involving these datasets.  It
        then actually performs all writes, and finally returns iterables of
        opaque table rows that must be inserted into the Registry before the
        context exits.

        When the context manager exists without an error, the journal file (or
        equivalent) is deleted.
        """
        raise NotImplementedError()

    @abstractmethod
    def unstore_transaction(
        self, refs: Iterable[DatasetRef]
    ) -> AbstractContextManager[Iterable[tuple[OpaqueTableName, Iterable[uuid.UUID]]]]:
        """Remove datasets within a journal-file transaction.

        Notes
        -----
        Simply calling this method performs no write operations of any kind.
        Implementations *may* check or otherwise process inputs at this time,
        including contacting a server to obtain signed URLs.

        Entering the context manager writes a journal file or otherwise
        persists the list of datasets (at least UUIDs, generally a URI or other
        storage-specific identifier as well) to a location that indicates that
        a fallable deletion operation is underway involving these datasets.  It
        also returns UUIDs that should be used to delete rows from opaque
        tables by Registry, which must occur before the context exits.

        When the context manager exit without an error, actual deletions are
        executed and the journal file (or equivalent) is removed only after all
        deletions have succeeded.
        """
        raise NotImplementedError()

    @abstractmethod
    def import_(
        self,
        file_datasets: Iterable[FileDataset],
        opaque_table_insertions: OpaqueTableInsertionBatch,
        *,
        mode: str | None = "auto",
        own_absolute: bool = False,
        record_validation_info: bool = True,
        directory: ResourcePath | None = None,
        config: DatastoreConfig | None = None,
    ) -> AbstractContextManager[None]:
        """Add external files to this datastore.

        Parameters
        ----------
        file_datasets
            Files to add to the datastore.  Relative URIs should be interpreted
            as relative to ``directory`` and should still be relocated via
            ``mode``.
        opaque_table_insertions
            Object that should be modified to include the appropriate opaque
            table records for this datastore.  If ``config`` is not `None`,
            this will start with records generated by a Datastore with that
            configuration that may be left (partially or wholly) as-is if that
            configuration is compatible with ``self``.
        mode, optional
            Transfer mode recognized by `ResourcePath`
        own_absolute, optional
            If `True`, transfer even absolute URIs outside the Datastore root
            into the Datastore's location and assume ownership of them.  If
            `False`, these URIs are ingested as external files with absolute
            URIs that are known to the Datastore but never deleted by it.
        record_validation_info, optional
            Whether to record file sizes, checksums, etc.
        directory, optional
            Root URI for relative URIs in ``file_datasets``.  If not provided,
            all URIs in ``file_datasets`` must be absolute.
        config, optional
            Datastore configuration responsible for the initial state of
            ``opaque_table_insertions``.  If `None`,
            ``opaque_table_insertions`` is guaranteed to start empty.  If not
            `None`, existing rows in ``opaque_table_insertions`` must be
            removed if they are not compatible with this Datastore.
        """
        raise NotImplementedError()

    @abstractmethod
    def export(
        self,
        refs: Iterable[DatasetRef],
        *,
        mode: str | None = "auto",
        directory: ResourcePath | None = None,
        return_records: bool = True,
    ) -> tuple[OpaqueTableInsertionBatch | None, list[FileDataset]]:
        raise NotImplementedError()
