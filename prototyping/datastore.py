from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from contextlib import AbstractContextManager
from typing import Any

from lsst.daf.butler import DatasetRef, DatastoreConfig, FileDataset
from lsst.resources import ResourcePath

from .aliases import GetParameter, InMemoryDataset
from .primitives import OpaqueTableBatch, OpaqueTableDefinition, OpaqueTableKeyBatch, OpaqueTableName


class Datastore(ABC):
    """Abstract interface for storage backends.

    The new Datastore interface has no methods in common with the current one,
    but plenty that are extremely similar in spirit.  Overall this interface is
    designed to be precise and concise to make it easier to write Datastore
    implementations - convenience logic like accepting multiple similar kinds
    of arguments has been moved up into butler to avoid requiring each
    Datastore to reimplement it.

    I've tried to sketch this out with `ChainedDatastore` very much in mind,
    but actually trying to prototype that implementation out while delegating
    to the same interface on child Datastores is something we absolutely do
    before diving all the way into the implementation.  We should probably also
    try to work out in more detail how a FileDatastore with signed URIs would
    implement these.

    A method for existence checks needs to be added, but I keep going in
    circles trying to precisely define the behavior of the `LimitedButler` and
    `Butler` methods that would delegate to it, and I don't want that to hold
    up getting the rest of the prototype out for review sooner.
    """

    config: DatastoreConfig

    @property
    @abstractmethod
    def opaque_table_definitions(self) -> Mapping[OpaqueTableName, OpaqueTableDefinition]:
        """The definitions of the opaque tables used by this Datastore."""
        raise NotImplementedError()

    @abstractmethod
    def get_many(
        self,
        arg: Iterable[tuple[DatasetRef, Mapping[GetParameter, Any]]],
        /,
    ) -> Iterable[tuple[DatasetRef, Mapping[GetParameter, Any], InMemoryDataset]]:
        """Load datasets into memory.

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
    def get_many_uris(self, refs: Iterable[DatasetRef]) -> Iterable[tuple[DatasetRef, ResourcePath]]:
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
    def put_many(self, arg: Iterable[tuple[InMemoryDataset, DatasetRef]], /) -> OpaqueTableBatch:
        """Insert new datasets from in-memory objects, assuming some kind of
        external datastore transaction (such as QuantumGraph execution) is
        already under way.

        Full `Butler` should use `put_many_transaction` instead.
        """
        raise NotImplementedError()

    @abstractmethod
    def predict_put_many(self, refs: Iterable[DatasetRef]) -> OpaqueTableBatch:
        """Generate the opaque table records that would be returned by
        `put_many` or `put_many_transaction` if called with the given
        `DatasetRef` iterable, without actually writing any datasets to
        storage.

        This will have to omit "validation" columns like sizes and checksums
        that might be included by a real ``put`` operation; this is fine as
        as the omitted columns are not needed for existence checks or
        ``get_many``.
        """
        raise NotImplementedError()

    @abstractmethod
    def put_many_transaction(
        self,
        arg: Iterable[tuple[InMemoryDataset, DatasetRef]],
        /,
    ) -> AbstractContextManager[OpaqueTableBatch]:
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

        When the context exits with an exception, an attempt may be made to
        delete the recently-added files, and the journal file may be removed if
        this is successful.
        """
        raise NotImplementedError()

    @abstractmethod
    def unstore(self, refs: Iterable[DatasetRef]) -> OpaqueTableKeyBatch:
        """Remove datasets, assuming some kind of datastore transaction (such
        as a QuantumGraph exection) is already underway.

        Full `Butler` should use `unstore_transaction` instead.
        """
        raise NotImplementedError()

    @abstractmethod
    def unstore_transaction(self, refs: Iterable[DatasetRef]) -> AbstractContextManager[OpaqueTableKeyBatch]:
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

        When the context exits with an exception before any deletions occur,
        the journal file should be removed.
        """
        raise NotImplementedError()

    @abstractmethod
    def receive(
        self,
        file_datasets: Mapping[ResourcePath | None, list[FileDataset]],
        *,
        transfer: str | None = "auto",
        own_absolute: bool = False,
        record_validation_info: bool = True,
        opaque_data: tuple[DatastoreConfig, OpaqueTableBatch] | None = None,
    ) -> AbstractContextManager[OpaqueTableBatch]:
        """Add external files to this datastore.

        Parameters
        ----------
        file_datasets
            Files to add to the datastore.  Keys are URI roots for any files
            that have relative URIs nested under that key (some nested URIs may
            be absolute).  If a key matches this Datastore's own root, a
            transfer should never be performed and usable records will
            generally be available in ``opaque_data``; this represents a
            receipt of registry content only (e.g. after QuantumGraph execution
            completes).  `None` may be used as a key if all nested files have
            absolute URIs; these files should be treated the same as absolute
            URIs under a root URI key.
        transfer, optional
            Transfer mode recognized by `ResourcePath`.  If `None`, the
            datastore should not assume ownership of the files unless a
            ``file_datasets`` key matches the datastore's root URI.  If not
            `None`, files with absolute URIs should still be ingested with no
            transfer and no assumption of ownership.
        record_validation_info, optional
            Whether to record file sizes, checksums, etc.
        opaque_data, optional
            Opaque table records for these datasets and the Datastore
            configuration they originated from.  If this configuration is
            compatible in ``self``, these records may be used as-is instead of
            creating new ones.  These records may or may not have validation
            info present, and if they do, that information may not remain valid
            if transfers occur.

        Returns
        -------
        context
            A context manager that returns opaque table records for this
            datastore, superseding any that may have been passed in with
            ``opaque_data``.  Entering this context manager causes a journal
            file to be written to manage the transaction and all files to be
            transferred.  When the context exits successfully the journal file
            is removed.  When the context exits with an exception, the context
            manager may attempt to remove files added since the transaction
            started, and it may remove the journal file if it is certain that
            this was successful.
        """
        raise NotImplementedError()

    @abstractmethod
    def export(
        self,
        refs: Iterable[DatasetRef],
        *,
        transfer: str | None = "auto",
        directory: ResourcePath | None = None,
        return_records: bool = True,
    ) -> tuple[OpaqueTableBatch | None, dict[Mapping[ResourcePath | None, list[FileDataset]]]]:
        """Export datasets and Datastore metadata.

        Parameters
        ----------
        refs
            Fully expanded `DatasetRef` objects to export.  All relevant
            Datastore opaque table records will already be attached.
        transfer, optional
            Transfer mode recognized by `ResourcePath`.  If `None`,
            ``directory`` is ignored, all datasets are left where they are and
            Datastore root URIs should be used as the keys in the mapping of
            `FileDataset` instances returned.  If not `None`, either all files
            must have absolute URIs or ``directory`` must not be `None`, and
            ``directory`` should be the only key in the `FileDataset` mapping
            returned.
        directory, optional
            Root URI for exported datasets after the transfer.  Ignored if
            ``transfer`` is `None`.  Must be provided if ``transfer`` is not
            `None` unless all exported files have absolute URIs within the
            Datastore already.
        return_records, optional
            If `True`, return the Datastore's internal records for use by a
            compatible receiving Datastore.  The `FileDataset` mapping must
            still be returned as well; it is up to the receiving Datastore to
            determine whether to use the records or fall back to `FileDataset`
            ingest.

        Returns
        -------
        opaque_table_rows
            Batch of opaque table rows that represent these datasets, or `None`
            if ``return_records`` is `False` or the datastore does not have
            records (though this should only be true of datastores with
            ephemeral storage).
        files
            Mapping from root URI to a list of `FileDataset` instances that are
            either relative to that root or absolute.  If ``transfer`` is not
            `None`, ``directory`` will be the only key.
        """
        raise NotImplementedError()
