from __future__ import annotations

from abc import abstractmethod
from collections.abc import Iterable

from lsst.daf.butler import DatasetIdFactory, DimensionUniverse, StorageClassFactory
from lsst.daf.butler.registry import RegistryDefaults
from lsst.daf.butler.registry.wildcards import CollectionWildcard, DatasetTypeWildcard

from .bridge import DatastoreBridge
from .primitives import DatasetRef
from .queries import CollectionQuery, DatasetTypeQuery, Query
from .raw_batch import RawBatch


class Registry:
    """An ABC for classes that provide organization and metadata storage
    implementations for a butler.

    This is envisioned to have two major implementations: one with direct
    access to a SQL database via SQLAlchemy, and one that talks to a server
    over http.
    """

    dimensions: DimensionUniverse
    defaults: RegistryDefaults
    is_writeable: bool
    storage_class_factory: StorageClassFactory
    dataset_id_factory: DatasetIdFactory
    datastore_bridge: DatastoreBridge

    @abstractmethod
    def query(self, defer: bool = True) -> Query:
        """Return a Query object for this data repository."""
        raise NotImplementedError()

    @abstractmethod
    def query_collections(self, pattern: CollectionWildcard) -> CollectionQuery:
        """Return all collections matching the given pattern.

        Additional filtering for `Butler.query_collections` will be implemented
        in that method; it should be no less efficient (and probably more
        efficient) to just fetch all collections and cache them on the client,
        or maybe in the future fetch all official shared  collections and all
        "u/<user>" collections and cache those.
        """
        raise NotImplementedError()

    @abstractmethod
    def query_dataset_types(self, pattern: DatasetTypeWildcard) -> DatasetTypeQuery:
        """Return all dataset types matching the given pattern.

        Same additional filtering and rationale as for `query_collections`.
        While we don't have a way to distinguish user dataset types from shared
        ones, we'll need to someday.
        """
        raise NotImplementedError()

    @abstractmethod
    def apply_batch(self, batch: RawBatch) -> None:
        """Apply a suite of batched operations in a single transaction."""
        raise NotImplementedError()

    @abstractmethod
    def expand_new_dataset_refs(
        self,
        refs: Iterable[DatasetRef],
        sign: bool,
    ) -> Iterable[DatasetRef]:
        """Expand data IDs in datasets that are assumed not to exist in the
        Registry.

        An expanded version of every given ref must be returned.  If one or
        more dataset refs already exist in the registry, the implementation may
        fail or ignore the fact that they exist.
        """
        raise NotImplementedError("Will delegate to self.query and self.datastore_bridge methods.")

    @abstractmethod
    def expand_existing_dataset_refs(
        self, refs: Iterable[DatasetRef], sign_for_get: bool = False, sign_for_unstore: bool = False
    ) -> Iterable[DatasetRef]:
        """Expand data IDs in datasets that are assumed to exist in the
        Registry.

        Datasets that do not actually exist in the Registry need not be
        returned, but implementations should trust already-expanded content in
        the given refs to avoid unnecessary queries.
        """
        raise NotImplementedError("Will delegate to self.query and self.datastore_bridge methods.")
