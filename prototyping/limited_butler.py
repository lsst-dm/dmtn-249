from __future__ import annotations

__all__ = ("LimitedButler",)

from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from typing import Any

from lsst.daf.butler import DeferredDatasetHandle, DimensionUniverse

from .aliases import GetParameter, InMemoryDataset, StorageURI
from .primitives import DatasetRef


class LimitedButler(ABC):
    """Minimal butler interface.

    This interface will be sufficient for `~lsst.pipe.base.PipelineTask`
    execution, in that it can fully back a `~lsst.pipe.base.QuantumContext`
    implementation.  It doesn't have the existence-check and removal
    capabilities of the current `LimitedButler` (which are needed by
    `lsst.ctrl.mpexec.SingleQuantumExecutor`) because DMTN-271's workspace
    concept will handle clobbering and existence-checks differently.
    """

    @property
    @abstractmethod
    def dimensions(self) -> DimensionUniverse:
        """Definitions for all dimensions."""
        raise NotImplementedError()

    @property
    @abstractmethod
    def is_writeable(self) -> bool:
        """Whether `put` and other write methods are supposed."""
        raise NotImplementedError()

    def get(
        self,
        ref: DatasetRef,
        *,
        parameters: Mapping[GetParameter, Any] | None = None,
    ) -> InMemoryDataset:
        """Load a dataset, given a `DatasetRef` with a fully-expanded data ID
        and datastore records.

        Notes
        -----
        The full `Butler` will have an overload of this method that takes
        dataset type and data ID, and its version of this overload will not
        require the `DatasetRef` to be fully expanded.
        """
        ((_, _, result),) = self.get_many([(ref, parameters if parameters is not None else {})])
        return result

    @abstractmethod
    def get_many(
        self,
        arg: Iterable[tuple[DatasetRef, Mapping[GetParameter, Any]]],
        /,
    ) -> Iterable[tuple[DatasetRef, Mapping[GetParameter, Any], InMemoryDataset]]:
        """Vectorized implementation of `get`."""
        raise NotImplementedError()

    def get_deferred(
        self,
        ref: DatasetRef,
        *,
        parameters: Mapping[GetParameter, Any] | None = None,
    ) -> DeferredDatasetHandle:
        """Return a handle that can fetch a dataset into memory later.

        Notes
        -----
        The full `Butler` will have an overload of this method that takes
        dataset type and data ID, and its version of this overload will not
        require the `DatasetRef` to be fully expanded.
        """
        ((_, _, handle),) = self.get_many_deferred([(ref, parameters if parameters is not None else {})])
        return handle

    def get_many_deferred(
        self,
        arg: Iterable[tuple[DatasetRef, Mapping[GetParameter, Any]]],
        /,
    ) -> Iterable[tuple[DatasetRef, Mapping[GetParameter, Any], DeferredDatasetHandle]]:
        """Vectorized implementation of `get_deferred`."""
        return [
            (ref, parameters_for_ref, DeferredDatasetHandle(self, ref, parameters_for_ref))  # type: ignore
            for ref, parameters_for_ref in arg
        ]

    def get_uri(self, ref: DatasetRef) -> StorageURI:
        """Return the relative URI for the artifact that holds this dataset.

        The given dataset must have fully-expanded data IDs and datastore
        records.

        This method raises if there is no single URI for the dataset; use
        `get_many_uris` for the general case.

        Notes
        -----
        The full `Butler` will have an overload of this method that takes
        dataset type and data ID, and its version of this overload will not
        require the `DatasetRef` to be fully expanded.
        """
        ((_, uri),) = self.get_many_uris([ref])
        return uri

    @abstractmethod
    def get_many_uris(self, refs: Iterable[DatasetRef]) -> Iterable[tuple[DatasetRef, StorageURI]]:
        """Return all relative URIs associated with the given datasets.

        The given datasets must have fully-expanded data IDs and datastore
        records.

        Each `DatasetRef` may be associated with multiple URIs.

        Notes
        -----
        The full `Butler` version of this method will not require the given
        `DatasetRef` instances to be fully expanded.
        """
        raise NotImplementedError()

    def put(self, obj: InMemoryDataset, ref: DatasetRef) -> None:
        """Write a dataset given a DatasetRef with a fully-expanded data ID
        (but no Datastore records).

        Notes
        -----
        The full Butler will have an overload of this method that takes dataset
        type and data ID, and its version of this overload will not require the
        `DatasetRef` to be fully expanded.
        """
        self.put_many([(obj, ref)])

    @abstractmethod
    def put_many(self, arg: Iterable[tuple[InMemoryDataset, DatasetRef]], /) -> None:
        """Vectorized implementation of `put`."""
        raise NotImplementedError()
