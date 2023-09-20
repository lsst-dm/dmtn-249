from __future__ import annotations

__all__ = ("MinimalButler",)

from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from typing import Any

from lsst.daf.butler import DimensionUniverse

from .aliases import GetParameter, InMemoryDataset
from .primitives import DatasetRef, DeferredDatasetHandle


class MinimalButler(ABC):
    """Minimal butler interfaces.

    This is a subset of today's `LimitedButler`, because we don't need it to be
    able to support `PipelineTask` if that all goes through `WorkspaceButler`.
    So this is just the read-only interface common to `WorkspaceButler` and
    full `Butler`.
    """

    @property
    @abstractmethod
    def dimensions(self) -> DimensionUniverse:
        raise NotImplementedError()

    @property
    @abstractmethod
    def is_writeable(self) -> bool:
        raise NotImplementedError()

    def get(
        self,
        ref: DatasetRef,
        *,
        parameters: Mapping[GetParameter, Any] | None = None,
    ) -> InMemoryDataset:
        """Fetch a dataset given a DatasetRef with a fully-expanded data ID,
        including datastore records.

        Notes
        -----
        The full Butler will have an overload of this method that takes dataset
        type and data ID, and its version of this overload will not require the
        `DatasetRef` to be fully expanded.
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
        The full Butler will have an overload of this method that takes dataset
        type and data ID, and its version of this overload will not require the
        `DatasetRef` to be fully expanded.
        """
        ((_, _, handle),) = self.get_many_deferred([(ref, parameters if parameters is not None else {})])
        return handle

    @abstractmethod
    def get_many_deferred(
        self,
        arg: Iterable[tuple[DatasetRef, Mapping[GetParameter, Any]]],
        /,
    ) -> Iterable[tuple[DatasetRef, Mapping[GetParameter, Any], DeferredDatasetHandle]]:
        """Vectorized implementation of `get_deferred`."""
        raise NotImplementedError()

    # No more getURIs - you can now ask an expanded DatasetRef for them.  But
    # maybe we need a way to obtained signed URIs?
