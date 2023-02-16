from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from typing import Any

from lsst.daf.butler import DatasetRef, DeferredDatasetHandle, DimensionUniverse
from lsst.resources import ResourcePath

from .aliases import GetParameter, InMemoryDataset
from .primitives import DatasetExistence


class LimitedButler(ABC):
    """A minimal abstract butler interface that is sufficient to back
    `~lsst.pipe.base.PipelineTask` execution and support other simplified use
    cases.
    """

    ###########################################################################
    #
    # Open questions / notable changes:
    #
    # - This includes RFC-888, which drops all of the *Direct methods in favor
    #   of overloading.  But LimitedButler only has the "takes a DatasetRef"
    #   overloads; the "takes a DatasetType [name] and data ID" overloads are
    #   added by the full Butler subclass.
    #
    # - I've added vectorized m* methods for everything.  I think that's good
    #   future-proofing for performance, especially when you consider http
    #   latency + gazillions of tiny (e.g. metric value or cutout-service
    #   postage stamp) datasets.
    #
    # - Should we rename the m* methods?  I like mget/mput/mexists, but
    #   mget_deferred and mget_uri are pretty bad.  Would overloading
    #   vectorization al a Numpy ufuncs be better?
    #
    # - Should we have async variants of get/put/exists?  If so should those be
    #   be vectorized?  How would async relate to get_deferred?
    #
    # - I've dropped getURIs and kept just getURI (now get_uri) and its
    #   variants: I envision URIs now being something query_datasets can be
    #   used to get, and if we do continue to support disassembly I think we
    #   can relegate the general case of getting multiple URIs for one dataset
    #   to that.
    #
    # - I've replaced 'pruneDatasets' with new 'purge' and 'unstore' methods
    #   that I like better.  Full Butler will also have collection-level
    #   removal methods.
    #
    # - LimitedButler does not specify its constructor interface and there is
    #   no expectation that there will be a single factory method that can
    #   construct any LimitedButler instance - if you want to get the right
    #   kind of Butler with just a repository URI, that's a Butler addition,
    #   not a LimitedButler thing.
    #
    ###########################################################################

    def put(self, obj: InMemoryDataset, ref: DatasetRef) -> DatasetRef:
        self.mput([(obj, ref)])
        return ref

    @abstractmethod
    def mput(self, arg: Iterable[tuple[InMemoryDataset, DatasetRef]], /) -> None:
        raise NotImplementedError()

    def get(
        self,
        ref: DatasetRef,
        *,
        parameters: Mapping[GetParameter, Any] | None = None,
    ) -> InMemoryDataset:
        ((_, _, result),) = self.mget([(ref, parameters)])
        return result

    @abstractmethod
    def mget(
        self,
        arg: Iterable[tuple[DatasetRef, Mapping[GetParameter, Any] | None]],
        /,
    ) -> Iterable[tuple[DatasetRef, Mapping[GetParameter, Any], InMemoryDataset]]:
        raise NotImplementedError()

    def get_deferred(
        self,
        ref: DatasetRef,
        *,
        parameters: Mapping[GetParameter, Any] | None = None,
    ) -> DeferredDatasetHandle:
        ((_, _, handle),) = self.mget_deferred([(ref, parameters)])
        return handle

    @abstractmethod
    def mget_deferred(
        self,
        arg: Iterable[tuple[DatasetRef, Mapping[GetParameter, Any] | None]],
        /,
    ) -> Iterable[tuple[DatasetRef, Mapping[GetParameter, Any], DeferredDatasetHandle]]:
        raise NotImplementedError()

    def get_uri(self, ref: DatasetRef) -> ResourcePath:
        ((_, uri),) = self.mget_uri([ref])
        return uri

    @abstractmethod
    def mget_uri(
        self,
        arg: Iterable[DatasetRef],
        /,
    ) -> Iterable[tuple[DatasetRef, ResourcePath]]:
        raise NotImplementedError()

    def exists(self, ref: DatasetRef, exact: bool = True) -> DatasetExistence:
        (result,) = self.mexists([ref], exact=exact)
        return result

    @abstractmethod
    def mexists(
        self, refs: Iterable[DatasetRef], exact: bool = True
    ) -> Iterable[DatasetRef, DatasetExistence]:
        raise NotImplementedError()

    @abstractmethod
    def unstore(self, refs: Iterable[DatasetRef]) -> None:
        raise NotImplementedError()

    @abstractmethod
    def purge(self, refs: Iterable[DatasetRef]) -> None:
        """Fully delete datasets from datastore storage and their
        RUN collection.

        Should fail if other references to the dataset exist (e.g. TAGGED
        collection membership, in a full butler).  Full butlers will provide
        more complete deletion interfaces; this is just the subset that I think
        both full Butler and QBB need to support.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def dimensions(self) -> DimensionUniverse:
        raise NotImplementedError()

    @property
    @abstractmethod
    def is_writeable(self) -> bool:
        raise NotImplementedError()
