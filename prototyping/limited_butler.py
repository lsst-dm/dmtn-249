from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from contextlib import AbstractContextManager
from typing import Any

from lsst.daf.butler import DeferredDatasetHandle, DimensionUniverse, FileDataset
from lsst.resources import ResourcePath

from .aliases import GetParameter, InMemoryDataset
from .primitives import DatasetRef
from .raw_batch import RawBatch


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
    # - I've added vectorized 'many' methods for everything.  I think that's
    #   good future-proofing for performance, especially when you consider http
    #   latency + gazillions of tiny (e.g. metric value or cutout-service
    #   postage stamp) datasets.
    #
    # - Should we have async variants of get/put/exists?  If so should those be
    #   be vectorized?  How would async relate to get_deferred?
    #
    # - The get_uri family needs at least one more argument if we want it to be
    #   able to predict URIs for datasets that don't exist, and it may need a
    #   much richer interface if we want it to URIs that are signed, especially
    #   if they are signed differently for read vs. write vs. delete vs....
    #
    # - I've dropped any version of 'exists' for now with the full knowledge we
    #   need to add something of that sort back.  I haven't been able to come
    #   up with a LimitedButler signature for that that actually makes sense
    #   for full Butler, too, since only the latter has a "Registry existence"
    #   as a meaningful concept.  Something that explicitly only checks for
    #   Datastore existence only might be all we want in LimitedButler, but
    #   then we need to make sure the name doesn't imply more than that when
    #   full Butler implements that interface (since implemntation shoudln't
    #   change what it means) if we want the new interface to satisfy DM-32940.
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
        """Write a dataset given a DatasetRef with a fully-expanded data ID
        (but no Datastore records).

        The returned DatasetRef will be further expanded to include the new
        Datastore records as well.
        """
        (new_ref,) = self.put_many([(obj, ref)])
        return new_ref

    @abstractmethod
    def put_many(self, arg: Iterable[tuple[InMemoryDataset, DatasetRef]], /) -> Iterable[DatasetRef]:
        """Write datasets given DatasetRefs with fully-expanded data IDs (but
        no Datastore records).

        The returned DatasetRefs will be further expanded to include the new
        Datastore records as well.  They may not be returned in the same order
        as the given ones.
        """
        raise NotImplementedError()

    def get(
        self,
        ref: DatasetRef,
        *,
        parameters: Mapping[GetParameter, Any] | None = None,
    ) -> InMemoryDataset:
        """Fetch a dataset given a DatasetRef with a fully-expanded data ID,
        including datastore records.
        """
        ((_, _, result),) = self.get_many([(ref, parameters)])
        return result

    @abstractmethod
    def get_many(
        self,
        arg: Iterable[tuple[DatasetRef, Mapping[GetParameter, Any] | None]],
        /,
    ) -> Iterable[tuple[DatasetRef, Mapping[GetParameter, Any], InMemoryDataset]]:
        """Fetch datasets given DatasetRefs with fully-expanded data IDs and
        Datastore records.
        """
        raise NotImplementedError()

    def get_deferred(
        self,
        ref: DatasetRef,
        *,
        parameters: Mapping[GetParameter, Any] | None = None,
    ) -> DeferredDatasetHandle:
        ((_, _, handle),) = self.get_many_deferred([(ref, parameters)])
        return handle

    @abstractmethod
    def get_many_deferred(
        self,
        arg: Iterable[tuple[DatasetRef, Mapping[GetParameter, Any] | None]],
        /,
    ) -> Iterable[tuple[DatasetRef, Mapping[GetParameter, Any], DeferredDatasetHandle]]:
        raise NotImplementedError()

    def get_uri(self, ref: DatasetRef) -> ResourcePath:
        pairs = list(self.get_many_uris([ref]))
        if len(pairs) != 1:
            raise ValueError(f"Dataset {ref} has no single unique URI; use get_many_uri instead.")
        return pairs[0][1]

    @abstractmethod
    def get_many_uris(self, refs: Iterable[DatasetRef]) -> Iterable[tuple[DatasetRef, ResourcePath]]:
        raise NotImplementedError()

    @abstractmethod
    def unstore(self, refs: Iterable[DatasetRef]) -> None:
        raise NotImplementedError()

    @property
    @abstractmethod
    def dimensions(self) -> DimensionUniverse:
        raise NotImplementedError()

    @property
    @abstractmethod
    def is_writeable(self) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def _make_extractor(
        self,
        directory: ResourcePath | None,
        raw_batch: RawBatch,
        file_datasets: list[FileDataset],
        *,
        transfer: str | None = None,
    ) -> LimitedButlerExtractor:
        raise NotImplementedError()

    @abstractmethod
    def export(
        self,
        filename: ResourcePath,
        directory: ResourcePath | None,
        *,
        transfer: str | None = None,
    ) -> AbstractContextManager[LimitedButlerExtractor]:
        raise NotImplementedError()


class LimitedButlerExtractor(ABC):
    @abstractmethod
    def include_datasets(self, refs: Iterable[DatasetRef]) -> None:
        raise NotImplementedError()
