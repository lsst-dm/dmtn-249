from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Mapping
from contextlib import AbstractContextManager
from typing import Any

from lsst.daf.butler import DeferredDatasetHandle, DimensionUniverse, FileDataset
from lsst.resources import ResourcePath

from .aliases import GetParameter, InMemoryDataset
from .datastore import DatastoreConfig
from .primitives import DatasetRef
from .raw_batch import RawBatch


class LimitedButler(ABC):
    """A minimal abstract butler interface that is sufficient to back
    `~lsst.pipe.base.PipelineTask` execution and support other simplified use
    cases.

    Notes
    -----
    - This includes RFC-888, which drops all of the *Direct methods in favor of
      overloading.  But LimitedButler only has the "takes a DatasetRef"
      overloads; the "takes a DatasetType [name] and data ID" overloads are
      added by the full Butler subclass.

    - I've added vectorized 'many' methods for everything.  I think that's good
      future-proofing for performance, especially when you consider http
      latency + gazillions of tiny (e.g. metric value or cutout-service postage
      stamp) datasets.  All of the scalar methods delegate to these.

    - I've dropped any version of 'exists' for now with the full knowledge we
      need to add something of that sort back.  I haven't been able to come up
      with a LimitedButler signature for that that actually makes sense for
      full Butler, too, since only the latter has a "Registry existence" as a
      meaningful concept.  Something that explicitly only checks for Datastore
      existence only might be all we want in LimitedButler, but then we need to
      make sure the name doesn't imply more than that when full Butler
      implements that interface (since implementation shouldn't change what it
      means) if we want the new interface to satisfy DM-32940.

    - LimitedButler does not specify its constructor interface and there is no
      expectation that there will be a single factory method that can construct
      any LimitedButler instance - if you want to get the right kind of Butler
      with just a repository URI, that's a Butler feature, not a LimitedButler
      feature.
    """

    def put(self, obj: InMemoryDataset, ref: DatasetRef) -> DatasetRef:
        """Write a dataset given a DatasetRef with a fully-expanded data ID
        (but no Datastore records).

        The returned DatasetRef will be further expanded to include the new
        Datastore records as well.

        Notes
        -----
        The full Butler will have an overload of this method that takes dataset
        type and data ID, and its version of this overload will not require the
        `DatasetRef` to be fully expanded (though it will have to be resolved,
        since all DatasetRefs will be after RFC-888).
        """
        (new_ref,) = self.put_many([(obj, ref)])
        return new_ref

    @abstractmethod
    def put_many(self, arg: Iterable[tuple[InMemoryDataset, DatasetRef]], /) -> Iterable[DatasetRef]:
        """Vectorized implementation of `put`."""
        raise NotImplementedError()

    @abstractmethod
    def predict_put_many(self, refs: Iterable[DatasetRef]) -> Iterable[DatasetRef]:
        """Return an iterable of `DatasetRef` objects that have been augmented
        with datastore records as if they had been passed to `put_many`.

        This is intended to be used by QuantumGraph generation to pre-populate
        datastore records for intermediates as well as inputs.  It doesn't
        actually need to be present on QuantumBackedButler (which consumes
        these predictions rather than creating them), but I think
        `LimitedButler` is the right place to put this interface.
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

        Notes
        -----
        The full Butler will have an overload of this method that takes dataset
        type and data ID, and its version of this overload will not require the
        `DatasetRef` to be fully expanded (though they will have to be
        resolved, since all DatasetRefs will be after RFC-888).
        """
        ((_, _, result),) = self.get_many([(ref, parameters)])
        return result

    @abstractmethod
    def get_many(
        self,
        arg: Iterable[tuple[DatasetRef, Mapping[GetParameter, Any] | None]],
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
        `DatasetRef` to be fully expanded (though they will have to be
        resolved, since all DatasetRefs will be after RFC-888).
        """
        ((_, _, handle),) = self.get_many_deferred([(ref, parameters)])
        return handle

    @abstractmethod
    def get_many_deferred(
        self,
        arg: Iterable[tuple[DatasetRef, Mapping[GetParameter, Any] | None]],
        /,
    ) -> Iterable[tuple[DatasetRef, Mapping[GetParameter, Any], DeferredDatasetHandle]]:
        """Vectorized implementation of `get_deferred`."""
        raise NotImplementedError()

    def get_uri(self, ref: DatasetRef) -> ResourcePath:
        """Return the primary URI for the given dataset.

        For datasets that may have multiple URIs, or to get URIs for many
        datasets at once, use `get_many_uris` instead.

        Notes
        -----
        The full Butler will have an overload of this method that takes dataset
        type and data ID, and its version of this overload will not require the
        `DatasetRef` to be fully expanded (though they will have to be
        resolved, since all DatasetRefs will be after RFC-888).

        This method and its ``_many`` counterpart need at least one more
        argument if we want it to be able to predict URIs for datasets that
        don't exist, and it may need a much richer interface if we want it to
        URIs that are signed, especially if they are signed differently for
        read vs. write vs. delete vs....
        """
        pairs = list(self.get_many_uris([ref]))
        if len(pairs) != 1:
            raise ValueError(f"Dataset {ref} has no single unique URI; use get_many_uri instead.")
        return pairs[0][1]

    @abstractmethod
    def get_many_uris(self, refs: Iterable[DatasetRef]) -> Iterable[tuple[DatasetRef, ResourcePath]]:
        """Return all URIs for the given datasets.

        The given `DatasetRef` objects may not all be returned; some may be
        returned via constituent component `DatasetRef` objects, if they map
        to disassembled datasets, and datasets that are stored in Datastore
        that does not URIs will not be returned at all.
        """
        raise NotImplementedError()

    @abstractmethod
    def unstore(self, refs: Iterable[DatasetRef]) -> None:
        """Remove the referenced datasets from storage.

        Notes
        -----
        This is a Datastore-only removal; `LimitedButler` has no concept of
        removal from Registry because it doesn't have one, and always works
        with expanded `DatasetRef` objects that already include all content
        that a Registry might add.

        The full-Butler version of this method also performs Datastore-only
        removals, but that does include removing datastore opaque records from
        the Registry.  The full-Butler version also accepts non-expanded
        `DatasetRef objects`
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

    @abstractmethod
    def _make_extractor(
        self,
        raw_batch: RawBatch,
        file_datasets: dict[ResourcePath, list[FileDataset]],
        on_commit: Callable[[DatastoreConfig | None], None],
        *,
        transfer: str | None = None,
        directory: ResourcePath | None = None,
    ) -> LimitedButlerExtractor:
        """Return a helper object that prodies an interface for transferring
        content out of this butler.

        This is a package-private method for use by other Butler objects and
        helper classes, most notably `LimitedButler.export` (and its full
        `Butler` override) and `Butler.transfer_from`.

        Parameters
        ----------
        raw_batch
            Serializable representation of multiple Registry operations to be
            applied in a single transaction.  This will be modified in-place by
            the returned extractor object.
        file_datasets
            Dictionary that will be modified to hold exported file-dataset
            associations, with root URIs as keys.
        on_commit
            A callback to invoke when `LimitedExtractor.commit` is called.  It
            takes a single argument, which will be the exporting Datastore's
            configuration or `None`.  This is typically a closure that captures
            ``raw_batch`` and ``file_datasets`` before they are passed here,
            and then uses the information added to them by the extractor.
        transfer, optional
            Transfer mode recognized by `ResourcePath`.  If `None`,
            ``directory`` is ignored, all datasets are left where they are and
            Datastore root URIs will be used as the keys in ``file_datasets``.
            If not `None`, either all files must have absolute URIs or
            ``directory`` must not be `None`, and ``directory`` will be the
            only key in the ``file_datasets`` mapping returned.
        directory, optional
            Root URI for exported datasets after the transfer.  Ignored if
            ``transfer`` is `None`.  Must be provided if ``transfer`` is not
            `None` unless all exported files have absolute URIs within the
            Datastore already.

        Returns
        -------
        extractor
            Object that provides an interface for defining the content to be
            extracted from the repository.

        Notes
        -----
        Full `Butler` reimplements this to return a `ButlerExtractor`, which
        provides methods to extract registry content.
        """
        raise NotImplementedError()

    @abstractmethod
    def export(
        self,
        filename: ResourcePath,
        *,
        transfer: str | None = None,
        directory: ResourcePath | None,
    ) -> AbstractContextManager[LimitedButlerExtractor]:
        """Export content from this data repository to a metadata file and
        optional directory of dataset files.

        Parameters
        ----------
        filename
            URI for the metadata file that allows the exported content to be
            imported into another data repository.
        transfer, optional
            Transfer mode recognized by `ResourcePath`.
        directory, optional
            Root URI for exported datasets after the transfer.  Ignored if
            ``transfer`` is `None`.  Must be provided if ``transfer`` is not
            `None` unless all exported files have absolute URIs within the
            Datastore already.

        Returns
        -------
        extractor_context
            Context manager that returns a `LimitedButlerExtractor` instance
            when entered and writes the metadata file when it exits without
            error.
        """
        raise NotImplementedError()


class LimitedButlerExtractor(ABC):
    """ABC for the helper classes returned by `LimitedButler._make_extractor`,
    `LimitedButler.export`, and `Butler.transfer_from`.
    """

    butler: LimitedButler
    """Parent butler object being extracted from.
    """

    @abstractmethod
    def include_datasets(
        self,
        refs: Iterable[DatasetRef],
        include_types: bool = True,
        include_dimensions: bool = True,
        include_run_collections: bool = True,
    ) -> None:
        """Include the referenced datasets in the content transfer.

        Parameters
        ----------
        refs
            Dataset references to export.  Must have fully-expanded data IDs
            and datastore records (the full `Butler` variant of this method
            does not require this).
        include_types, optional
            If `True`, also export registrations for all dataset types in
            ``refs``.
        include_dimensions, optional
            If `True`, also export dimension records for all data IDs in
            ``refs``.
        include_run_collections, optional
            If `True`, also export collection registrations for all RUN
            collections in ``refs``.
        """
        raise NotImplementedError()

    @abstractmethod
    def commit(self) -> None:
        """Mark the current state of the extraction as a complete transaction
        and start a new one.
        """
        raise NotImplementedError()
