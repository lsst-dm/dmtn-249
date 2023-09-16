from __future__ import annotations

import itertools
from collections.abc import Iterable, Iterator, Sequence
from contextlib import ExitStack, contextmanager
from typing import TYPE_CHECKING, cast

from lsst.daf.butler import CollectionType, DataId, DimensionRecord, FileDataset, StorageClass, Timespan
from lsst.resources import ResourcePath, ResourcePathExpression

from .aliases import (
    CollectionDocumentation,
    CollectionName,
    DatasetTypeName,
    DimensionElementName,
    DimensionName,
    StorageClassName,
)
from .primitives import DatasetRef, DatasetType
from .raw_batch import (
    ChainedCollectionEdit,
    CollectionRegistration,
    DatasetTypeRegistration,
    RawBatch,
    SequenceEditMode,
    SetCollectionDocumentation,
    SetEditMode,
    TaggedCollectionEdit,
)

if TYPE_CHECKING:
    from .butler import Butler
    from .removal_helper import RemovalHelper


class BatchHelper:
    """Helper object that allows Butler write operations to be batched up
    and executed (usually) within a single transaction with lower (overall)
    latency.

    Much of the Butler write interface is actually implemented here, so that's
    where I've put the corresponding comments and docs (as opposed to the
    non-batch forwarding methods on Butler itself).

    Notes
    -----
    Method call order here is not respected - instead, operations are applied
    in a fixed order based on the operation type.  This saves the user from
    having to know anything about the order needed to satisfy foreign keys, but
    we will have to add guards to prevent users from e.g. attempting to delete
    an existing collection and then create a new one with he same name in a
    batch, as that would actually first attempt to create the new one and then
    try to delete that same new collection later.

    We also need to be wary of users catching exceptions raised by these
    methods instead of letting them propagate up to the context manager.  We
    can't stop that from happening, so to be absolutely safe we should make
    sure all of these methods provide strong exception safety on their own
    (i.e.  leave all helper state unchanged if there is an exception in the
    middle of them).  That is definitely not the case right now for the more
    complex methods.
    """

    def __init__(self, butler: Butler, raw_batch: RawBatch, exit_stack: ExitStack):
        self.butler = butler
        self._raw_batch = raw_batch
        self._exit_stack = exit_stack

    def register_collection(
        self,
        name: CollectionName,
        type: CollectionType = CollectionType.RUN,
        doc: CollectionDocumentation = "",
    ) -> None:
        """Like `Registry.registerCollection`, but the default collection type
        is RUN.

        RUN is more useful default than TAGGED, and with that as the default
        it's easier to drop the separate current `registerRun` method in favor
        of just this.
        """
        self._raw_batch.collection_registrations[name] = CollectionRegistration(name, type, doc)

    def edit_collection_chain(
        self,
        chain_name: CollectionName,
        children: Sequence[CollectionName | int],
        mode: SequenceEditMode,
        *,
        flatten: bool = False,
    ) -> None:
        """Modify the definition of a CHAINED collection.

        This is a replacement for `Registry.setCollectionChain` that picks up
        the mode-setting conveniences from the ``butler collection-chain` CLI.
        """
        self._raw_batch.collection_edits.append(
            ChainedCollectionEdit(chain_name, list(children), mode, flatten)
        )

    def set_collection_documentation(self, name: CollectionName, doc: CollectionDocumentation) -> None:
        """Like the current `Registry.setCollectionDocumentation`."""
        self._raw_batch.collection_edits.append(SetCollectionDocumentation(name, doc))

    def edit_associations(
        self, collection: CollectionName, refs: Iterable[DatasetRef], mode: SetEditMode
    ) -> None:
        """Unified replacement for `Registry.associate` and
        `Registry.disassociate`.

        This can also handle other edit modes, which mostly amounts to conflict
        resolution options.
        """
        uuids = {ref.uuid for ref in refs}
        self._raw_batch.collection_edits.append(TaggedCollectionEdit(collection, uuids, mode))

    def certify(self, collection: CollectionName, refs: Iterable[DatasetRef], timespan: Timespan) -> None:
        """Like the current `Registry.certify`."""
        raise NotImplementedError("TODO")

    def decertify(
        self,
        collection: CollectionName,
        dataset_type: DatasetTypeName | DatasetType,
        timespan: Timespan | None = None,
        *,
        data_ids: Iterable[DataId] | None = None,
    ) -> None:
        """Like the current `Registry.decertify`.

        I'd still like to unify this with `certify` with more convenient
        `edit_` interface, as CHAINED and TAGGED collections now support, but
        that requires more work because they different modes here seem like
        they need to be packaged with different arguments.  I'm putting it off
        since it doesn't impact the big picture.
        """
        raise NotImplementedError("TODO")

    def merge_certifications(self, output: CollectionName, inputs: Sequence[CollectionName]) -> None:
        """Flatten multiple CALIBRATION collections into a single one,
        resolving conflicts in priority order.

        This comes from DM-36590, which appears to be what the CPP team really
        want instead of a decertify CLI.  I still think at least some of
        decertify is worth keeping in its current form, but with
        merge_certificatoins it becomes more of a rare-use admin tool instead
        of ever becoming part of the normal calibration management workflow.
        """
        raise NotImplementedError("TODO")

    @contextmanager
    def removal(self) -> Iterator[RemovalHelper]:
        """Remove content from the data repository.

        This method is for fully removing ("purging") datasets and collections.
        To remove datasets from storage while retaining their metadata, use
        `unstore` instead.

        Returns
        -------
        removal_context
            Context manager that when enters returns a `RemovalHelper` object
            that provides methods for identifying the objects to be removed
            and tracking their dependencies.
        """
        helper = RemovalHelper(self.butler)
        yield helper
        if helper:
            self._raw_batch.dataset_removals.include(helper.datasets)
            self._raw_batch.collection_removals.update(helper.collections)
            for chain_name, children in helper.chain_links.items():
                if chain_name not in self._raw_batch.collection_removals:
                    self.edit_collection_chain(chain_name, list(children), SequenceEditMode.REMOVE)
            for collection, refs in helper.associations.items():
                self.edit_associations(collection, refs, SetEditMode.REMOVE)
            opaque_table_uuids = self._exit_stack.enter_context(
                self.butler._datastore.unstore_transaction(helper.datasets, helper._journal_uris)
            )
            self._raw_batch.opaque_table_removals.update(opaque_table_uuids)

    def register_dataset_type(
        self,
        dataset_type_or_name: DatasetTypeName | DatasetType,
        /,
        dimensions: Iterable[DimensionName] | None = None,
        storage_class: StorageClassName | StorageClass | None = None,
        is_calibration: bool | None = None,
        update: bool = False,
    ) -> None:
        """Like the current `Registry.registerDatasetType`, but it can also be
        passed the args needed to construct a `DatasetType` instance instead of
        a `DatasetType` instance.

        This also includes an ``update`` option for at least storage-class
        changes.  If there are no datasets that need to be updated it could
        also modify ``is_calibration`` and the ``dimensions``.

        If we retire `NameKeyCollectionManager` we could also pretty easily add
        a ``rename: str | None = None`` that could do renames when
        ``update=True``.
        """
        if isinstance(dataset_type_or_name, DatasetType):
            registration = DatasetTypeRegistration.from_dataset_type(dataset_type_or_name, update)
        else:
            raise NotImplementedError("Check and conform arguments, make a DatasetTypeRegistration.")
        registration.add_to(self._raw_batch)

    def remove_dataset_type(self, name: str) -> None:
        """Like the current `Registry.removeDatasetType`.

        This will also still fail if there are any datasets of this type.  We
        could add something to `RemovalHelper` if we wanted to support
        automatically deleting datasets when the type is removed.
        """
        self._raw_batch.dataset_type_removals.add(name)

    def sync_dimension_data(self, record: DimensionRecord, update: bool = False) -> None:
        """Like the current `Registry.syncDimensionData`, but it can't return
        anything since the action is deferred.  The use cases that relied on
        that should be able to use the ``on_insert_of`` and ``on_update_of``
        args to `insert_dimension_data` instead.
        """
        raise NotImplementedError(
            """Make a DimensionDataSync and add it to batch.
            """
        )

    def insert_dimension_data(
        self,
        element: DimensionElementName,
        data: Iterable[DimensionRecord],
        mode: SetEditMode = SetEditMode.INSERT_OR_FAIL,
        on_update_of: DimensionElementName | None = None,
        on_insert_of: DimensionElementName | None = None,
    ) -> None:
        """Like the current `Registry.insertDimensionData`, but it can't return
        anything since the action is deferred.  The use cases that relied on
        that should be able to use the ``on_insert`` and ``on_update`` args
        to `insert_dimension_data` instead.
        """
        raise NotImplementedError(
            """Make a DimensionDataInsert and add it to batch.

            If on_update_of or on_insert_of is not None, we nest the new
            DimensionDataInsert under a DimensionDataSync that must already
            exist.  If both are None, we make check that any existing inserts
            for this element use the same mode, and fail if they do not.
            """
        )

    def ingest(
        self,
        *datasets: FileDataset,
        directory: ResourcePathExpression | None = None,
        transfer: str | None = "auto",
        own_absolute: bool = False,
        record_validation_info: bool = True,
    ) -> None:
        """Ingest external datasets.

        This signature is different here largely because of RFC-888 - if
        DatasetRefs are always require, then the RUN collection and dataset ID
        generation mode have to be applied when creating the FileDataset
        objects, not here.  We'll to see if that's a burden in the places we
        call `ingest` downstream, and provide some utility code for making
        `FileDataset` instances with resolved refs if that's the case.
        """
        expanded_datasets, journal_paths = self.butler._registry.expand_new_file_datasets(datasets, sign=True)
        self._raw_batch.dataset_insertions.include(
            itertools.chain.from_iterable(cast(Iterable[DatasetRef], d.refs) for d in expanded_datasets)
        )
        if directory is not None:
            directory = ResourcePath(directory)
        opaque_table_data = self._exit_stack.enter_context(
            self.butler._datastore.receive(
                {directory: list(expanded_datasets)},
                journal_paths,
                transfer=transfer,
                own_absolute=own_absolute,
                record_validation_info=record_validation_info,
            )
        )
        self._raw_batch.opaque_table_insertions.update(opaque_table_data)
