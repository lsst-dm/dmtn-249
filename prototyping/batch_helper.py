from __future__ import annotations

import itertools
from collections.abc import Iterable, Iterator, Sequence
from contextlib import ExitStack, contextmanager
from typing import TYPE_CHECKING

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
    non-batch forwarding methods on Butler itself.

    Notes
    -----
    Method call order here is not respected - instead, operations are applied
    in a fixed order based on the operation type.  This saves the user from
    having to know anything about the order needed to satisfy foreign keys, but
    we will have to add guards to prevent users from e.g. attempting to delete
    an existing collection and then create a new one with he same name in a
    batch, as that would actually first attempt to create the new one and then
    try to delete that later.

    We also need to be wary of users catching exceptions raised by these
    methods instead of letting them propagate up to the context manager.  We
    can't stop that from happening, so to be absolute safe we should make sure
    all of these methods provide strong exception safety on their own (i.e.
    leave all helper state unchanged if there is an exception in the middle of
    them).  That is definitely not the case right now for the more complex
    methods.
    """

    def __init__(self, butler: Butler, raw_batch: RawBatch, exit_stack: ExitStack):
        self.butler = butler
        self._raw_batch = raw_batch
        self._exit_stack = exit_stack

    ###########################################################################
    #
    # Collection manipulation, adapted from the current Registry public
    # interface.
    #
    # Open questions / notable changes:
    #
    # - registerRun is gone, but RUN is now the default type for
    #   register_collections.
    #
    # - merge_certifications comes from DM-36590, which appears to be what the
    #   CPP team really want instead of a decertify CLI.  I still think
    #   decertify is worth keeping in its current form, but with
    #   merge_certificatoins it becomes more of a rare-use admin tool instead
    #   of ever becoming part of the normal calibration management workflow.
    #
    # - removal and the RemovalHelper class are an attempt to provide both
    #   atomic collection deletion and some of the conveniences of the
    #   remove-collections CLI in Python.
    #
    ###########################################################################

    def register_collection(
        self,
        name: CollectionName,
        type: CollectionType = CollectionType.RUN,
        doc: CollectionDocumentation = "",
    ) -> None:
        self._raw_batch.collection_registrations.append((name, type, doc))

    def edit_collection_chain(
        self,
        chain_name: CollectionName,
        children: Sequence[CollectionName | int],
        mode: SequenceEditMode,
        *,
        flatten: bool = False,
    ) -> None:
        self._raw_batch.collection_edits.append(
            ChainedCollectionEdit(chain_name, list(children), mode, flatten)
        )

    def set_collection_documentation(self, name: CollectionName, doc: CollectionDocumentation) -> None:
        self._raw_batch.collection_edits.append(SetCollectionDocumentation(name, doc))

    def edit_associations(
        self, collection: CollectionName, refs: Iterable[DatasetRef], mode: SetEditMode
    ) -> None:
        uuids = {ref.uuid for ref in refs}
        self._raw_batch.collection_edits.extend(TaggedCollectionEdit(collection, uuids, mode))

    def certify(self, collection: CollectionName, refs: Iterable[DatasetRef], timespan: Timespan) -> None:
        raise NotImplementedError("TODO")

    def decertify(
        self,
        collection: CollectionName,
        dataset_type: DatasetTypeName | DatasetType,
        timespan: Timespan | None = None,
        *,
        data_ids: Iterable[DataId] | None = None,
    ) -> None:
        raise NotImplementedError("TODO")

    def merge_certifications(self, output: CollectionName, inputs: Sequence[CollectionName]) -> None:
        raise NotImplementedError("TODO")

    @contextmanager
    def removal(self) -> Iterator[RemovalHelper]:
        helper = RemovalHelper(self.butler)
        yield helper
        if helper:
            self._raw_batch.dataset_removals.include(helper.datasets)
            self._raw_batch.collection_removals.update(helper.collections)
            for chain_name, children in helper.chain_links.items():
                if chain_name not in self._raw_batch.collection_removals:
                    self.edit_collection_chain(chain_name, children, SequenceEditMode.REMOVE)
            for collection, refs in helper.associations.items():
                self.edit_associations(collection, refs, SetEditMode.REMOVE)
            opaque_table_uuids = self._exit_stack.enter_context(
                self.butler._datastore.unstore_transaction(helper.datasets)
            )
            self._raw_batch.opaque_table_removals.include(opaque_table_uuids)

    ###########################################################################
    #
    # Dataset type manipulation, adapted from the current Registry public
    # interface.
    #
    # Open questions / notable changes:
    #
    # - register_dataset_type doesn't need to be passed a full DatasetType
    #   instance anymore, so it's less verbose to call.
    #
    # - register_dataset_type now has an `update` option, which could be used
    #   to change the storage class.  If there are no datasets that need to be
    #   updated it could also modify is_calibration and the dimensions.  If
    #   we retire NameKeyCollectionManager we could also pretty easily add a
    #   ``rename: str | None = None`` that could do renames when
    #   ``update=True``.
    #
    ###########################################################################

    def register_dataset_type(
        self,
        dataset_type_or_name: DatasetTypeName | DatasetType,
        /,
        dimensions: Iterable[DimensionName] | None = None,
        storage_class: StorageClassName | StorageClass | None = None,
        is_calibration: bool | None = None,
        update: bool = False,
    ) -> None:
        if isinstance(dataset_type_or_name, DatasetType):
            registration = DatasetTypeRegistration.from_dataset_type(dataset_type_or_name, update)
        else:
            raise NotImplementedError("Check and conform arguments, make a DatasetTypeRegistration.")
        registration.add_to(self._raw_batch)

    def remove_dataset_type(self, name: str) -> None:
        self._raw_batch.dataset_type_removals.add(name)

    ###########################################################################
    #
    # Dimension data manipulation.
    #
    ###########################################################################

    def insert_dimension_data(
        self,
        element: DimensionElementName,
        data: Iterable[DimensionRecord],
        mode: SetEditMode = SetEditMode.INSERT_OR_FAIL,
    ) -> None:
        raise NotImplementedError("Make a DimensionDataInsert and add it to batch.")

    def sync_dimension_data(self, record: DimensionRecord, update: bool = False) -> None:
        raise NotImplementedError(
            """Make a DimensionDataSync and add it to batch.

            But this needs some interface work to make the on_update and
            on_insert fields work; maybe nested context managers, maybe
            just exposing those classes or a higher-level non-serializable
            counterpart.
            """
        )

    ###########################################################################
    #
    # Bulk transfers
    #
    # 'ingest' is different here largely because of RFC-888 - if resolved refs
    # are required, then the RUN collection and dataset ID generation mode have
    # to be applied when creating the FileDataset objects, not here.  We'll
    # to see if that's a burden in the places we call `ingest` downstream and
    # provide some utility code for making FileDatasets if that's the case.
    #
    ###########################################################################

    def ingest(
        self,
        *datasets: FileDataset,
        directory: ResourcePathExpression | None = None,
        transfer: str | None = "auto",
        own_absolute: bool = False,
        record_validation_info: bool = True,
    ) -> None:
        self._raw_batch.dataset_insertions.include(itertools.chain.from_iterable(d.refs for d in datasets))
        if directory is not None:
            directory = ResourcePath(directory)
        opaque_table_data = self._exit_stack.enter_context(
            self.butler._datastore.import_(
                datasets,
                mode=transfer,
                own_absolute=own_absolute,
                directory=directory,
                record_validation_info=record_validation_info,
            )
        )
        self._raw_batch.opaque_table_insertions.update(opaque_table_data)
