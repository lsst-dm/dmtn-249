from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

from lsst.daf.butler import DataCoordinate, DatasetRef, DimensionRecord, FileDataset
from lsst.resources import ResourcePath

from .aliases import CollectionName, DimensionElementName
from .datastore_butler import DatastoreButlerExtractor
from .primitives import SetInsertMode
from .raw_batch import RawBatch

if TYPE_CHECKING:
    from .butler import Butler


class ButlerExtractor(DatastoreButlerExtractor):
    """Helper class returned by `Butler.export` and when calling
    `Butler.transfer_from` on a full `Butler`.

    See `DatastoreButlerExtractor` for ``__init__`` parameter documentation.
    """

    def __init__(
        self,
        butler: Butler,
        directory: ResourcePath | None,
        transfer: str | None,
        raw_batch: RawBatch,
        file_datasets: list[FileDataset],
        include_datastore_records: bool,
    ):
        self.butler: Butler
        super().__init__(
            self, butler, directory, transfer, raw_batch, file_datasets, include_datastore_records
        )

    def include_datasets(
        self,
        refs: Iterable[DatasetRef],
        include_types: bool = True,
        include_dimensions: bool = True,
        include_run_collections: bool = True,
    ) -> None:
        # Docs inherited; only change is that this overload does not require
        # fully-expanded DatasetRefs.
        super().include_datasets(
            self.butler._expand_existing_dataset_refs(refs),
            include_types=include_types,
            include_dimensions=include_dimensions,
            include_run_collections=include_run_collections,
        )

    def include_collections(self, names: Iterable[CollectionName]) -> None:
        raise NotImplementedError(
            """Use butler.query() to get types and docs, then update
            self._raw_batch.
            """
        )

    def include_dimension_records(
        self,
        element: DimensionElementName,
        records: Iterable[DimensionRecord],
        mode: SetInsertMode = SetInsertMode.INSERT_OR_SKIP,
    ) -> None:
        raise NotImplementedError(
            """Extract dimension records and update self._raw_batch, while
            dealing with mode conflicts.
            """
        )

    def include_data_ids(
        self, data_ids: Iterable[DataCoordinate], mode: SetInsertMode = SetInsertMode.INSERT_OR_SKIP
    ) -> None:
        raise NotImplementedError(
            """Extract dimension records and update self._raw_batch, while
            dealing with mode conflicts.
            """
        )

    def commit(self) -> None:
        raise NotImplementedError(
            """Query for associations between exported TAGGED and CALIBRATION
            collections and the exported DatasetRefs and add those to
            self._raw_batch, then call super().commit().
            """
        )
