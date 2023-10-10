"""Modified versions of simple lsst.daf.butler objects."""

from __future__ import annotations

__all__ = ("DatasetRef",)

import dataclasses
import uuid
from typing import final

from lsst.daf.butler import (
    DataCoordinate,
    DatasetType,
)
from lsst.daf.butler import StoredDatastoreItemInfo

from .aliases import CollectionName, DatastoreTableName


@final
@dataclasses.dataclass
class DatasetRef:
    """Proxy for the real DatasetRef, but with records added.

    TODO: remove after DM-40053.
    """

    id: uuid.UUID
    datasetType: DatasetType
    dataId: DataCoordinate
    run: CollectionName

    _datastore_records: dict[DatastoreTableName, list[StoredDatastoreItemInfo]] | None = None
    """datastore records associated with this dataset.

    This will be used initially to store FileDatastore metadata records, but it
    could be used to store anything a Datastore likes.
    """
