from __future__ import annotations

import dataclasses
import enum
import uuid
from typing import final, TYPE_CHECKING

from lsst.daf.butler import (
    DataCoordinate,
    DatasetType,
)
from lsst.daf.butler import StoredDatastoreItemInfo

from .aliases import CollectionName, OpaqueTableName


if TYPE_CHECKING:
    from .datastore import Datastore


@final
@dataclasses.dataclass
class DatasetRef:
    """Proxy for the real DatasetRef, but with records added.

    TODO: remove after DM-40053
    """

    id: uuid.UUID
    datasetType: DatasetType
    dataId: DataCoordinate
    run: CollectionName

    _opaque_records: dict[OpaqueTableName, list[StoredDatastoreItemInfo]] | None = None
    """Opaque records associated with this dataset.

    This will be used initially to store FileDatastore metadata records, but it
    could be used to store anything a Datastore likes.
    """


class SequenceEditMode(enum.Enum):
    """Enum for edit operations on sequences."""

    ASSIGN = enum.auto()
    REMOVE = enum.auto()
    EXTEND = enum.auto()
    PREPEND = enum.auto()


class SetInsertMode(enum.Enum):
    """Enum for insert operations on sets."""

    INSERT_OR_FAIL = enum.auto()
    INSERT_OR_SKIP = enum.auto()
    INSERT_OR_REPLACE = enum.auto()


class SetEditMode(enum.Enum):
    """Enum for edit operations on sets."""

    INSERT_OR_FAIL = SetInsertMode.INSERT_OR_FAIL
    INSERT_OR_SKIP = SetInsertMode.INSERT_OR_SKIP
    INSERT_OR_REPLACE = SetInsertMode.INSERT_OR_REPLACE
    ASSIGN = enum.auto()
    REMOVE = enum.auto()
    DISCARD = enum.auto()


@final
@dataclasses.dataclass
class DeferredDatasetHandle:
    """Like the current DeferredDatasetHandle, but with:

    - A datastore instead of a LimitedButler.
    """

    ref: DatasetRef
    datastore: Datastore
