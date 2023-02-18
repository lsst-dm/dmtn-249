from __future__ import annotations

import dataclasses
import enum
from collections.abc import Iterable, Mapping, Set
from typing import TYPE_CHECKING
from uuid import UUID

from lsst.daf.butler import DataCoordinate, StorageClass

if TYPE_CHECKING:
    from .aliases import (
        CollectionName,
        DatasetTypeName,
        DimensionName,
        OpaqueTableName,
        OpaqueTableRow,
        StorageClassName,
    )


class SequenceEditMode(enum.Enum):
    ASSIGN = enum.auto()
    REMOVE = enum.auto()
    EXTEND = enum.auto()
    PREPEND = enum.auto()


class SetInsertMode(enum.Enum):
    INSERT_OR_FAIL = enum.auto()
    INSERT_OR_SKIP = enum.auto()
    INSERT_OR_REPLACE = enum.auto()


class SetEditMode(enum.Enum):
    INSERT_OR_FAIL = SetInsertMode.INSERT_OR_FAIL
    INSERT_OR_SKIP = SetInsertMode.INSERT_OR_SKIP
    INSERT_OR_REPLACE = SetInsertMode.INSERT_OR_REPLACE
    ASSIGN = enum.auto()
    REMOVE = enum.auto()
    DISCARD = enum.auto()


class DimensionGroup(Set[DimensionName]):
    """Replacement for DimensionGraph approved on RFC-834.

    Note that this satisfies `Iterable[str]`, which is how high-level
    interfaces will usually accept it, in order to allow users to also pass
    built-in iterables.
    """


@dataclasses.dataclass
class DatasetType:
    """Like the current DatasetType, but:

    - it has a DimensionGroup instead of a DimensionGraph;
    - snake case (*maybe* worth; these differences aren't as visible as those
      in DatasetRef).
    """

    name: DatasetTypeName
    dimensions: DimensionGroup
    storage_class_name: StorageClassName
    is_calibration: bool

    @property
    def storage_class(self) -> StorageClass:
        raise NotImplementedError()


@dataclasses.dataclass
class DatasetRef:
    """Like the current DatasetRef, but:

    - it can hold datastore records;
    - id renamed to uuid (more obviously distinct from data ID);
    - snake case (probably not a good after the prototype).
    """

    uuid: UUID
    dataset_type: DatasetType
    data_id: DataCoordinate
    run: CollectionName

    # Might want to consider another (outermost) mapping layer here for
    # something like a secure hash of the datastore config (including roots).
    # I think just opaque table name is enough to work within a single repo,
    # since even a chained datastore can delegate to its members to pull out
    # what they need based on those table names.  But things get hairy when
    # multiple related Datastores (e.g. some central repo and an exported
    # subset thereof) get involved in transfers.
    #
    # Also worth considering whether this should be package private or public.
    # It's important that both Registry and Datastore access it, so it can't be
    # class private.
    datastore_records: Mapping[OpaqueTableName, Iterable[OpaqueTableRow]]
