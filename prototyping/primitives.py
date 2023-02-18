from __future__ import annotations

import dataclasses
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
    datastore_records: Mapping[OpaqueTableName, Iterable[OpaqueTableRow]]
