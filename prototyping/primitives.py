from __future__ import annotations

import dataclasses
from collections.abc import Iterable, Mapping, Set
from typing import TYPE_CHECKING
from uuid import UUID

from lsst.daf.butler import DataCoordinate, DatasetType

if TYPE_CHECKING:
    from .aliases import CollectionName, DimensionName, OpaqueTableName, OpaqueTableRow


class DimensionGroup(Set[DimensionName]):
    """Replacement for DimensionGraph approved on RFC-834.

    Note that this satisfies `Iterable[str]`, which is how high-level
    interfaces will usually accept it, in order to allow users to also pass
    built-in iterables.
    """


@dataclasses.dataclass
class DatasetRef:
    uuid: UUID
    dataset_type: DatasetType
    data_id: DataCoordinate
    run: CollectionName
    datastore_records: Mapping[OpaqueTableName, Iterable[OpaqueTableRow]]
