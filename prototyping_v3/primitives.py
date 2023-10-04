from __future__ import annotations

import dataclasses
import enum
import uuid
from collections.abc import Iterable, Set
from typing import Any, Iterator, final, TYPE_CHECKING

import pydantic
from lsst.daf.butler import DataCoordinate, DimensionUniverse, StorageClass
from lsst.resources import ResourcePath
from pydantic_core import core_schema

from .aliases import (
    CollectionName,
    DatasetTypeName,
    DimensionName,
    StorageClassName,
)
from .opaque import DatasetOpaqueRecordSet, RepoValidationContext


if TYPE_CHECKING:
    from .datastore import Datastore


class Permissions(enum.Flag):
    """Flag enum for operations a signed URI can support."""

    NONE = 0
    GET = enum.auto()
    PUT = enum.auto()
    DELETE = enum.auto()


@final
class DimensionGroup(Set[DimensionName]):
    """Placeholder for DimensionGraph replacement approved on RFC-834.

    Note that this satisfies `Iterable[str]`, which is how high-level
    interfaces will usually accept it, in order to allow users to also pass
    built-in iterables.
    """

    def __init__(self, names: Iterable[str], universe: DimensionUniverse):
        self._names = universe.extract(names).dimensions.names
        self._universe = universe

    def __hash__(self) -> int:
        return hash(tuple(self._names))

    def __contains__(self, x: object) -> bool:
        return x in self._names

    def __iter__(self) -> Iterator[DimensionName]:
        return iter(self._names)

    def __len__(self) -> int:
        return len(self._names)

    @classmethod
    def _validate(cls, data: Any, info: pydantic.ValidationInfo) -> DimensionGroup:
        if isinstance(data, cls):
            return data
        context = RepoValidationContext.from_info(info)
        return cls(data, context.universe)

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: pydantic.GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        from_list_schema = core_schema.chain_schema(
            [
                core_schema.list_schema(core_schema.str_schema()),
                core_schema.general_plain_validator_function(cls._validate),
            ]
        )
        return core_schema.json_or_python_schema(
            json_schema=from_list_schema,
            python_schema=core_schema.union_schema([core_schema.is_instance_schema(cls), from_list_schema]),
            serialization=core_schema.plain_serializer_function_ser_schema(list),
        )


@final
@dataclasses.dataclass
class DatasetType:
    """Placeholder for the current DatasetType, but with:

    - a DimensionGroup instead of a DimensionGraph;
    - snake case for consistency with the rest of the prototyping.
    """

    name: DatasetTypeName
    dimensions: DimensionGroup
    storage_class_name: StorageClassName
    is_calibration: bool

    @property
    def storage_class(self) -> StorageClass:
        raise NotImplementedError("TODO")

    def override_storage_class(self, storage_class: StorageClass | StorageClassName) -> DatasetType:
        raise NotImplementedError("TODO")


@final
@dataclasses.dataclass
class DatasetRef:
    """Like the current DatasetRef, but with:

    - ``id`` renamed to ``uuid`` (more obviously distinct from data ID);
    - snake case for consistency with the rest of the prototyping.
    """

    uuid: uuid.UUID
    dataset_type: DatasetType
    data_id: DataCoordinate
    run: CollectionName

    _opaque_records: DatasetOpaqueRecordSet | None = None
    """Opaque records associated with this dataset.

    This will be used initially to store FileDatastore metadata records, but it
    could be used to store anything a Datastore likes.
    """

    def get_uri(self) -> ResourcePath:
        """Return the URI for this dataset, and raise if there is more than
        one.
        """
        (uri,) = self.get_all_uris()
        return uri

    def get_all_uris(self) -> list[ResourcePath]:
        """Return all URIs for this dataset.

        Returns
        -------
        uris: `list` [ `lsst.resources.ResourcePath` ]
            URIs for the dataset.
        """
        raise NotImplementedError("TODO: call `extract_unsigned_uris` on opaque_records.")


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
