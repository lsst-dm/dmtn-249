from __future__ import annotations

import dataclasses
import enum
import uuid
from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping, Set
from typing import TYPE_CHECKING, Any, Iterator, NewType, Self, final

import pydantic
from lsst.daf.butler import DataCoordinate, DimensionUniverse, StorageClass
from lsst.resources import ResourcePath
from pydantic_core import core_schema

from .aliases import CollectionName, DatasetTypeName, DimensionName, OpaqueTableName, StorageClassName

if TYPE_CHECKING:
    from .butler import Datastore


class SignedPermissions(enum.Flag):
    """Flag enum for operations a signed URI can support."""

    GET = enum.auto()
    PUT = enum.auto()
    DELETE = enum.auto()


class Checksum(pydantic.BaseModel):
    hex: str
    algorithm: str


class OpaqueRecordSet(ABC):
    @abstractmethod
    @classmethod
    def from_json_data(cls, data: Any) -> Self:
        raise NotImplementedError()

    @abstractmethod
    @classmethod
    def make_empty(cls) -> Self:
        raise NotImplementedError()

    @property
    @abstractmethod
    def tables(self) -> Set[OpaqueTableName]:
        raise NotImplementedError()

    @property
    @abstractmethod
    def datasets(self) -> Set[uuid.UUID]:
        raise NotImplementedError()

    @abstractmethod
    def __bool__(self) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def filter_dataset(self, uuid: uuid.UUID) -> OpaqueRecordSet:
        """Return just the records corresponding to a single dataset."""
        raise NotImplementedError()

    @abstractmethod
    def update(self, other: OpaqueRecordSet) -> None:
        raise NotImplementedError()

    @abstractmethod
    def to_sql_rows(self, table: OpaqueTableName) -> tuple[dict[str, Any], ...]:
        """Extract the records for one table as a tuple of mappings, where each
        mapping corresponds to a row in that table.
        """
        raise NotImplementedError()

    @abstractmethod
    def insert_sql_rows(self, table: OpaqueTableName, rows: Iterable[Mapping[str, Any]]) -> None:
        raise NotImplementedError()

    @abstractmethod
    def to_json_data(self) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def extract_files(self) -> dict[ResourcePath, Checksum | None]:
        """Return any URIs embedded in these records that may need to be
        signed.

        Returns
        -------
        uris : `dict` [ `~lsst.resources.ResourcePath`, `Checksum` | `None` ]
            Unsigned URIs embedded in these records and their checksums, if
            known.
        """
        raise NotImplementedError()

    @abstractmethod
    def add_signed_uris(
        self, permissions: SignedPermissions, signed: Mapping[ResourcePath, ResourcePath]
    ) -> None:
        """Add the given signed URIs to this object.

        Parameters
        ----------
        permissions : `SignedPermissions`
            Operations the signed URIs support.
        signed : `~collections.abc.Mapping` [ `~lsst.resources.ResourcePath`, \
                `~lsst.resources.ResourcePath` ]
            Mapping from unsigned URI to its signed counterpart.
        """
        raise NotImplementedError()

    @classmethod
    def _pydantic_validate(cls, data: Any, info: pydantic.ValidationInfo) -> Self:
        if isinstance(data, cls):
            return data
        context = RepoValidationContext.from_info(info)
        assert issubclass(context.opaque_record_type, cls)
        return context.opaque_record_type.from_json_data(data)

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: pydantic.GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        from_dict_schema = core_schema.chain_schema(
            [
                core_schema.dict_schema(keys_schema=core_schema.str_schema()),
                core_schema.general_plain_validator_function(cls._pydantic_validate),
            ]
        )
        return core_schema.json_or_python_schema(
            json_schema=from_dict_schema,
            python_schema=core_schema.union_schema([core_schema.is_instance_schema(cls), from_dict_schema]),
            serialization=core_schema.plain_serializer_function_ser_schema(cls.to_json_data),
        )


# Opaque records that are guaranteed to correspond to a single dataset.
DatasetOpaqueRecordSet = NewType("DatasetOpaqueRecordSet", OpaqueRecordSet)


class EmptyOpaqueRecordSet(OpaqueRecordSet):
    @classmethod
    def from_json_data(cls, data: Any) -> Self:
        return cls()

    @classmethod
    def make_empty(cls) -> Self:
        return cls()

    @property
    def tables(self) -> Set[OpaqueTableName]:
        return frozenset()

    @property
    def datasets(self) -> Set[uuid.UUID]:
        return frozenset()

    def __bool__(self) -> bool:
        return False

    def filter_dataset(self, uuid: uuid.UUID) -> OpaqueRecordSet:
        raise LookupError("Opaque record set is empty.")

    def update(self, other: OpaqueRecordSet) -> None:
        raise TypeError("Cannot update EmptyOpaqueRecordSet in place.")

    def to_sql_rows(self, table: OpaqueTableName) -> tuple[dict[str, Any], ...]:
        return ()

    def insert_sql_rows(self, table: OpaqueTableName, rows: Iterable[Mapping[str, Any]]) -> None:
        raise TypeError("Cannot update EmptyOpaqueRecordSet in place.")

    def to_json_data(self) -> Any:
        return {}

    def extract_files(self) -> dict[ResourcePath, Checksum | None]:
        return {}

    def add_signed_uris(
        self, permissions: SignedPermissions, signed: Mapping[ResourcePath, ResourcePath]
    ) -> None:
        assert not signed


@final
@dataclasses.dataclass
class RepoValidationContext:
    """Object that must be provided as the ``context`` argument to
    `pydantic.BaseModel.model_validate` (etc) when deserializing various
    primitives that depend on repository configuration.
    """

    universe: DimensionUniverse
    opaque_record_type: type[OpaqueRecordSet]

    @classmethod
    def from_info(cls, info: pydantic.ValidationInfo) -> RepoValidationContext:
        if info.context is None:
            raise ValueError("This object cannot be deserialized without a RepoValidationContext provided.")
        return cls(universe=info.context["universe"], opaque_record_type=info.context["opaque_record_type"])


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
