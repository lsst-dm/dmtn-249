from __future__ import annotations

import dataclasses
import enum
import uuid
from abc import ABC, abstractmethod
from collections.abc import Hashable, Mapping, Set, Iterable
from typing import TYPE_CHECKING, Any, Iterator, final, Dict

import pydantic
from pydantic_core import core_schema

from lsst.daf.butler import DataCoordinate, StorageClass, DimensionUniverse
from lsst.resources import ResourcePath

from .aliases import (
    CollectionName,
    DatasetTypeName,
    DimensionName,
    OpaqueTableName,
    StorageClassName,
)

if TYPE_CHECKING:
    from .butler import Datastore


class SignedPermissions(enum.Flag):
    """Flag enum for operations a signed URI can support."""

    GET = enum.auto()
    PUT = enum.auto()
    DELETE = enum.auto()


class DatasetOpaqueRecordSet(ABC):
    """A set of records that correspond to a single (parent) dataset in a
    single opaque table.

    These sets will *very* frequently hold only a single record (they only need
    to hold more to support disassembled components), and we anticipate a
    concrete subclass optimized for this.

    Concrete subclasses are expected to be dataclasses, Pydantic models, or
    primitives that Pydantic knows how to serialize and validate (i.e.
    ``pydantic.TypeAdapter(instance)`` should work).
    """

    @abstractmethod
    def extract_unsigned_uris(self) -> dict[Hashable, ResourcePath]:
        """Return any URIs embedded in these records that may need to be
        signed.

        Returns
        -------
        uris : `dict` [ `~collections.abc.Hashable`, \
                `~lsst.resources.ResourcePath` ]
            Unsigned URIs embedded in these records.  Key interpretation is
            datastore-specific and opaque the caller.
        """
        raise NotImplementedError()

    @abstractmethod
    def with_signed_uris(
        self, permissions: SignedPermissions, signed: Mapping[Hashable, ResourcePath]
    ) -> DatasetOpaqueRecordSet:
        """Return a record set that holds the given signed URIs as well as its
        original unsigned URIs.

        Parameters
        ----------
        permissions : `SignedPermissions`
            Operations the signed URIs support.
        signed : `~collections.abc.Mapping` [ `~collections.abc.Hashable`, \
                `~lsst.resources.ResourcePath` ]
            Signed URIs with the same keys as the unsigned-URI mapping returned
            by `extract_unsigned_uris`.

        Returns
        -------
        records : `DatasetOpaqueRecordSet`
            Record set that includes the signed URIs.  May be ``self`` if it is
            mutable and was modified in place.  Need not be the same derived
            type as self.
        """
        raise NotImplementedError()

    @abstractmethod
    def to_sql_rows(self, uuid: uuid.UUID) -> tuple[dict[str, Any], ...]:
        """Convert to a tuple of mappings where each mapping corresponds to a
        row in the corresponding SQL table.
        """
        raise NotImplementedError()


@final
@dataclasses.dataclass
class RepoValidationContext:
    """Object that must be provided as the ``context`` argument to
    `pydantic.BaseModel.model_validate` (etc) when deserializing various
    primitives that depend on repository configuration.
    """

    universe: DimensionUniverse
    opaque_record_types: Mapping[OpaqueTableName, type[DatasetOpaqueRecordSet]]

    @classmethod
    def from_info(cls, info: pydantic.ValidationInfo) -> RepoValidationContext:
        if info.context is None:
            raise ValueError("This object cannot be deserialized without a RepoValidationContext provided.")
        return cls(universe=info.context["universe"], opaque_record_types=info.context["opaque_record_types"])


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

    _opaque_records: Mapping[OpaqueTableName, DatasetOpaqueRecordSet] | None = None
    """Opaque records associated with this dataset, keyed by table name.

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

    @classmethod
    def from_mapping(
        cls,
        data: Mapping[str, Any],
        *,
        record_set_types: Mapping[OpaqueTableName, type[DatasetOpaqueRecordSet]] | None = None,
    ) -> DatasetRef:
        raise NotImplementedError(
            "TODO: validate with pydantic but use `record_set_types` for _opaque_records."
        )


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
class OpaqueTableRecordSet(
    pydantic.RootModel[
        Dict[OpaqueTableName, Dict[uuid.UUID, pydantic.SerializeAsAny[DatasetOpaqueRecordSet]]]
    ]
):
    root: Dict[OpaqueTableName, Dict[uuid.UUID, pydantic.SerializeAsAny[DatasetOpaqueRecordSet]]]

    @pydantic.model_validator(mode="before")
    @classmethod
    def _validate(cls, data: Any, info: pydantic.ValidationInfo) -> OpaqueTableRecordSet:
        match data:
            case OpaqueTableRecordSet() as done:
                return done
            case _:
                # Check that the outer structure of the dictionary is what
                # we expect.
                mapping: dict[OpaqueTableName, dict[uuid.UUID, Any]] = pydantic.TypeAdapter(
                    dict[OpaqueTableName, dict[uuid.UUID, Any]]
                ).validate_python(data)
        context = RepoValidationContext.from_info(info)
        # Iterate over nested records and validate/coerce them to the type in
        # the schema.
        for table_name, records_for_table in mapping.items():
            record_adapter: pydantic.TypeAdapter = pydantic.TypeAdapter(
                context.opaque_record_types[table_name]
            )
            for dataset_uuid, serialized in records_for_table.items():
                records_for_table[dataset_uuid] = record_adapter.validate_python(
                    serialized, context=info.context
                )
        return cls.model_construct(data)


@final
@dataclasses.dataclass
class DeferredDatasetHandle:
    """Like the current DeferredDatasetHandle, but with:

    - A datastore instead of a LimitedButler.
    """

    ref: DatasetRef
    datastore: Datastore
