from __future__ import annotations

import dataclasses
import uuid
from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping, Set
from typing import Any, NewType, Self, final

import pydantic
from lsst.daf.butler import DimensionUniverse
from lsst.resources import ResourcePath
from pydantic_core import core_schema

from .aliases import OpaqueTableName, ColumnName


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
    def filter_dataset(self, uuid: uuid.UUID) -> DatasetOpaqueRecordSet:
        """Return just the records corresponding to a single dataset."""
        raise NotImplementedError()

    @abstractmethod
    def update(self, other: OpaqueRecordSet) -> None:
        raise NotImplementedError()

    @abstractmethod
    def to_sql_rows(self, table: OpaqueTableName) -> tuple[dict[ColumnName, Any], ...]:
        """Extract the records for one table as a tuple of mappings, where each
        mapping corresponds to a row in that table.
        """
        raise NotImplementedError()

    @abstractmethod
    def insert_sql_rows(self, table: OpaqueTableName, rows: Iterable[Mapping[ColumnName, Any]]) -> None:
        raise NotImplementedError()

    @abstractmethod
    def to_json_data(self) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def extract_artifact(self) -> dict[ResourcePath, Checksum | None]:
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
    def add_signed_uris(self, signed: Mapping[ResourcePath, ResourcePath]) -> None:
        """Add the given signed URIs to this object.

        Parameters
        ----------
        signed : `~collections.abc.Mapping` [ `~lsst.resources.ResourcePath`, \
                `~lsst.resources.ResourcePath` ]
            Mapping from unsigned URI to its signed counterpart.  May include
            entries irrelevant to these records that should be ignored.
        """
        raise NotImplementedError()

    @classmethod
    def _pydantic_validate(cls, data: Any, info: pydantic.ValidationInfo) -> Self:
        if isinstance(data, cls):
            return data
        context = RepoValidationContext.from_info(info)
        assert issubclass(context.opaque_record_set_type, cls)
        return context.opaque_record_set_type.from_json_data(data)

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

    def filter_dataset(self, uuid: uuid.UUID) -> DatasetOpaqueRecordSet:
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

    def add_signed_uris(self, signed: Mapping[ResourcePath, ResourcePath]) -> None:
        assert not signed


@final
@dataclasses.dataclass
class RepoValidationContext:
    """Object that must be provided as the ``context`` argument to
    `pydantic.BaseModel.model_validate` (etc) when deserializing various
    primitives that depend on repository configuration.
    """

    universe: DimensionUniverse
    opaque_record_set_type: type[OpaqueRecordSet]
    dataset_opaque_record_set_type: type[DatasetOpaqueRecordSet]

    @classmethod
    def from_info(cls, info: pydantic.ValidationInfo) -> RepoValidationContext:
        if info.context is None:
            raise ValueError("This object cannot be deserialized without a RepoValidationContext provided.")
        return cls(
            universe=info.context["universe"],
            opaque_record_set_type=info.context["opaque_record_set_type"],
            dataset_opaque_record_set_type=info.context["dataset_opaque_record_set_type"],
        )
