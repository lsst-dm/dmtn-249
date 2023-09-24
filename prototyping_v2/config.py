from __future__ import annotations

__all__ = ("ExtensionConfig", "RegistryConfig", "DatastoreConfig", "StorageClassConfig", "ButlerConfig")

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Self

import pydantic
from lsst.resources import ResourcePath
from lsst.utils.doImport import doImportType
from lsst.utils.introspection import get_full_type_name
from pydantic_core import core_schema

if TYPE_CHECKING:
    from .butler import Datastore, Registry


class ExtensionConfig(ABC):
    """A pydantic-friendly ABC that serializes its fully-qualified type name
    to enable dynamic polymorphism on read.

    Notes
    -----
    Concrete derived classes typically inherit from `pydantic.BaseModel`
    *first* and delegate to pydantic to implement `_from_dict_impl` and
    `_to_dict_impl`::

        class ConcreteConfig(pydantic.BaseModel, ExtensionConfig):

            @classmethod def _from_dict_impl(cls, data: dict[str, Any]) -> Self
                return cls.model_validate(data)

            def _to_dict_impl(self) -> dict[str, Any]:
                return self.model_dump()

    Unfortunately this system does not permit intermediate base classes that
    both have pydantic fields and provide dynamic polymorphism to their derived
    classes; one has to choose between:

    - A non-pydantic ABC that provides dynamic polymorphism when used in a
      pydantic field annotation.  If these types have attributes, they or their
      derived classes must manually implement their serialization in
      `_to_dict_impl` and `_from_dict_impl`.

    - A concrete pydantic model class that type-slices any derived classes when
      used as a pydantic field (as pydantic models usually do).  These should
      generally be marked `~typing.final` to avoid the potential for slicing.
    """

    @classmethod
    def from_dict(cls, data: dict[str, Any] | Self) -> Self:
        if isinstance(data, ExtensionConfig):
            assert isinstance(data, cls)
            return data
        if type_name := data.pop("cls", None):
            persisted_cls: type[ExtensionConfig] = doImportType(type_name)
            assert issubclass(persisted_cls, cls)
            return persisted_cls._from_dict_impl(data)
        raise RuntimeError(f"Could not convert {data}.")

    @classmethod
    @abstractmethod
    def _from_dict_impl(cls, data: dict[str, Any]) -> Self:
        raise NotImplementedError()

    def to_dict(self) -> dict[str, Any]:
        data = self._to_dict_impl()
        data["cls"] = get_full_type_name(self)
        return data

    @abstractmethod
    def _to_dict_impl(self) -> dict[str, Any]:
        raise NotImplementedError()

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: pydantic.GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        from_dict_schema = core_schema.chain_schema(
            [
                core_schema.dict_schema(keys_schema=core_schema.str_schema()),
                core_schema.no_info_plain_validator_function(ExtensionConfig.from_dict),
            ]
        )
        return core_schema.json_or_python_schema(
            json_schema=from_dict_schema,
            python_schema=core_schema.union_schema([core_schema.is_instance_schema(cls), from_dict_schema]),
            serialization=core_schema.plain_serializer_function_ser_schema(cls.to_dict),
        )


class RegistryConfig(ExtensionConfig):
    @classmethod
    @abstractmethod
    def make_registry(cls, root: ResourcePath) -> Registry:
        raise NotImplementedError()


class DatastoreConfig(ExtensionConfig):
    @classmethod
    @abstractmethod
    def make_datastore(cls, root: ResourcePath) -> Datastore:
        raise NotImplementedError()


class StorageClassConfig(pydantic.BaseModel):
    pytype: str
    converters: list[dict[str, str]] = pydantic.Field(default_factory=list)
    delegate: str | None = None
    parameters: list[str] = pydantic.Field(default_factory=list)
    inheritsFrom: str | None = None
    components: dict[str, str] = pydantic.Field(default_factory=dict)
    derivedComponents: dict[str, str] = pydantic.Field(default_factory=dict)


class ButlerConfig(pydantic.BaseModel):
    registry: RegistryConfig
    datastore: DatastoreConfig
    storageClasses: dict[str, StorageClassConfig]
    workspaceRoot: ResourcePath
    workspaceOptions: dict[str, Any]
