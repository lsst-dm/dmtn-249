from __future__ import annotations

from abc import ABC, abstractmethod
import dataclasses
import enum
from collections.abc import Hashable, Mapping, Set
from typing import TYPE_CHECKING, Any
from uuid import UUID

import pydantic
from lsst.daf.butler import DataCoordinate, StorageClass
from lsst.resources import ResourcePath

if TYPE_CHECKING:
    from .aliases import (
        CollectionName,
        DatasetTypeName,
        DimensionName,
        StorageClassName,
        OpaqueTableName,
    )
    from .butler import Datastore


class DimensionGroup(Set[DimensionName]):
    """Placeholder for DimensionGraph replacement approved on RFC-834.

    Note that this satisfies `Iterable[str]`, which is how high-level
    interfaces will usually accept it, in order to allow users to also pass
    built-in iterables.
    """


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
        raise NotImplementedError()


@dataclasses.dataclass
class DatasetRef:
    """Like the current DatasetRef, but with:

    - ``id`` renamed to ``uuid`` (more obviously distinct from data ID);
    - snake case for consistency with the rest of the prototyping.
    """

    uuid: UUID
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
        record_set_types: Mapping[OpaqueTableName, type[DatasetOpaqueRecordSet]] | None,
    ) -> DatasetRef:
        raise NotImplementedError(
            "TODO: validate with pydantic but use `record_set_types` for _opaque_records."
        )


class SignedPermissions(enum.Flag):
    """Flag enum for operations a signed URI can support."""

    GET = enum.auto()
    PUT = enum.auto()
    DELETE = enum.auto()


class DatasetOpaqueRecordSet(pydantic.BaseModel, ABC):
    """A set of records that correspond to a single (parent) dataset
    in a single opaque table.

    These sets will *very* frequently hold only a single record (they only need
    to hold more to support disassembled components), and we anticipate a
    concrete subclass optimized for this.
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
    def to_sql_rows(self) -> tuple[dict[str, Any], ...]:
        """Convert to a tuple of mappings where each mapping corresponds to a
        row in the corresponding SQL table.
        """
        raise NotImplementedError()


class OpaqueTableDefinition:
    """Definition of an opaque table."""

    name: OpaqueTableName
    record_set_type: type[DatasetOpaqueRecordSet]


@dataclasses.dataclass
class DeferredDatasetHandle:
    """Like the current DeferredDatasetHandle, but with:

    - A datastore instead of a LimitedButler.
    """

    ref: DatasetRef
    datastore: Datastore
