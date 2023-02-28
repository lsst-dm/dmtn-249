from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Mapping
from typing import Any
from uuid import UUID

from lsst.daf.butler import ddl

from .aliases import ColumnName, OpaqueTableName, OpaqueTableValues
from .primitives import DatasetRef, OpaqueTableBatch


class DatastoreBridge(ABC):
    """Object that represents the definition of an opaque table.

    Datastores construct these to describe what they need; Butler takes them
    from Datastore and gives them to Registry; Registry holds them and uses
    them to create and query tables without caring about the details.  This
    means they'll need to be serializable so the remote Registry client can
    send them to its server.

    I'm still thinking vaguely about opaque tables being used by things other
    than Datastore in the future (e.g. if we ever get around to
    storage-class-specific metadata tables).  But I think I'm willing to lock
    us into dataset UUIDs always being [part of] the primary key for these
    tables to try to reduce complexity, and I'm okay with this being the only
    way to define opaque tables for now.

    This class's methods interface with an  ``OpaqueTableValues`` alias that's
    really just `typing.Any` under the hood (see docstring in aliases.py).  I'm
    using `Any` not just to avoid being overly specific here: this is a type
    erasure pattern, in which each OpaqueTableDefinition implementation
    probably has its own preferred type for OpaqueTableValues, but nothing else
    cares what it is.  Using `Any` avoids a lot of casts, and not using `Any`
    (but casting all over the place) isn't any type-safer.
    """

    @property
    @abstractmethod
    def tables(self) -> Mapping[OpaqueTableName, ddl.TableSpec]:
        raise NotImplementedError()

    @abstractmethod
    def opaque_values_to_raw(
        self, table_name: OpaqueTableName, values: Mapping[UUID, OpaqueTableValues]
    ) -> Iterable[dict[ColumnName, Any]]:
        """Convert rows from the OpaqueTableValues type used for this opaque
        table to dictionaries of built-ins that can be passed directly to
        SQLAlchemy.

        Output rows should only contain unsigned (original) URIs.  If input
        rows contain signed URIs as well, they will be ignored.
        """
        raise NotImplementedError()

    @abstractmethod
    def opaque_raw_to_values(
        self, table_name: OpaqueTableName, rows: Iterable[Mapping[ColumnName, Any]]
    ) -> dict[UUID, OpaqueTableValues]:
        """Convert from an iterable of SQLAlchemy-friendly mappings to a
        mapping of OpaqueTableValues keyed by UUID.
        """
        raise NotImplementedError()

    @abstractmethod
    def sign_opaque_values(
        self,
        table_name: OpaqueTableName,
        values: dict[UUID, OpaqueTableValues],
        callback: Callable[[UUID], UUID],
    ) -> None:
        """Add signed versions of any URIs present in the given values using
        the given callback.

        This modifies values in-place, which is why it takes a `dict` instead
        of a `Mapping`.

        The server side of a RemoteRegistry implementation would be responsible
        for checking that the user has the appropriate permissions for any
        particular request, but it does not know where in the Datastore's
        records URIs might appear (since those records are by design opaque to
        it).
        """
        raise NotImplementedError()

    @abstractmethod
    def make_opaque_values(self, refs: Iterable[DatasetRef]) -> OpaqueTableBatch:
        """Make opaque-table values for a new datasets to be written to this
        Datastore.

        This is responsible for generating filename templates and anything else
        needed for read operations.  Fields that can only be populated after
        files are written should be left blank.  All embedded URIs should be
        unsigned; `sign_opaque_values` will be called separatedly to add signed
        URLS.
        """
        raise NotImplementedError()
