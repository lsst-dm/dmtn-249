from __future__ import annotations

__all__ = ("ArtifactTransaction", "ArtifactTransactionHeader")

import enum
import uuid
from abc import ABC, abstractmethod
from collections.abc import Mapping, Set
from typing import Any, Self

import pydantic

from lsst.resources import ResourcePath
from lsst.utils.doImport import doImportType
from lsst.utils.introspection import get_full_type_name

from .aliases import ArtifactTransactionName, CollectionName
from .butler import Datastore
from .opaque import OpaqueRecordSet
from .primitives import Permissions
from .raw_batch import RawBatch


class ArtifactTransactionResolution(enum.Enum):
    COMMIT = enum.auto()
    ABANDON = enum.auto()


class ArtifactTransaction(ABC):
    @classmethod
    @abstractmethod
    def from_header_data(cls, header_data: Any, workspace_root: ResourcePath | None) -> Self:
        """Reconstruct this transaction from its header data and an optiona;
        workspace root.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_header_data(self) -> Any:
        """Extract JSON-compatible data (e.g. `dict` of built-in types) that
        can be used to reconstruct this transaction.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_operation_name(self) -> str:
        """Return a human-readable name for the type of transaction.

        This is used to form the user-visible transaction name when none is
        provided.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_runs(self) -> Set[CollectionName]:
        """Return the `~CollectionType.RUN` collections whose dataset artifacts
        may be modified by this transaction.
        """
        raise NotImplementedError()

    def get_initial_batch(self) -> RawBatch:
        """Return a batch of registry operations to perform when beginning the
        transaction.
        """
        raise NotImplementedError()

    def get_unstores(self) -> Set[uuid.UUID]:
        """Return the UUIDs of datasets whose artifacts may be removed by this
        transaction.

        Since opaque record presence implies artifact existence, opaque records
        need to be removed at the start of an artifact transaction and
        re-inserted only if the transaction is *successfully* abandoned (which
        may not always be possible, e.g. if artifacts have already been
        irreversably deleted).
        """
        return frozenset()

    @abstractmethod
    def get_uris(self, resolution: ArtifactTransactionResolution) -> dict[ResourcePath, Permissions]:
        """Return a mapping of URIs that need be signed to pass to `run`."""
        raise NotImplementedError()

    @abstractmethod
    def run(
        self,
        resolution: ArtifactTransactionResolution,
        datastore: Datastore,
        uris: Mapping[ResourcePath, ResourcePath],
        verify: bool = False,
    ) -> tuple[RawBatch, OpaqueRecordSet, bool]:
        """Commit or abandon this transaction.

        Parameters
        ----------
        resolution : `ArtifactTransactionResolution`
            Whether to commit or abandon.
        datastore : `Datastore`
            Datastore client for this data repository.
        uris : `~collections.abc.Mapping` [ `~lsst.resources.ResourcePath`, \
                `~lsst.resources.ResourcePath` ]
            Mapping from unsigned URI to signed URI.  Keys are the same as
            those returned by `get_uris` for the resolution, and values have
            the permissions specified there.  Values are the same as keys when
            signing is unnecessary.
        verify : `bool`, optional
            If `False`, do not spend extra time verifying that datastore
            artifact state is consistent with the returned opaque records,
            because the caller guarantees that `verify` will be
            called before any database changes are made.  This is typically
            passed on the client side in client/server butlers, since the
            verification needs to be done on the server side anyway.
            If `True`, either do this verification as part of executing the
            transaction or call `verify` before returning.

        Returns
        -------
        final_batch : `RawBatch`
            Database operations to apply when the transaction is closed.
        records : `OpaqueRecordSet`
            Opaque records to insert into the registry when the transaction
            is closed.
        verified : `bool`
            Whether the datastore artifact state was actually verified to be
            consistent with the returned opaque records.  This must be `True`
            if ``verify=True``, but it may be `True` even if ``verify=False``
            if the natural implementation of `run` verifies as a side-effect.
        """
        raise NotImplementedError()

    @abstractmethod
    def verify(
        self,
        resolution: ArtifactTransactionResolution,
        datastore: Datastore,
        uris: Mapping[ResourcePath, ResourcePath],
        records: OpaqueRecordSet,
    ) -> None:
        """Verify that the datastore artifact state is consistent with the
        given records for those records.

        See `run` for parameters, but note that signed URI permissions are
        always just `SignedPermissions.GET` here, since this method should
        not modify any persistent state.
        """
        raise NotImplementedError()


class ArtifactTransactionHeader(pydantic.BaseModel):
    name: ArtifactTransactionName
    type_name: str
    data: Any

    def make_transaction(self, workspace_root: ResourcePath | None) -> ArtifactTransaction:
        transaction_type: type[ArtifactTransaction] = doImportType(self.type_name)
        return transaction_type.from_header_data(self.data, None)

    @classmethod
    def from_transaction(cls, transaction: ArtifactTransaction, name: ArtifactTransactionName) -> Self:
        return cls(name=name, type_name=get_full_type_name(transaction), data=transaction.get_header_data())
