from __future__ import annotations

__all__ = ("ArtifactTransaction", "ArtifactTransactionHeader")

import enum
import uuid
from abc import ABC, abstractmethod
from collections.abc import Mapping, Set
from typing import Any, Self

import pydantic

from lsst.daf.butler import StoredDatastoreItemInfo
from lsst.resources import ResourcePath
from lsst.utils.doImport import doImportType
from lsst.utils.introspection import get_full_type_name

from .aliases import ArtifactTransactionName, CollectionName, OpaqueTableName, StorageURI
from .butler import Datastore
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
        irreversibly deleted).
        """
        return frozenset()

    @abstractmethod
    def get_uris(self, datastore: Datastore) -> list[StorageURI]:
        """Return possibly-relative URIs that need to be turned into
        possibly-signed and definitely absolute URLs to pass to `run`.
        """
        raise NotImplementedError()

    @abstractmethod
    def run_phase_one(
        self,
        resolution: ArtifactTransactionResolution,
        datastore: Datastore,
        paths: Mapping[StorageURI, ResourcePath],
    ) -> dict[OpaqueTableName, list[StoredDatastoreItemInfo]]:
        """Begin to commit or abandon this transaction.

        This method will always be called by the client.

        Parameters
        ----------
        resolution : `ArtifactTransactionResolution`
            Whether to commit or abandon.
        datastore : `Datastore`
            Datastore client for this data repository.
        paths : `~collections.abc.Mapping` [ `str`, \
                `~lsst.resources.ResourcePath` ]
            Mapping from unsigned possibly-relative URI to absolute
            possibly-signed URI.  Keys are the same as those returned by
            `get_uris` for the resolution.

        Returns
        -------
        final_batch : `RawBatch`
            Database operations to apply when the transaction is closed.
        records : `OpaqueRecordSet`
            Opaque records to insert into the registry when the transaction is
            closed.
        """
        raise NotImplementedError()

    @abstractmethod
    def run_phase_two(
        self,
        resolution: ArtifactTransactionResolution,
        datastore: Datastore,
        paths: Mapping[StorageURI, ResourcePath],
        records: dict[OpaqueTableName, list[StoredDatastoreItemInfo]],
    ) -> RawBatch:
        """Verify that the datastore artifact state is consistent with the
        given records for those records.

        This method will be called on the server in `RemoteButler`.

        See `run_phase_one` for parameters.  Records are those returned by
        `run_phase_one`, and may be modified or just verified by this method.
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
