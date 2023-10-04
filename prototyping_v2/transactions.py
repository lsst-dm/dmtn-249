from __future__ import annotations

__all__ = ()

import uuid
from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import Any, Self, final

import pydantic

from lsst.resources import ResourcePath

from .aliases import CollectionName, ArtifactTransactionID, ArtifactTransactionName
from .butler import Datastore, Registry
from .config import ExtensionConfig
from .raw_batch import RawBatch


class ArtifactTransaction(ABC):
    # TODO: add hook for creation from client-side data.

    @classmethod
    @abstractmethod
    def load(cls, header: ArtifactTransactionHeader, workspace_root: ResourcePath | None) -> Self:
        raise NotImplementedError()

    @abstractmethod
    def abandon(self, datastore: Datastore) -> None:
        raise NotImplementedError()

    @abstractmethod
    def commit(self, datastore: Datastore, batch: RawBatch) -> None:
        raise NotImplementedError()

    def make_client(self, datastore: Datastore) -> Any:
        return None


@final
class EmptyArtifactTransaction(ArtifactTransaction):
    @classmethod
    def load(cls, header: ArtifactTransactionHeader, workspace_root: ResourcePath | None) -> Self:
        return cls()

    def abandon(self, datastore: Datastore) -> None:
        pass

    def check_abandoned(self) -> None:
        pass

    def commit(self, datastore: Datastore, batch: RawBatch) -> None:
        pass

    def check_committed(self) -> None:
        pass


class ArtifactTransactionHeader(pydantic.BaseModel):
    name: ArtifactTransactionName
    id: ArtifactTransactionID
    cls: type[ArtifactTransaction] = pydantic.Field(
        default=EmptyArtifactTransaction
    )  # TODO: serialize/deserialize type to/from str
    data: dict[str, Any] = pydantic.Field(default_factory=dict)


class ArtifactTransactionManager(ABC):
    """Butler component that provides transactions that keep datastore and
    registry content consistent.

    Notes
    -----
    We anticipate two implementations of this class:

    - `DirectArtifactTransactionManager` has no server component of its own,
      and is appropriate for clients that are trusted to maintain data
      repository invariants.  This is what's used for personal SQLite repos
      with no server component whatsoever, but it's also what trusted clients
      of a major repository would use - not just administrators but processes
      running task execution directly against a central-repository datastore as
      well. This manager could accept any `ArtifactTransaction` subclass it can
      import.

    - `ClientServerArtifactTransactionManager` has a server component and
      permits only a handful of built-in concrete `ArtifactTransaction` types
      that are recognized by the server.  The `~ArtifactTransaction.abandon`
      and `~ArtifactTransaction.commit` methods of these transactions will
      never be called directly by this manager; instead the manager will
      partially reimplement those operations for these transactions itself, by
      delegating to other methods they provide in order to allow the client to
      perform datastore write operations while still allowing the server to
      verify their completion.
    """

    @abstractmethod
    def open(
        self,
        cls: type[ArtifactTransaction],
        data: dict[str, Any],
        runs: Iterable[CollectionName],
        name: ArtifactTransactionName | None = None,
        unstores: Iterable[uuid.UUID] = (),
    ) -> ArtifactTransactionID:
        """Open a persistent transaction in this repository.

        Parameters
        ----------
        cls
            Transaction type object.
        data
            Serialized representation of the initial state of the transaction.
        runs
            `~CollectionType.RUN` collections whose datastore artifacts may be
            modified by this transaction. No other transaction may target any
            of these collections until this transaction is committed or
            abandoned.
        name
            Name of the transaction.  Should be prefixed with ``u/$USER`` for
            regular users.  If not provided, defaults to something unique with
            that prefix.
        unstores
            Datasets whose opaque records should be dropped from the registry
            when the transaction is opened, yielding a transaction that would
            fully delete them if abandoned.

        Returns
        -------
        transaction_id
            ID for the new transaction.

        Notes
        -----
        After validating the arguments (including constructing the
        `ArtifactTransaction` instance it will eventually return), the manager
        assembles a `ArtifactTransactionHeader` and saves it to manager-defined
        location.  It then performs several database operations in a single
        transaction:

        - It inserts a row into a ``artifact_transaction`` table, generating
          the transaction ID via its autoincrement primary key.

        - It inserts rows into the ``artifact_transaction_run`` table,
          effectively locking those runs against writes from other artifact
          transactions (but not database transactions involving registry-only
          state).  This fails (due to unique constraints in
          ``artifact_transaction_run``) if those runs are already locked.

        - It removes all opaque records associated with the datasets in
          ``unstored``.

        If the database commit fails or is never attempted, the manager should
        attempt to remove the header it just saved, but we cannot count on this
        reliably occurring. Transaction managers must ignore headers with no
        associated database entry until given an opportunity to clean them up
        via `vacuum`.
        """
        raise NotImplementedError()

    @abstractmethod
    def commit(self, transaction_id: ArtifactTransactionID) -> None:
        """Finalize changes to the central repository made by the given
        transaction and close it.

        Notes
        -----
        The direct implementation:

        - loads the identified `ArtifactTransaction`;
        - calls `ArtifactTransaction.commit`;
        - calls `Registry.execute_batch` and removes the transaction database
          entries (in a single database commit);
        - deletes the transaction header and (if there is one) the workspace
          root and all of its contents.

        In the client/server implementation,

        - the server loads the identified `ArtifactTransaction` from the header
          and obtains write-signed URIs for the artifacts it identifies,
          returning them to the client;
        - the client performs the first part of `commit`, in which artifacts
          are actually written to the datastore;
        - the server verifies that the expected artifacts are present, and
          then performs all remaining steps (executing the registry batch and
          cleaning up).
        """
        raise NotImplementedError()

    @abstractmethod
    def abandon(self, transaction_id: ArtifactTransactionID) -> None:
        """Revert changes to the central repository made by the given
        transaction, and then close it.

        Notes
        -----
        The direct implementation:

        - loads the identified `ArtifactTransaction`;
        - calls `ArtifactTransaction.abandon`;
        - removes the transaction database entries;
        - deletes the transaction header and (if there is one) the workspace
          root and all of its contents.

        In the client/server implementation,

        - the server loads the identified `ArtifactTransaction` from the header
          and obtains delete-signed URIs for the artifacts it identifies,
          returning them to the client;
        - the client performs the first part of `commit`, in which artifacts
          are actually deleted from the datastore;
        - the server verifies that the expected artifacts are gone and then
          removes the transaction database entries, headers, and workspace
          roots.
        """
        raise NotImplementedError()

    @abstractmethod
    def list_active(self) -> list[tuple[str, int]]:
        """Return the names and IDs of all active transactions that
        the current user has write access to.

        Notes
        -----
        Administrators are expected to use this to check that there are no
        active artifact transactions before any migration or change to central
        datastore configuration.
        """
        raise NotImplementedError()

    @abstractmethod
    def vacuum(self) -> None:
        """Clean up any empty directories and persisted transaction headers not
        associated with an active transaction.
        """
        raise NotImplementedError()

    @abstractmethod
    def make_client(self, *, uri: ResourcePath | None = None) -> Any:
        raise NotImplementedError()


class ArtifactTransactionManagerConfig(ExtensionConfig):
    @abstractmethod
    def make_transaction_manager(
        self, root: ResourcePath, registry: Registry, datastore: Datastore
    ) -> ArtifactTransactionManager:
        raise NotImplementedError()

    @abstractmethod
    def get_transaction_client(
        self, root: ResourcePath, datastore: Datastore, name: ArtifactTransactionName
    ) -> Any:
        # Expected implementation shown below, with "..." indicating dependence
        # on derived-class fields.
        workspace_root = root.join(...).join(name, forceDirectory=True)  # type: ignore
        with workspace_root.join("header.json").open("r") as stream:
            header_json = stream.read()
        header = ArtifactTransactionHeader.model_validate_json(header_json)
        return header.cls.load(header, workspace_root).make_client(datastore)
