from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from lsst.daf.butler import DimensionUniverse
from lsst.resources import ResourcePath

from .aliases import InMemoryDataset, ArtifactTransactionName, CollectionName

from .butler import Butler
from .datastore import Datastore
from .opaque import OpaqueRecordSet
from .primitives import DatasetRef, Permissions
from .raw_batch import RawBatch
from .transactions import ArtifactTransaction, ArtifactTransactionHeader, ArtifactTransactionResolution


class PermissionsError(RuntimeError):
    pass


class User:
    pass


class _ButlerServer:
    """An object that stands in for the butler server when prototyping the
    client implementation.

    Calls to the public methods of this class from outside this class represent
    calls to the the server (so arguments and return types need to be
    serializable).  Implementations of the methods on this class represent
    server-side logic.

    This type doesn't need to exist in the real implementation - it exists here
    only to signal delegation to the server.
    """

    _root: ResourcePath
    _datastore: Datastore

    def begin_transaction(self, user: User, header: ArtifactTransactionHeader) -> bool:
        if not self._can_run_transaction(user, header.type_name, header.name):
            raise PermissionError()
        transaction: ArtifactTransaction = header.make_transaction(None)
        runs = transaction.get_runs()
        for run in runs:
            if not self._collection_permissions(user, run) & Permissions.PUT:
                raise PermissionsError()
        for ref in self._sql("SELECT DatasetRef WHERE uuid IN {transaction.unstores}"):
            if ref.run in runs:
                raise RuntimeError(f"Malformed transaction: {ref} is not in {runs}.")
            if not self._collection_permissions(user, run) & Permissions.DELETE:
                raise PermissionsError()
        initial_batch = transaction.get_initial_batch()
        if not self._can_execute(user, initial_batch):
            raise PermissionError()
        with self._root.join("transactions").join(header.name + ".json").open("w") as stream:
            header_json = header.model_dump_json()
            stream.write(header_json)
        self._sql(
            # We want this first insert to fail if the name already exists and
            # the digest of header_json matches, stop early and return False.
            # If the name exists and the digest is not the same, or if the run
            # is already present anywhere in the table, we attempt to delete
            # the JSON file we just wrote and return the error to the user.
            "INSERT INTO artifact_transaction (name, digest) VALUES ({name}, {digest(header_json)})",
            "INSERT INTO artifact_transaction_run (name, run) VALUES ({name}, *{runs})",
            "DELETE FROM *{opaque_tables} WHERE uuid IN ({transaction.unstores})",
            initial_batch,
        )

    def get_transaction_run_args(
        self, user: User, resolution: ArtifactTransactionResolution, name: ArtifactTransactionName
    ) -> tuple[ArtifactTransactionHeader, dict[ResourcePath, ResourcePath]]:
        with self._root.join("transactions").join(name + ".json").open("r") as stream:
            header = ArtifactTransactionHeader.model_validate_json(stream.read())
        if not self._can_run_transaction(user, header.type_name, header.name):
            raise PermissionError()
        transaction = header.make_transaction(None)
        signed_uris = self._sign_uris(user, transaction.get_uris(resolution))
        return header, signed_uris

    def finish_transaction(
        self,
        user: User,
        resolution: ArtifactTransactionResolution,
        name: ArtifactTransactionName,
        batch: RawBatch,
        records: OpaqueRecordSet,
    ) -> None:
        header_uri = self._root.join("transactions").join(name + ".json")
        with header_uri.open("r") as stream:
            header = ArtifactTransactionHeader.model_validate_json(stream.read())
        if not self._can_run_transaction(user, header.type_name, header.name):
            raise PermissionError()
        transaction = header.make_transaction(None)
        signed_uris = self._sign_uris(
            user, {uri: Permissions.GET for uri in transaction.get_uris(resolution)}
        )
        transaction.verify(resolution, self._datastore, signed_uris, records)
        self._sql(
            batch,
            "INSERT INTO {opaque_tables} VALUES (*{records})",
            "DELETE FROM artifact_transaction_run WHERE name={name}",
            "DELETE FROM artifact_transaction WHERE name={name}",
        )
        header_uri.remove()

    def _sql(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError(
            "Pedagogical placeholder for SQLAlchemy commands and making primitives from the results."
        )

    def _collection_permissions(self, user: User, collection: CollectionName) -> Permissions:
        raise NotImplementedError()

    def _can_run_transaction(self, user: User, type_name: str, name: ArtifactTransactionName) -> bool:
        raise NotImplementedError()

    def _can_execute(self, user: User, batch: RawBatch) -> bool:
        raise NotImplementedError()

    def _sign_uris(
        self, user: User, request: Mapping[ResourcePath, Permissions]
    ) -> dict[ResourcePath, ResourcePath]:
        raise NotImplementedError()


class RemoteButler(Butler):
    _user: User
    _dimensions: DimensionUniverse
    _server: _ButlerServer

    @property
    def dimensions(self) -> DimensionUniverse:
        return self._dimensions

    @property
    def is_writeable(self) -> bool:
        return True

    ###########################################################################
    #
    # The Butler read interface is fully implemented in the base class by
    # delegating to Datastore and _expand_existing_dataset_refs.  Nothing to
    # do here, as long as `_expand_existing_dataset_refs` returns URIs signed
    # for `~SignPermissions.GET`.
    #
    ###########################################################################

    ###########################################################################
    #
    # Write operations that demonstrate datastore transactions.
    #
    # These are pedagogical pale shadows of the more complete interfaces we'll
    # need (which will differ only in being able to also modify a lot of
    # database-only content).
    #
    ###########################################################################

    def put_many(self, arg: Iterable[tuple[InMemoryDataset, DatasetRef]], /) -> Iterable[DatasetRef]:
        raise NotImplementedError("TODO")

    def transfer_from(self, origin: Butler, refs: Iterable[DatasetRef], mode: str) -> None:
        raise NotImplementedError("TODO")

    def unstore_datasets(
        self,
        refs: Iterable[DatasetRef],
    ) -> None:
        raise NotImplementedError("TODO")

    ###########################################################################
    #
    # Full-butler-only query methods, very abbreviated here since being
    # read-only they're relevant here only as something other methods delegate
    # to.
    #
    ###########################################################################

    def query(self, defer: bool = True) -> Any:
        raise NotImplementedError("TODO")

    ###########################################################################
    #
    # Artifact transaction interface for ensuring consistency between database
    # and datastores.
    #
    ###########################################################################

    def begin_transaction(
        self,
        transaction: ArtifactTransaction,
        name: ArtifactTransactionName | None = None,
    ) -> tuple[ArtifactTransaction, bool]:
        if name is None:
            name = f"u/{self._user}/{transaction.get_operation_name()}"
        header = ArtifactTransactionHeader.from_transaction(transaction, name)
        just_opened = self._server.begin_transaction(self._user, header)
        return transaction, just_opened

    def commit(self, name: ArtifactTransactionName, transaction: ArtifactTransaction | None = None) -> None:
        header, signed_uris = self._server.get_transaction_run_args(
            self._user, ArtifactTransactionResolution.COMMIT, name
        )
        transaction = header.make_transaction(None)
        batch, records, _ = transaction.run(
            ArtifactTransactionResolution.COMMIT, self._datastore, signed_uris, verify=False
        )
        self._server.finish_transaction(
            self._user, ArtifactTransactionResolution.COMMIT, name, batch, records
        )

    def abandon(self, name: ArtifactTransactionName, transaction: ArtifactTransaction | None = None) -> None:
        header, signed_uris = self._server.get_transaction_run_args(
            self._user, ArtifactTransactionResolution.ABANDON, name
        )
        transaction = header.make_transaction(None)
        batch, records, _ = transaction.run(
            ArtifactTransactionResolution.ABANDON, self._datastore, signed_uris, verify=False
        )
        self._server.finish_transaction(
            self._user, ArtifactTransactionResolution.ABANDON, name, batch, records
        )

    def list_transactions(self) -> list[ArtifactTransactionName]:
        raise NotImplementedError("TODO")

    def vacuum_transactions(self) -> None:
        raise NotImplementedError("TODO")
