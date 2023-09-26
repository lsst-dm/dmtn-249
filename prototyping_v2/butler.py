from __future__ import annotations

import getpass
from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Any, cast, overload

from lsst.daf.butler import DataId, DataIdValue, DimensionUniverse, StorageClass
from lsst.resources import ResourcePath, ResourcePathExpression

from .aliases import (
    CollectionName,
    CollectionPattern,
    DatasetTypeName,
    GetParameter,
    InMemoryDataset,
    WorkspaceName,
)
from .artifact_transfer import ArtifactTransferManifest, ArtifactTransferRequest
from .config import ButlerConfig, DatastoreConfig
from .import_workspace import ImportWorkspaceFactory
from .minimal_butler import MinimalButler
from .primitives import (
    DatasetOpaqueRecordSet,
    DatasetRef,
    DatasetType,
    DeferredDatasetHandle,
    OpaqueRecordSet,
)
from .put_workspace import PutWorkspaceFactory
from .raw_batch import RawBatch
from .workspace import (
    CorruptedWorkspaceError,
    ExternalWorkspace,
    HandledWorkspaceFactoryError,
    InternalWorkspace,
    Workspace,
    WorkspaceConfig,
    WorkspaceFactory,
)


class Registry(ABC):
    """Interface for butler component that stores dimensions, dataset metadata,
    and relationships.
    """

    @abstractmethod
    def expand_existing_dataset_refs(
        self, refs: Iterable[DatasetRef], sign: bool = False
    ) -> Iterable[DatasetRef]:
        raise NotImplementedError()

    @abstractmethod
    def insert_workspace(self, name: WorkspaceName, runs: Iterable[CollectionName]) -> int:
        """Record an internal workspace's existence with the registry.

        Parameters
        ----------
        name
            Name of the workspace.  Should be prefixed with ``u/$USER`` for
            regular users.
        runs
            `~CollectionType.RUN` collections whose dataset storage file
            artifacts may be modified by this workspace. No other workspace may
            target the any of these collections until this workspace is
            committed or abandoned.
        """
        raise NotImplementedError()

    @abstractmethod
    def delete_workspace(self, workspace_id: int) -> None:
        """Delete the given workspace, certifying that registry state is now
        consistent with any file artifacts it manages.
        """
        raise NotImplementedError()

    @abstractmethod
    def execute_batch(self, batch: RawBatch) -> None:
        """Perform registry write operations within a single transaction.

        Parameters
        ----------
        batch : `RawBatch`
            Serializable description of the operations to be performed.
        """
        raise NotImplementedError()


class Datastore(ABC):
    """Interface for butler component that stores dataset contents."""

    config: DatastoreConfig
    """Configuration that can be used to reconstruct this datastore.
    """

    @abstractmethod
    def get_many(
        self,
        arg: Iterable[tuple[DatasetRef, Mapping[GetParameter, Any]]],
        /,
    ) -> Iterable[tuple[DatasetRef, Mapping[GetParameter, Any], InMemoryDataset]]:
        """Load datasets into memory.

        Incoming `DatasetRef` objects will have already been fully expanded to
        include both expanded data IDs and all possibly-relevant opaque table
        records.

        Notes
        -----
        The datasets are not necessarily returned in the order they are passed
        in, to better permit async implementations with lazy first-received
        iterator returns.  Implementations that can guarantee consistent
        ordering might want to explicitly avoid it, to avoid allowing callers
        to grow dependent on that behavior instead of checking the returned
        `DatasetRef` objects themselves.
        """
        raise NotImplementedError()

    @abstractmethod
    def make_transfer_requests(
        self, refs: Iterable[DatasetRef], transfer_mode: str
    ) -> Iterable[ArtifactTransferRequest]:
        """Map the given datasets into artifact transfer requests that could
        be used to transfer those datasets to another datastore.

        Each transfer request object should represent an integral number of
        datasets and correspond to how the artifacts would ideally be
        transferred to another datastore of the same type.  There is no
        guarantee that the receiving datastore will keep the same artifacts.
        """
        raise NotImplementedError()

    @abstractmethod
    def receive_transfer_requests(
        self, refs: Iterable[DatasetRef], requests: Iterable[ArtifactTransferRequest], origin: Datastore
    ) -> ArtifactTransferManifest:
        """Create a manifest that records how this datastore would receive the
        given artifacts from another datastore.

        This does not actually perform any artifact writes.

        Parameters
        ----------
        TODO: make refs a convenience collection
        requests : `~collections.abc.Iterable` [ `ArtifactTransferRequest` ]
            Artifacts according to the origin datastore.  Minimal-effort
            transfers - like file copies - preserve artifacts, but in the
            general case transfers only need to preserve datasets.
        origin : `Datastore`
            Datastore that owns or at least knows how to read the datasets
            being transferred.
        """
        raise NotImplementedError()

    @abstractmethod
    def execute_transfer_manifest(
        self, manifest: ArtifactTransferManifest, origin: Datastore
    ) -> OpaqueRecordSet:
        raise NotImplementedError()

    @abstractmethod
    def abandon_transfer_manifest(self, manifest: ArtifactTransferManifest) -> None:
        raise NotImplementedError()

    @abstractmethod
    def put(self, obj: InMemoryDataset, ref: DatasetRef) -> DatasetOpaqueRecordSet:
        raise NotImplementedError()

    @abstractmethod
    def unstore(self, refs: Iterable[DatasetRef]) -> None:
        raise NotImplementedError()


class Butler(MinimalButler):
    """Client for butler data repositories."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError()

    _root: ResourcePath
    _config: ButlerConfig
    _registry: Registry
    _datastore: Datastore

    @property
    def dimensions(self) -> DimensionUniverse:
        raise NotImplementedError("TODO")

    @property
    def is_writeable(self) -> bool:
        raise NotImplementedError("TODO")

    ###########################################################################
    #
    # Implementation of the minimal interface with overloads for dataset
    # type + data ID arguments.
    #
    ###########################################################################

    @overload
    def get(
        self,
        ref: DatasetRef,
        *,
        parameters: Mapping[GetParameter, Any] | None = None,
    ) -> InMemoryDataset:
        # Signature is inherited, but here it accepts not-expanded refs.
        ...

    @overload
    def get(
        self,
        dataset_type: DatasetType | DatasetTypeName,
        data_id: DataId,
        *,
        parameters: Mapping[GetParameter, Any] | None = None,
        storage_class: StorageClass | str | None = None,
        collections: CollectionPattern = None,
        **kwargs: DataIdValue,
    ) -> InMemoryDataset:
        # This overload is not inherited from MinimalButler, but it's unchanged
        # from what we have now except for snake_case.
        ...

    def get(self, *args: Any, **kwargs: Any) -> InMemoryDataset:
        raise NotImplementedError(
            """Will delegate to `Registry` to resolve ref and then delegate to
            ``super().get(ref)``, which will delegate to `get_many`.
            """
        )

    def get_many(
        self,
        arg: Iterable[tuple[DatasetRef, Mapping[GetParameter, Any]]],
        /,
    ) -> Iterable[tuple[DatasetRef, Mapping[GetParameter, Any], InMemoryDataset]]:
        # Signature is inherited, but here it accepts not-expanded refs.
        parameters = []
        refs = []
        for ref, parameters_for_ref in arg:
            parameters.append(parameters_for_ref)
            refs.append(ref)
        expanded_refs = self._registry.expand_existing_dataset_refs(refs, sign=True)
        return self._datastore.get_many(zip(list(expanded_refs), parameters))

    @overload
    def get_deferred(
        self,
        ref: DatasetRef,
        *,
        parameters: Mapping[GetParameter, Any] | None = None,
    ) -> DeferredDatasetHandle:
        # Signature is inherited, but here it accepts not-expanded refs.
        ...

    @overload
    def get_deferred(
        self,
        dataset_type: DatasetType | DatasetTypeName,
        data_id: DataId,
        *,
        parameters: Mapping[GetParameter, Any] | None = None,
        storage_class: StorageClass | str | None = None,
        collections: CollectionPattern = None,
        **kwargs: DataIdValue,
    ) -> DeferredDatasetHandle:
        # This overload is not inherited from MinimalButler, but it's unchanged
        # from what we have now except for snake_case.
        ...

    def get_deferred(self, *args: Any, **kwargs: Any) -> DeferredDatasetHandle:
        raise NotImplementedError(
            """Will delegate to `Registry` to resolve ref and then delegate to
            ``super().get_deferred(ref)``, which will delegate to
            `get_many_deferred`.
            """
        )

    def get_many_deferred(
        self,
        arg: Iterable[tuple[DatasetRef, Mapping[GetParameter, Any]]],
        /,
    ) -> Iterable[tuple[DatasetRef, Mapping[GetParameter, Any], DeferredDatasetHandle]]:
        # Signature is inherited, but here it accepts not-expanded refs.
        parameters = []
        refs = []
        for ref, parameters_for_ref in arg:
            parameters.append(parameters_for_ref)
            refs.append(ref)
        expanded_refs = self._registry.expand_existing_dataset_refs(refs, sign=True)
        return [
            (ref, parameters_for_ref, DeferredDatasetHandle(ref, self._datastore))
            for ref, parameters_for_ref in zip(expanded_refs, parameters)
        ]

    ###########################################################################
    #
    # Workspace stuff, including transfers to demonstrate workspace usage.
    # These are pedagogical pale shadows of the real thing.
    #
    ###########################################################################

    @overload
    def make_new_workspace(
        self,
        factory: WorkspaceFactory,
        runs: Iterable[CollectionName],
        *,
        name: str | None = None,
        root: ResourcePathExpression,
    ) -> ExternalWorkspace:
        ...

    @overload
    def make_new_workspace(
        self,
        factory: WorkspaceFactory,
        runs: Iterable[CollectionName],
        *,
        name: str | None = None,
        root: None = None,
    ) -> InternalWorkspace:
        ...

    def make_new_workspace(
        self,
        factory: WorkspaceFactory,
        runs: Iterable[CollectionName],
        *,
        name: str | None = None,
        root: ResourcePathExpression | None = None,
    ) -> Workspace:
        runs = frozenset(runs)
        if name is None:
            if len(runs) == 1:
                (name,) = runs
            else:
                name = f"u/{getpass.getuser()}/{datetime.now().isoformat()}"
        workspace_id = None
        if root is None:
            # This is an internal workspace.
            root = self._config.workspaceRoot.join(name)
            # For internal workspaces, we insert a row identifying the
            # workspace into a database table before it is actually created,
            # and fail if such a row already exists.  This is effectively a
            # per-name concurrency lock on the creation of workspaces.
            workspace_id = self._registry.insert_workspace(name, runs=runs)
        else:
            # This is an external workspace.
            root = ResourcePath(root, forceDirectory=True)
        config_uri = root.join(WorkspaceConfig.FILENAME.format(name))
        # Delegate to the factory object to do most of the work.  This writes
        # persistent state (e.g. to files or a database).
        try:
            workspace, workspace_config = factory(
                name=name, root=root, workspace_id=workspace_id, parent=self
            )
        except HandledWorkspaceFactoryError as err:
            # An error occurred, but the factory cleaned up its own mess.  We
            # can remove the record of the workspace from the database and
            # just re-raise the original exception.
            if workspace_id is not None:
                self._registry.delete_workspace(workspace_id)
            raise cast(BaseException, err.__cause__)
        except BaseException as err:
            # An error occurred and the factory cannot guarantee anything about
            # the persistent state.  Make it clear that administrative action
            # is needed.
            #
            # Note that this state is recognizable for internal workspaces from
            # the existence of a central database row for the workspace and the
            # absence of a config file, and that the database row needs to be
            # deleted for an administrator to mark it as cleaned up.  For
            # external workspaces we expect the user to just 'rm -rf' (or
            # equivalent) the workspace directory.
            raise CorruptedWorkspaceError(
                f"New workspace {name} with root {root} was corrupted during construction."
            ) from err
        try:
            # Save a configuration file for the workspace to allow the
            # WorkspaceButler to be reconstructed without the full Butler in
            # the future.
            with config_uri.open("w") as stream:
                stream.write(workspace_config.model_dump_json())
        except BaseException:
            # If we fail here, try to clean up.
            workspace.abandon()
            # Successfully cleaned up workspace persistent state, try to remove
            # from database as well if this is an internal workspace.
            if workspace_id is not None:
                self._registry.delete_workspace(workspace_id)
            raise
        return workspace

    def transfer_from(self, origin: Butler, refs: Iterable[DatasetRef], mode: str) -> None:
        refs_by_uuid = {}
        runs = set()
        for ref in refs:
            refs_by_uuid[ref.uuid] = ref
            runs.add(ref.run)
        workspace = self.make_new_workspace(
            factory=ImportWorkspaceFactory(
                origin._root,
                origin._datastore,
                db_only_batch=RawBatch(),
                transfer_refs=refs,
                transfer_mode=mode,
            ),
            runs=runs,
        )
        workspace.commit()

    def put(self, obj: InMemoryDataset, ref: DatasetRef) -> None:
        with ResourcePath.temporary_uri(self._config.workspaceTempRoot) as put_root:
            # We make a trivial *external* workspace to do the put, since that
            # allows us to do the file artifact write(s) before saving the
            # records anywhere.
            workspace = self.make_new_workspace(
                factory=PutWorkspaceFactory(obj, ref), root=put_root, runs=[ref.run]
            )
            # On commit, the PutWorkspace uses an internal ImportWorkspace (as
            # in transfer_from) to safely put its artifacts into the central
            # repo.
            workspace.commit_transfer(transfer="move")
