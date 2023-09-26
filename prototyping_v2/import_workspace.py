from __future__ import annotations

__all__ = ("ImportWorkspace", "ImportWorkspaceFactory", "ImportWorkspaceConfig")

import copy
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Self

import pydantic
from lsst.resources import ResourcePath

from .config import ButlerConfig, DatastoreConfig
from .primitives import DatasetRef
from .raw_batch import DatasetInsertion, RawBatch
from .workspace import (
    InternalWorkspace,
    Workspace,
    WorkspaceConfig,
    WorkspaceExtensionConfig,
    WorkspaceFactory,
)

if TYPE_CHECKING:
    from .butler import Butler, Datastore, TransferManifest


class ImportWorkspace(InternalWorkspace):
    """Internal butler workspace client for imports of repository content
    including datastore artifacts.

    Imports that don't involve writing datastore artifacts don't need to use a
    workspace at all, because we can use DB transactions to keep DB-only state
    self-consistent.  This includes ``transfer="direct"`` ingests.

    This class also supports DB-only operations because often we'll want to
    perform those along with datastore artifacts (especially registry-side
    dataset inserts).
    """

    def __init__(
        self,
        workspace_id: int,
        db_only_batch: RawBatch,
        manifest: TransferManifest,
        parent: Butler,
        origin: Datastore,
    ):
        self._workspace_id = workspace_id
        self._db_only_batch = db_only_batch
        self._manifest = manifest
        self._parent = parent
        self._origin = origin

    def abandon(self) -> None:
        # Note that we don't need the origin datastore here.
        self._parent._datastore.abandon_transfer_manifest(self._manifest)
        self._parent._registry.delete_workspace(self._workspace_id)

    def commit(self) -> None:
        # Set up insertions into registry as much as we can up front.
        batch = copy.deepcopy(self._db_only_batch)
        for ref in self._manifest.extract_refs():
            batch.dataset_insertions.setdefault(ref.run, {}).setdefault(ref.dataset_type.name, []).append(
                DatasetInsertion(uuid=ref.uuid, data_coordinate_values=ref.data_id.values_tuple())
            )
        # Do the actual datastore artifact transfers.
        records = self._parent._datastore.execute_transfer_manifest(self._manifest, self._origin)
        # Include the records derived from those artifact transfers in the
        # registry batch.
        if batch.opaque_table_insertions is None:
            batch.opaque_table_insertions = records
        else:
            batch.opaque_table_insertions.update(records)
        # Do all the registry insertions.
        self._parent._registry.execute_batch(batch)
        # Delete the workspace config and its registry entry.
        self._destroy()
        self._parent._registry.delete_workspace(self._workspace_id)

    def _destroy(self) -> None:
        """Delete all persistent artifacts associated with this workspace,
        including its configuration file, the saved transfer manifest, and
        the saved description of DB-only operations to perform.
        """
        raise NotImplementedError("TODO")


class ImportWorkspaceConfig(pydantic.BaseModel, WorkspaceExtensionConfig):
    origin_root: ResourcePath
    origin_config: DatastoreConfig
    db_only_batch: RawBatch
    manifest: TransferManifest  # TODO: need pydantic magic for importing derived type

    def make_client(
        self,
        root: ResourcePath,
        name: str,
        workspace_id: int | None,
        parent_config: ButlerConfig,
        parent_read_butler: Butler | None = None,
        parent_write_butler: Butler | None = None,
    ) -> Workspace:
        if parent_write_butler is None:
            parent_write_butler = Butler(parent_config, writeable=True)
        assert workspace_id is not None, "Import workspaces are always internal."
        return ImportWorkspace(
            workspace_id,
            db_only_batch=self.db_only_batch,
            manifest=self.manifest,
            parent=parent_write_butler,
            origin=self.origin_config.make_datastore(self.origin_root),
        )

    @classmethod
    def _from_dict_impl(cls, data: dict[str, Any]) -> Self:
        return cls.model_validate(data)

    def _to_dict_impl(self) -> dict[str, Any]:
        return self.model_dump()


class ImportWorkspaceFactory(WorkspaceFactory[ImportWorkspace]):
    def __init__(
        self,
        origin_root: ResourcePath,
        origin: Datastore,
        db_only_batch: RawBatch,
        transfer_refs: Iterable[DatasetRef],
    ):
        self.origin_root = origin_root
        self.origin = origin
        self.db_only_batch = db_only_batch
        self.transfer_refs = transfer_refs

    def __call__(
        self,
        name: str,
        root: ResourcePath,
        workspace_id: int | None,
        parent: Butler,
        parent_config: ButlerConfig,
    ) -> tuple[ImportWorkspace, WorkspaceConfig]:
        requests = self.origin.make_transfer_requests(self.transfer_refs)
        manifest = parent._datastore.receive_transfer_requests(self.transfer_refs, requests, self.origin)
        assert workspace_id is not None, "Import workspaces are always internal."
        workspace_config = WorkspaceConfig(
            name=name,
            workspace_id=workspace_id,
            parent=parent_config,
            extension=ImportWorkspaceConfig(
                origin_root=self.origin_root,
                origin_config=self.origin.config,
                db_only_batch=self.db_only_batch,
                manifest=manifest,
            ),
        )
        workspace = workspace_config.make_client(root, parent_write_butler=parent)
        return workspace, workspace_config
