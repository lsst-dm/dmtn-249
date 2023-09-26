from __future__ import annotations

__all__ = ("PutWorkspace", "PutWorkspaceFactory", "PutWorkspaceConfig")


from typing import TYPE_CHECKING, Any, Self

import pydantic
from lsst.resources import ResourcePath

from .aliases import InMemoryDataset
from .config import ButlerConfig, DatastoreConfig
from .import_workspace import ImportWorkspaceFactory
from .primitives import DatasetRef
from .raw_batch import RawBatch
from .workspace import (
    ExternalWorkspace,
    Workspace,
    WorkspaceConfig,
    WorkspaceExtensionConfig,
    WorkspaceFactory,
)

if TYPE_CHECKING:
    from .butler import Butler, Datastore


class PutWorkspace(ExternalWorkspace):
    def __init__(
        self,
        root: ResourcePath,
        ref: DatasetRef,
        datastore: Datastore,
        parent: Butler | None,
        parent_config: ButlerConfig,
    ):
        self._root = root
        self._ref = ref
        self._datastore = datastore
        self._parent_config = parent_config
        self._parent = parent

    @property
    def root(self) -> ResourcePath:
        return self._root

    def abandon(self) -> None:
        # Telling the datastore to unstore the ref is probably unnecessary,
        # given that we're going to blow away the whole directory, but
        # Datastore.unstore is required to silently ignore refs that are
        # already not stored.
        self._datastore.unstore([self._ref])
        # Recursively delete everything.  This includes at least the config
        # file.
        for dirpath, _, filenames in self._root.walk():
            for filename in filenames:
                dirpath.join(filename).remove()
        # Need to do this for POSIX, which has real directories.  We need it to
        # be a non-error for URI schemes that do not have real directories, at
        # least when there are no artifacts under that root.
        self._root.remove()

    def commit_transfer(self, transfer: str = "auto", destination: Butler | None = None) -> None:
        if destination is None:
            destination = self._parent
            if destination is None:
                destination = Butler(self._parent_config, writeable=True)
        # Make an *internal* workspace to transfer this *external* workspace's
        # artifact to.
        import_workspace = destination.make_new_workspace(
            factory=ImportWorkspaceFactory(
                self._root,
                self._datastore,
                db_only_batch=RawBatch(),
                transfer_refs=[self._ref],
                transfer_mode=transfer,
            ),
            runs=[self._ref.run],
        )
        import_workspace.commit()
        self.abandon()

    def commit_new_repo(self, **kwargs: Any) -> None:
        raise NotImplementedError("TODO: Make new repo, call commit_transfer.  Or just disable this method.")


class PutWorkspaceConfig(pydantic.BaseModel, WorkspaceExtensionConfig):
    ref: DatasetRef
    datastore_config: DatastoreConfig

    def make_client(
        self,
        root: ResourcePath,
        name: str,
        workspace_id: int | None,
        parent_root: ResourcePath,
        parent_config: ButlerConfig,
        parent_read_butler: Butler | None = None,
        parent_write_butler: Butler | None = None,
    ) -> Workspace:
        return PutWorkspace(
            root, self.ref, self.datastore_config.make_datastore(root), parent_write_butler, parent_config
        )

    @classmethod
    def _from_dict_impl(cls, data: dict[str, Any]) -> Self:
        return cls.model_validate(data)

    def _to_dict_impl(self) -> dict[str, Any]:
        return self.model_dump()


class PutWorkspaceFactory(WorkspaceFactory[PutWorkspace]):
    def __init__(self, obj: InMemoryDataset, ref: DatasetRef):
        self.obj = obj
        self.ref = ref

    def __call__(
        self,
        name: str,
        root: ResourcePath,
        workspace_id: int | None,
        parent: Butler,
    ) -> tuple[PutWorkspace, WorkspaceConfig]:
        assert workspace_id is None, "Put workspaces are always external."
        if "put_datastore" not in parent._config.workspaceOptions:
            raise RuntimeError("No datastore configured for external 'put' operations in this repo.")
        datastore_config: DatastoreConfig = parent._config.workspaceOptions["put_datastore"]
        datastore = datastore_config.make_datastore(root)
        self.ref._opaque_records = datastore.put(self.obj, self.ref)
        workspace_config = WorkspaceConfig(
            name=name,
            workspace_id=workspace_id,
            parent_root=parent._root,
            parent_config=parent._config,
            extension=PutWorkspaceConfig(ref=self.ref, datastore_config=datastore_config),
        )
        workspace = workspace_config.make_client(root, parent_write_butler=parent)
        return workspace, workspace_config
