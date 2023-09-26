from __future__ import annotations

__all__ = ("RemovalWorkspace", "RemovalWorkspaceFactory", "RemovalWorkspaceConfig")

import uuid
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Self

import pydantic
from lsst.resources import ResourcePath

from .config import ButlerConfig
from .primitives import DatasetRef
from .raw_batch import RawBatch
from .workspace import (
    InternalWorkspace,
    Workspace,
    WorkspaceConfig,
    WorkspaceExtensionConfig,
    WorkspaceFactory,
)

if TYPE_CHECKING:
    from .butler import Butler


class RemovalWorkspace(InternalWorkspace):
    def __init__(
        self,
        workspace_id: int,
        refs: dict[uuid.UUID, DatasetRef],
        parent: Butler,
    ):
        self._workspace_id = workspace_id
        self._refs = refs
        self._parent = parent

    def abandon(self) -> None:
        # Artifact removal is strange because we do not attempt to make it
        # reversible, and hence after we set up the workspace, the only thing
        # we can safely do is see it all the way through. That means abandon
        # and commit do the same thing.
        self.commit()

    def commit(self) -> None:
        # Start by removing opaque records from the registry, leaving the
        # workspace as the only entity managing artifact lifetimes.
        # It is important for rigorous fault-tolerance for this to be
        # idempotent, but that's naturally the case for DELETE ... WHERE,
        # since the WHERE will just yield no records the second (etc) time.
        opaque_removals_batch = RawBatch()
        for ref in self._refs.values():
            assert ref._opaque_records is not None
            for table_name in ref._opaque_records.tables:
                opaque_removals_batch.opaque_table_removals.setdefault(table_name, set()).add(ref.uuid)
        self._parent._registry.execute_batch(opaque_removals_batch)
        # This is the point of no return.
        self._parent._datastore.unstore(self._refs.values())
        # Delete the workspace config and its registry entry.
        self._destroy()
        self._parent._registry.delete_workspace(self._workspace_id)

    def _destroy(self) -> None:
        """Delete all persistent artifacts associated with this workspace,
        including its configuration file, the saved transfer manifest, and
        the saved description of DB-only operations to perform.
        """
        raise NotImplementedError("TODO")


class RemovalWorkspaceConfig(pydantic.BaseModel, WorkspaceExtensionConfig):
    refs: dict[uuid.UUID, DatasetRef]

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
        if parent_write_butler is None:
            parent_write_butler = Butler(parent_config, parent_root, writeable=True)
        assert workspace_id is not None, "Removal workspaces are always internal."
        return RemovalWorkspace(
            workspace_id,
            refs=self.refs,
            parent=parent_write_butler,
        )

    @classmethod
    def _from_dict_impl(cls, data: dict[str, Any]) -> Self:
        return cls.model_validate(data)

    def _to_dict_impl(self) -> dict[str, Any]:
        return self.model_dump()


class RemovalWorkspaceFactory(WorkspaceFactory[RemovalWorkspace]):
    def __init__(
        self,
        refs: Iterable[DatasetRef],
    ):
        self.refs = refs

    def __call__(
        self,
        name: str,
        root: ResourcePath,
        workspace_id: int | None,
        parent: Butler,
    ) -> tuple[RemovalWorkspace, WorkspaceConfig]:
        assert workspace_id is not None, "Removal workspaces are always internal."
        workspace_config = WorkspaceConfig(
            name=name,
            workspace_id=workspace_id,
            parent_root=parent._root,
            parent_config=parent._config,
            extension=RemovalWorkspaceConfig(
                refs={ref.uuid: ref for ref in self.refs},
            ),
        )
        workspace = workspace_config.make_client(root, parent_write_butler=parent)
        return workspace, workspace_config
