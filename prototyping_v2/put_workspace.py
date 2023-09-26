from __future__ import annotations

__all__ = ("PutWorkspace", "PutWorkspaceFactory", "PutWorkspaceConfig")


import json
from typing import TYPE_CHECKING, Any, Self

import pydantic
from lsst.resources import ResourcePath

from .aliases import InMemoryDataset
from .config import ButlerConfig
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
    from .butler import Butler, Datastore


class PutWorkspace(InternalWorkspace):
    def __init__(
        self,
        workspace_id: int,
        root: ResourcePath,
        ref: DatasetRef,
        datastore: Datastore,
        parent_root: ResourcePath,
        parent_config: ButlerConfig,
        parent_butler: Butler | None,
    ):
        self._workspace_id = workspace_id
        self._root = root
        self._ref = ref
        self._datastore = datastore
        self._parent_root = parent_root
        self._parent_config = parent_config
        self._parent_butler = parent_butler

    def put(self, obj: InMemoryDataset) -> None:
        records = self._datastore.put(obj, self._ref)
        with self._root.join("records.json").open("w") as stream:
            json.dump(records.to_json_data(), stream)

    def abandon(self) -> None:
        if self._parent_butler is None:
            self._parent_butler = Butler(self._parent_config, self._parent_root, writeable=True)
        records_uri = self._root.join("records.json")
        if records_uri.exists():
            records_uri.remove()
        self._datastore.unstore([self._ref])
        self._destroy()
        self._parent_butler._registry.delete_workspace(self._workspace_id)

    def commit(self) -> None:
        if self._parent_butler is None:
            self._parent_butler = Butler(self._parent_config, self._parent_root, writeable=True)
        batch = RawBatch()
        batch.dataset_insertions[self._ref.run] = {
            self._ref.dataset_type.name: [
                DatasetInsertion(uuid=self._ref.uuid, data_coordinate_values=self._ref.data_id.values_tuple())
            ]
        }
        with self._root.join("records.json").open("r") as stream:
            record_data = json.load(stream)
        batch.opaque_table_insertions = self._datastore.opaque_record_set_type.from_json_data(record_data)
        self._parent_butler._registry.execute_batch(batch)
        records_uri = self._root.join("records.json")
        if records_uri.exists():
            records_uri.remove()
        self._destroy()
        self._parent_butler._registry.delete_workspace(self._workspace_id)

    def _destroy(self) -> None:
        """Delete all persistent artifacts associated with this workspace,
        including its configuration file, the saved transfer manifest, and
        the saved description of DB-only operations to perform.
        """
        raise NotImplementedError("TODO")


class PutWorkspaceConfig(pydantic.BaseModel, WorkspaceExtensionConfig):
    ref: DatasetRef

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
        assert workspace_id is not None, "Put workspaces are always internal."
        if parent_write_butler is None:
            datastore = parent_config.datastore.make_datastore(parent_root)
        return PutWorkspace(
            workspace_id,
            root=root,
            ref=self.ref,
            datastore=datastore,
            parent_root=parent_root,
            parent_config=parent_config,
            parent_butler=parent_write_butler,
        )

    @classmethod
    def _from_dict_impl(cls, data: dict[str, Any]) -> Self:
        return cls.model_validate(data)

    def _to_dict_impl(self) -> dict[str, Any]:
        return self.model_dump()


class PutWorkspaceFactory(WorkspaceFactory[PutWorkspace]):
    def __init__(self, ref: DatasetRef):
        self.ref = ref

    def __call__(
        self,
        name: str,
        root: ResourcePath,
        workspace_id: int | None,
        parent: Butler,
    ) -> tuple[PutWorkspace, WorkspaceConfig]:
        workspace_config = WorkspaceConfig(
            name=name,
            workspace_id=workspace_id,
            parent_root=parent._root,
            parent_config=parent._config,
            extension=PutWorkspaceConfig(ref=self.ref),
        )
        workspace = workspace_config.make_client(root, parent_write_butler=parent)
        return workspace, workspace_config
