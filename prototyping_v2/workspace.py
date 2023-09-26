from __future__ import annotations

__all__ = (
    "ExternalWorkspace",
    "HandledWorkspaceFactoryError",
    "CorruptedWorkspaceError",
    "InternalWorkspace",
    "Workspace",
    "WorkspaceConfig",
    "WorkspaceFactory",
    "WorkspaceExtensionConfig",
)

from abc import abstractmethod
from typing import TYPE_CHECKING, ClassVar, Generic, Protocol, TypeVar

import pydantic
from lsst.resources import ResourcePath

if TYPE_CHECKING:
    from .butler import Butler
    from .config import ButlerConfig, ExtensionConfig


class Workspace(Protocol):
    def abandon(self) -> None:
        """Fully remove this workspace, including all datastore artifacts
        written as part of it.

        This invalidates the workspace if it returns without error.  If it
        fails, `abandon` may be called again to try again.
        """


class InternalWorkspace(Workspace, Protocol):
    """An interface for butler workspace clients that write datastore artifacts
    directly to locations managed by a full butler.

    Internal workspaces must maintain a persistent record of the artifacts they
    *may* have written to central repository location.  This may be written to
    a special file location in the central repository or to some database that
    is also considered part of the central repository, in a format defined by
    the `InternalWorkspace` implementation.

    On commit, an `InternalWorkspace` may perform datastore writes as well
    as reads (e.g., merge small files into larger ones).

    Initializing an internal workspace *may* automatically a parent `Butler`
    instance if all of its operations require one to work.
    """

    def commit(self) -> None:
        """Commit this workspace by registering all datastore artifacts with
        the registry and performing any registry-only operations.

        This invalidates the workspace if it returns without error.  If it
        fails, `commit` may be called again to try again, or `abandon` may be
        called to remove the workspace instead.
        """


class ExternalWorkspace(Workspace, Protocol):
    """An interface for butler workspace clients that write datastore artifacts
    to locations that are not managed by a registry database.

    Initializing an internal workspace *may not* automatically a parent
    `Butler` instance, since external workspaces may be abandoned without
    consulting the parent repository at all.  Instead, parent `Butler` creation
    should be deferred to `commit_transfer`, and only occur if
    ``destination=None``.
    """

    @property
    def root(self) -> ResourcePath:
        """Root location of this workspace.

        This should be known to the in-memory client, it should not be saved to
        the workspace config file or other persistent state in order to permit
        relocation.
        """

    def commit_transfer(self, transfer: str = "auto", destination: Butler | None = None) -> None:
        """Commit this workspace's contents by transferring datastore
        artifacts to a location managed by another butler and inserting
        associated metadata into a registry.

        Parameters
        ----------
        destination : `Butler`, optional
            Butler transfer to.  If not provided the workspace's origin butler
            is used.
        transfer : `str`
            Transfer mode for artifacts.  Note that `None` is not supported
            because external workspaces do not write to a valid full-butler
            location prior to commmit by definition.
        """


_W = TypeVar("_W", bound="Workspace", covariant=True)


class WorkspaceExtensionConfig(ExtensionConfig, Generic[_W]):
    @abstractmethod
    def make_client(
        self,
        root: ResourcePath,
        name: str,
        workspace_id: int | None,
        parent_root: ResourcePath,
        parent_config: ButlerConfig,
        parent_read_butler: Butler | None = None,
        parent_write_butler: Butler | None = None,
    ) -> _W:
        """Construct a `Workspace` instance from this configuration."""
        raise NotImplementedError()


class WorkspaceConfig(pydantic.BaseModel, Generic[_W]):
    name: str
    workspace_id: int | None
    parent_root: ResourcePath
    parent_config: ButlerConfig
    extension: WorkspaceExtensionConfig[_W]

    FILENAME: ClassVar[str] = "butler-workspace.json"
    """Filename used for all workspace butler configuration files."""

    def make_client(
        self,
        root: ResourcePath,
        parent_read_butler: Butler | None = None,
        parent_write_butler: Butler | None = None,
    ) -> _W:
        return self.extension.make_client(
            root,
            self.name,
            self.workspace_id,
            self.parent_root,
            self.parent_config,
            parent_read_butler=parent_read_butler,
            parent_write_butler=parent_write_butler,
        )


class HandledWorkspaceFactoryError(RuntimeError):
    """Exception raised by WorkspaceFactory when it has encountered an error
    but no persistent state has been left behind.

    This error should always be chained to the originating exception.
    """


class CorruptedWorkspaceError(RuntimeError):
    """Exception raised when a workspace is known to be in an inconsistent
    state and requires administrative assistance to be removed.
    """


class WorkspaceFactory(Protocol[_W]):
    """An interface for callables that construct new workspaces.

    Implementations of this protocol may be regular methods or functions, but
    we expect them to frequently be full types so instance state can be used to
    hold workspace-specific initialization state.
    """

    def __call__(
        self,
        name: str,
        root: ResourcePath,
        workspace_id: int | None,
        parent: Butler,
    ) -> tuple[_W, WorkspaceConfig]:
        ...
