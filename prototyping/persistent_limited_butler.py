from __future__ import annotations

__all__ = ("PersistentLimitedButler", "PersistentLimitedButlerConfig")

from abc import abstractmethod
from collections.abc import Iterable
from typing import TYPE_CHECKING

from lsst.resources import ResourcePath

from .extension_config import ExtensionConfig
from .aliases import StorageURI
from .limited_butler import LimitedButler

if TYPE_CHECKING:
    from .datastore import Datastore


class PersistentLimitedButler(LimitedButler):
    """An extension of `LimitedButler` that can server as the origin of a
    dataset transfer.

    `PersistenceLimitedButler` objects must be constructable from a stored
    configuration file, and must delegate storage to a `Datastore`.  Expected
    implementations include full `Butler` implementations and long-lived
    workspaces (both internal and external).
    """

    _config: PersistentLimitedButlerConfig
    """Configuration and persistable factory for this client."""

    _root: ResourcePath | None
    """Optional root for the butler.
    """

    _datastore: Datastore
    """Datastore client object."""

    @abstractmethod
    def _get_resource_paths(
        self, uris: Iterable[StorageURI], write: bool = False
    ) -> dict[StorageURI, ResourcePath]:
        """Return `ResourcePath` objects usable for direct Datastore operations
        for the given possibly-relative URIs.

        This turns URIs into absolute URLs and will sign them if needed for the
        relevant datastore.
        """
        raise NotImplementedError()


class PersistentLimitedButlerConfig(ExtensionConfig):
    """Configuration and factory for a `PersistentLimitedButler`."""

    @abstractmethod
    def make_butler(self, root: ResourcePath | None) -> PersistentLimitedButler:
        """Construct a butler from this configuration and the given root.

        The root is not stored with the configuration to encourage
        relocatability, and hence must be provided on construction in addition
        to the config.  The root is only optional if all nested datastores
        know their own absolute root or do not require any paths.
        """
        raise NotImplementedError()
