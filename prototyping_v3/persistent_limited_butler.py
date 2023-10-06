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


class PersistentLimitedButlerConfig(ExtensionConfig):
    @abstractmethod
    def make_butler(self, root: ResourcePath) -> PersistentLimitedButler:
        raise NotImplementedError()


class PersistentLimitedButler(LimitedButler):
    _config: PersistentLimitedButlerConfig
    _root: ResourcePath
    _datastore: Datastore

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
