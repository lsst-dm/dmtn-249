from __future__ import annotations

__all__ = ("PersistentLimitedButler", "PersistentLimitedButlerConfig")

from abc import abstractmethod
from collections.abc import Callable, Iterable, Iterator, Mapping, Set
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, ClassVar

from lsst.resources import ResourcePath

from .extension_config import ExtensionConfig
from .aliases import StorageURI
from .limited_butler import LimitedButler

if TYPE_CHECKING:
    from .datastore import Datastore


class PathMapping(Mapping[StorageURI, ResourcePath]):
    def __init__(
        self,
        uris: Set[StorageURI],
    ):
        self._uris = uris

    def __iter__(self) -> Iterator[StorageURI]:
        return iter(self._uris)

    def __contains__(self, __key: object) -> bool:
        return __key in self._uris

    def __len__(self) -> int:
        return len(self._uris)

    def __getitem__(self, __key: StorageURI) -> ResourcePath:
        root, relative = __key
        if root is not None:
            return root.join(relative)
        else:
            # TODO: I'm not sure forceAbsolute=True is appropriate - we require
            # the result to be absolute, but we want it to raise if it isn't
            # already, rather than assume the current directory is relevant.
            return ResourcePath(relative, forceAbsolute=True)


class SignedPathMapping(PathMapping):
    def __init__(
        self,
        uris: Set[StorageURI],
        sign: Callable[[Iterable[StorageURI]], tuple[dict[StorageURI, ResourcePath], datetime]],
    ):
        super().__init__(uris)
        self._sign = sign
        self._cache, self._expiration = sign(self._uris)

    _PADDING: ClassVar[timedelta] = timedelta(seconds=5)

    def __getitem__(self, __key: StorageURI) -> ResourcePath:
        if self._expiration + self._PADDING < datetime.now():
            self._cache, self._expiration = self._sign(self.keys())
        return self._cache[__key]


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

    def _get_resource_paths(
        self, uris: Iterable[StorageURI], write: bool = False
    ) -> Mapping[StorageURI, ResourcePath]:
        """Return `ResourcePath` objects usable for direct Datastore operations
        for the given possibly-relative URIs.

        This turns URIs into absolute URLs and will sign them if needed for the
        relevant datastore.
        """
        return PathMapping(frozenset(uris))


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
