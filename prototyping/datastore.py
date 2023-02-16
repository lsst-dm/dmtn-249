from __future__ import annotations

from abc import abstractmethod
from collections.abc import Iterable
from contextlib import AbstractContextManager

from lsst.daf.butler import DatasetRef


class Datastore:
    @abstractmethod
    def unstore(self, refs: Iterable[DatasetRef]) -> AbstractContextManager[None]:
        """Remove datasets.

        Simply calling this method performs no write operations of any kind.

        Entering the context manager writes a journal file or otherwise
        persists the list of datasets (at least UUIDs, generally a URI or other
        storage-specific identifier) to a location that indicates that a
        fallable deletion operation is underway and these datasets may be
        effective.

        When the context manager exits, actual deletions are executed and the
        journal file (or equivalent) is removed only after all deletions have
        succeeded.
        """
        raise NotImplementedError()
