from __future__ import annotations

__all__ = ("WorkspaceButler",)

from abc import abstractmethod
from collections.abc import Iterable
from typing import Any, TYPE_CHECKING

from lsst.resources import ResourcePathExpression

from .aliases import InMemoryDataset
from .primitives import DatasetRef
from .minimal_butler import MinimalButler

if TYPE_CHECKING:
    from .butler import Butler


class WorkspaceButler(MinimalButler):
    """Client for a butler workspace, in which writes are conditional until
    the workspace is committed back to a central repository.
    """

    @abstractmethod
    def put(self, obj: InMemoryDataset, ref: DatasetRef) -> DatasetRef:
        """Write a dataset given a DatasetRef with a fully-expanded data ID
        (but no Datastore records).

        The returned DatasetRef will be further expanded to include the new
        Datastore records as well.
        """
        raise NotImplementedError()

    # There is no put_many because WorkspaceButler implementations are not
    # expected to have high overheads, even for tiny datasets, since there's no
    # server to talk to (neither http nor SQL) and we can rework dataset
    # storage on commit (e.g. to merge tiny files).

    @abstractmethod
    def abandon(self) -> None:
        raise NotImplementedError()

    @abstractmethod
    def commit(self, *, transfer: str | None = None) -> None:
        raise NotImplementedError()

    @abstractmethod
    def into_repo(
        self, *, root: ResourcePathExpression | None = None, transfer: str | None = None, **kwargs: Any
    ) -> Butler:
        raise NotImplementedError()

    @abstractmethod
    def manifest(self) -> Iterable[DatasetRef]:
        raise NotImplementedError()
