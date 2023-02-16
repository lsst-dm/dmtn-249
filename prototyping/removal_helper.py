from __future__ import annotations

import dataclasses
from collections.abc import Iterable
from typing import Any, TYPE_CHECKING
from lsst.daf.butler import DatasetRef

if TYPE_CHECKING:
    from .butler import Butler
    from .batched_edit import BatchedEdit
    from .aliases import CollectionName


@dataclasses.dataclass
class RemovalHelper:
    """A helper class for removing datasets and collections while tracking
    their dependencies.

    This class envisions us removing ON DELETE CASCADE from the
    dataset_tags -> dataset foreign keys as per DM-33635, making it necessary
    to explicitly delete TAGGED and CALIBRATION collection membership rows
    before actually deleting datasets.
    """

    butler: Butler
    """Butler being operated upon.
    """

    datasets: set[DatasetRef] = dataclasses.field(default_factory=set)
    """Datasets to be purged.

    This is populated by ``include_collections`` when it processes a RUN
    collection.  Attempting to delete a RUN collection when this list does
    not contain all datasets within it is an error that will cause transaction
    rollback.
    """

    collections: set[CollectionName] = dataclasses.field(default_factory=set)
    """Collections to be removed.

    CHAINED collections in this set will always be removed first.
    """

    chain_links: dict[CollectionName, set[CollectionName]] = dataclasses.field(default_factory=dict)
    """CHAINED collection membership links that will be deleted in order to
    allow the target collections to be removed.

    Keys are CHAINED collection names and values are their to-be-deleted
    members.

    This is populated by ``include_collections``.  Attempting to delete a
    member of a CHAINED collection member without removing the CHAINED
    collection or having an entry here is an error.  Entries here for CHAINED
    collections that are being deleted will be silently ignored.
    """

    associations: dict[CollectionName, Iterable[DatasetRef]] = dataclasses.field(default_factory=dict)
    """TAGGED and CALIBRATION collection membership links that will be deleted
    in order to allow the target datasets to be removed.

    This is populated by ``included_collections`` and ``include_datasets`` if
    ``find_associations_in`` matches all TAGGED or CALIBRATION collections to
    consider (use ``find_associations_in=...`` to find them all).
    """

    orphaned: set[CollectionName] = dataclasses.field(default_factory=set)
    """Current members of a target CHAINED collection that will not belong to
    any CHAINED collection after this removal.

    This is populated by by `incldue_collections` if ``find_orphaned=True``.
    """

    def include_datasets(self, refs: Iterable[DatasetRef], find_associations_in: Any = ()) -> None:
        """Include datasets in the set to be deleted while optoonally looking
        for dangling associations.
        """
        raise NotImplementedError()

    def include_collections(
        self, wildcard: Any, find_orphaned: bool = False, find_associations_in: Any = ()
    ) -> dict[CollectionName, Iterable[DatasetRef]]:
        """Include collections in the set to be deleted while optoonally
        looking for dangling associations and CHAINED collection links.
        """
        raise NotImplementedError()

    def include_orphaned(
        self, find_orphaned: bool = True, recurse: bool = False, find_associations_in: Any = ()
    ) -> None:
        """Include all orphaned collections in the set to be deleted."""
        just_added = self.include(
            self.orphaned, find_orphaned=find_orphaned, find_associations_in=find_associations_in
        )
        self.orphaned.difference_update(just_added.keys())
        if recurse and self.orphaned:
            self.include_orphaned(find_orphaned=True, recurse=True, find_associations_in=find_associations_in)

    def cancel(self) -> None:
        self.datasets.clear()
        self.collections.clear()
        self.chain_links.clear()
        self.associations.clear()
        self.orphaned.clear()

    def __nonzero__(self) -> bool:
        return bool(self.datasets or self.collections or self.chain_links or self.associations)

    def _into_batched_edit(self) -> BatchedEdit:
        """Package-private method that converts this helper's content into
        a BatchedEdit object that Registry understands how to operate on.
        """
        raise NotImplementedError()
