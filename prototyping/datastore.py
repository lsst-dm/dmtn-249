from __future__ import annotations

__all__ = ("Datastore", "DatastoreTableDefinition")

import dataclasses
from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from typing import Any, TypeAlias, final

from lsst.daf.butler import DatasetId, StoredDatastoreItemInfo, ddl
from lsst.resources import ResourcePath

from .aliases import ColumnName, DatastoreTableName, GetParameter, InMemoryDataset, StorageURI
from .primitives import DatasetRef


class Datastore(ABC):
    """Interface for the polymorphic butler component that stores dataset
    contents.
    """

    @property
    @abstractmethod
    def tables(self) -> Mapping[DatastoreTableName, DatastoreTableDefinition]:
        """Database tables needed to store this Datastore's records."""
        raise NotImplementedError()

    @abstractmethod
    def get_root(self, datastore_name: str) -> ResourcePath | None:
        """Return the root URL for datasets stored in datastore with the given
        name.

        If the datastore name is not recognized by this datastore or any nested
        datastore, `None` should be returned.  If `None` is ultimately returned
        to the calling code that datastore may have only absolute URLs.
        """
        raise NotImplementedError()

    @abstractmethod
    def extract_existing_uris(self, refs: Iterable[DatasetRef]) -> list[StorageURI]:
        """Extract URIs from the records attached to datasets.

        These are possibly-relative URIs that need to be made absolute and
        possibly signed in order to be used.
        """
        raise NotImplementedError()

    @abstractmethod
    def predict_new_uris(self, refs: Iterable[DatasetRef]) -> list[StorageURI]:
        """Predict URIs for new datasets to be written.

        These are possibly-relative URIs that need to be made absolute and
        possibly signed in order to be used.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_many(
        self,
        arg: Iterable[tuple[DatasetRef, Mapping[GetParameter, Any]]],
        /,
        paths: Mapping[StorageURI, ResourcePath],
    ) -> Iterable[tuple[DatasetRef, Mapping[GetParameter, Any], InMemoryDataset]]:
        """Load datasets into memory.

        Incoming `DatasetRef` objects will have already been fully expanded to
        include both expanded data IDs and all possibly-relevant datastore
        table records.

        Notes
        -----
        The datasets are not necessarily returned in the order they are passed
        in, to better permit async implementations with lazy first-received
        iterator returns.  Implementations that can guarantee consistent
        ordering might want to explicitly avoid it, to avoid allowing callers
        to grow dependent on that behavior instead of checking the returned
        `DatasetRef` objects themselves.
        """
        raise NotImplementedError()

    @abstractmethod
    def put_many(
        self, arg: Iterable[tuple[InMemoryDataset, DatasetRef]], /, paths: Mapping[StorageURI, ResourcePath]
    ) -> None:
        """Write an in-memory object to this datastore.

        Parameters
        ----------
        arg
            Objects to write and the `DatasetRef` objects that should identify
            them.
        paths
            Mapping from possibly-relative URI to definitely-absolute,
            signed-if-needed URL.

        Notes
        -----
        This method is not responsible for generating records; in this
        prototype we delegate that to `verify` so we can always do that work on
        the Butler REST server when there is one, since `put_many` can only be
        called on the client.
        """
        raise NotImplementedError()

    @abstractmethod
    def verify(
        self, refs: Iterable[DatasetRef]
    ) -> Iterable[tuple[DatasetId, bool | None, dict[DatastoreTableName, list[StoredDatastoreItemInfo]]]]:
        """Test whether all artifacts are present for a dataset and return
        datastore records that represent them.

        Parameters
        ----------
        ref : ~collections.abc.Iterable` [ `DatasetRef` ]
            Dataset to verify.  May or may not have datastore records attached;
            if records are attached, they must be consistent with the artifact
            content (e.g. if checksums are present, they should be checked).

        Returns
        -------
        results : `~collections.abc.Iterable`
            Result 3-tuples, each of which contains:

            - ``dataset_id``: the ID of one of the given datasets.
            - ``valid``: if all artifacts for this dataset are present ,
              `True`.  If no artifacts are present, `None`.  If only some
              of the needed artifacts are present or any artifact is corrupted,
              `False`.
            - ``records``: datastore records that should be attached to this
              `DatasetRef`. These must be created from scratch if none were
              passed in (the datastore may assume its configuration has not
              changed since the artifact(s) were written) and maybe augmented
              if incomplete (e.g. if sizes or checksums were absent and have
              now been calculated).
        """
        raise NotImplementedError()

    @abstractmethod
    def unstore(self, refs: Iterable[DatasetRef]) -> None:
        """Remove all stored artifacts associated with the given datasets.

        Notes
        -----
        Artifacts that do not exist should be silently ignored - that allows
        this method to be called to clean up after an interrupted operation has
        left artifacts in an unexpected state.

        The given `DatasetRef` object may or may not have datastore records
        attached.  If no records are attached the datastore may assume its
        configuration has not changed since the artifact(s) were written.
        """
        raise NotImplementedError()


@final
@dataclasses.dataclass
class DatastoreTableDefinition:
    """Definition of a database table used to store datastore records."""

    spec: ddl.TableSpec
    """Specification used to create the table.

    Must include a UUID ``dataset_id`` column.
    """

    record_type: type[StoredDatastoreItemInfo]
    """Python type used to represent records of the table.

    Must include the UUID; this is a major difference from the current
    `StoredDatastoreItemInfo` (since DM-40053) that needs to be resolved.
    """

    artifact_key: ColumnName | None
    """A column name that should be used to identify connect the rows for
    datasets that share the same artifact, to prevent those artifacts from
    being deleted unless all such datasets are being unstored.
    """


ArtifactTransferResponse: TypeAlias = Any
"""A type alias for the datastore-internal type used to represent the response
to a sequence of `ArtifactTransferRequest`.

An `ArtifactTransferResponse` generally combines one or more
`ArtifactTransferResponse` instances with a datastore-specific representation
of how they will appear in the datastore.
"""
