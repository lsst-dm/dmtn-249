from __future__ import annotations

import dataclasses
import uuid
from typing import Any

from lsst.daf.butler import DataCoordinate, Formatter
from lsst.resources import ResourcePath

from .aliases import CollectionName, TransferMode
from .butler import ArtifactTransferRequest
from .primitives import DatasetType


@dataclasses.dataclass
class SingleDatasetFileTransferRequest(ArtifactTransferRequest):
    """Transfer request for a single-complete-dataset file that will be
    ingested by copying that file to the destination datastore root.
    """

    run: CollectionName
    uuid: uuid.UUID
    dataset_type: DatasetType
    data_id: DataCoordinate
    formatter: type[Formatter]
    transfer_mode: TransferMode
    origin_uri: ResourcePath


@dataclasses.dataclass
class SingleDatasetFileReferenceRequest(ArtifactTransferRequest):
    """Transfer request for a single-complete-dataset file that will be
    ingested via an absolute external URI.
    """

    run: CollectionName
    uuid: uuid.UUID
    dataset_type: DatasetType
    data_id: DataCoordinate
    formatter: type[Formatter]
    uri: ResourcePath


@dataclasses.dataclass
class DisassembledDatasetFileTransferRequest(ArtifactTransferRequest):
    """Transfer request for a collection of files that together comprise a
    complete dataset, with each file corresponding to a component dataset, to
    be ingested by copying all files to the destination datastore root.
    """

    run: CollectionName
    uuid: uuid.UUID
    dataset_type: DatasetType
    data_id: DataCoordinate
    formatter: type[Formatter]
    transfer_mode: TransferMode
    component_origin_uris: list[tuple[str, ResourcePath]]


@dataclasses.dataclass
class DisassembledDatasetFileReferenceRequest(ArtifactTransferRequest):
    """Transfer request for a collection of files that together comprise a
    complete dataset, with each file corresponding to a component dataset, to
    be ingested via absolute external URIs.
    """

    run: CollectionName
    uuid: uuid.UUID
    dataset_type: DatasetType
    data_id: DataCoordinate
    formatter: type[Formatter]
    component_uris: list[tuple[str, ResourcePath]]


@dataclasses.dataclass
class MultipleCoordinateDatasetFileReferenceRequest(ArtifactTransferRequest):
    """Transfer request for a single file that holds multiple datasets
    corresponding to different data IDs, to be ingested via absolute external
    URIs.
    """

    run: CollectionName
    dataset_type: DatasetType
    common_data_id: DataCoordinate
    children: list[tuple[uuid.UUID, DataCoordinate]]
    formatter: type[Formatter]
    uri: ResourcePath


# There is no MultipleCoordinateDatasetFileTransferRequest right now because
# managing those artifacts (i.e. knowing when we can safely delete them) is a
# pain, and something I propose we try to not support for now.  So we could add
# the request object, but I don't think we should try to have a datastore that
# could accept such a request.


@dataclasses.dataclass
class StructuredDataWriteRequest(ArtifactTransferRequest):
    """A transfer request that directly sends a JSON-compatible nested dict to
    be saved.

    A simple file datastore could accept this by just writing a JSON file.
    A more complicated file datastore could merge multiple such requests into
    a single file.  A Sasquatch datastore could check whether they have a
    metric storage class and upload them, etc...
    """

    run: CollectionName
    uuid: uuid.UUID
    dataset_type: DatasetType
    data_id: DataCoordinate
    content: dict[str, Any]
