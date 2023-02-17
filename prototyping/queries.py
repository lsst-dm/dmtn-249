from __future__ import annotations

import dataclasses
import enum
import itertools
import uuid
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager
from typing import Any, TypeVar

from lsst.daf.butler import (
    CollectionType,
    ColumnTag,
    DataCoordinate,
    DataId,
    DatasetRef,
    DatasetType,
    DimensionGraph,
    DimensionRecord,
    StorageClass,
)

from .aliases import (
    CollectionDocumentation,
    CollectionName,
    CollectionPattern,
    ColumnName,
    DatasetTypeName,
    DatasetTypePattern,
    DimensionElementName,
    DimensionGroup,
    DimensionName,
)


class OverlapJoinPolicy(enum.Enum):
    FINE = enum.auto()
    COARSE = enum.auto()
    REQUIRE_MANUAL = enum.auto()
    CARTESIAN = enum.auto()


_S = TypeVar("_S", bound="QueryAdapter")


@dataclasses.dataclass
class QueryAdapter(AbstractContextManager[_S]):
    query: Query

    def __enter__(self: _S) -> _S:
        self.query.__enter__()
        return self

    def sliced(
        self: _S,
        start: int = 0,
        stop: int | None = None,
        defer: bool | None = None,
    ) -> _S:
        return dataclasses.replace(self, query=self.query.sliced(start, stop, defer))

    def sorted(
        self: _S,
        order_by: Iterable[str],
        defer: bool | None = None,
    ) -> _S:
        return dataclasses.replace(self, query=self.query.sorted(order_by, defer))

    def count(self, *, exact: bool = True, discard: bool = False) -> int:
        return self.query.count(exact=exact, discard=discard)

    def any(self, *, execute: bool = True, exact: bool = True) -> bool:
        return self.query.any(exact=exact, execute=execute)


@dataclasses.dataclass
class DatasetQueryAdapter(QueryAdapter, Iterable[DatasetRef]):
    def expanded(self) -> DatasetQueryAdapter:
        return dataclasses.replace(
            self, query=self.query.with_dimension_record_columns().with_datastore_record_columns()
        )


@dataclasses.dataclass
class DatasetQueryChain(Iterable[DatasetRef]):
    by_dataset_type: Mapping[DatasetTypeName, DatasetQueryAdapter]

    def __iter__(self) -> Iterator[DatasetRef]:
        return itertools.chain.from_iterable(self.by_dataset_type.values())

    def expanded(self) -> DatasetQueryChain:
        return dataclasses.replace(
            self,
            by_dataset_type={
                dataset_type_name: query.expanded()
                for dataset_type_name, query in self.by_dataset_type.items()
            },
        )

    def count(self, *, exact: bool = True, discard: bool = False) -> int:
        return sum(q.count(exact=exact, discard=discard) for q in self.by_dataset_type.values())

    def any(self, *, execute: bool = True, exact: bool = True) -> bool:
        return any(q.any(execute=execute, exact=exact) for q in self.by_dataset_type.values())


@dataclasses.dataclass
class DimensionRecordQueryAdapter(QueryAdapter, Iterable[DimensionRecord]):
    ...


class DataCoordinateQueryAdapter(QueryAdapter, Iterable[DimensionRecord]):
    def expanded(self) -> DataCoordinateQueryAdapter:
        return dataclasses.replace(self, query=self.query.with_dimension_record_columns())


class CollectionQuery(Sequence[CollectionName]):
    def details(
        self,
    ) -> Iterator[
        tuple[CollectionName, CollectionType, CollectionDocumentation, Sequence[CollectionName] | None]
    ]:
        ...

    def summarize_types(self) -> Iterable[CollectionType]:
        ...


class DatasetTypeQuery(Mapping[DatasetTypeName, DatasetType]):
    ...

    def summarize_storage_classes(self) -> Iterable[StorageClass]:
        ...


class Query(AbstractContextManager["Query"], Iterable[Mapping[ColumnTag, Any]]):
    @property
    def dimensions(self) -> DimensionGraph:
        ...

    @property
    def has_dimension_record_columns(self) -> bool | str:
        ...

    @property
    def has_datastore_record_columns(self) -> bool | str:
        ...

    def collections(
        self,
        collections: CollectionPattern = ...,
        *,
        types: Iterable[CollectionType] = CollectionType.all(),
        flatten_chains: bool = False,
        include_chains: bool | None = None,
        with_child: CollectionName | None = None,
    ) -> CollectionQuery:
        ...

    def dataset_types(
        self,
        dataset_types: DatasetTypePattern = ...,
        *,
        # TODO: constrain on storage class or override storage class
        dimensions: Iterable[DimensionName] = (),
        missing: list[DatasetTypeName] | None = None,
    ) -> DatasetTypeQuery:
        ...

    def iter_column(self, column: ColumnTag) -> Iterable[Any]:
        ...

    def join_datasets(
        self,
        dataset_type: DatasetType | DatasetTypeName,
        collections: Sequence[CollectionName],
        *,
        find_first: bool = True,
        existence_only: bool = False,
        columns: Iterable[ColumnName] = frozenset(("dataset_id", "run")),
        temporal_join: OverlapJoinPolicy | DimensionElementName = OverlapJoinPolicy.FINE,
        defer: bool | None = None,
    ) -> Query:
        ...

    def join_dimensions(
        self,
        dimensions: Iterable[DimensionName] | DimensionGraph,
        spatial_joins: Iterable[tuple[DimensionElementName, DimensionElementName]] = (),
        temporal_joins: Iterable[
            tuple[DimensionElementName | DatasetTypeName, DimensionElementName | DatasetTypeName]
        ] = (),
        overlap_policy: OverlapJoinPolicy = OverlapJoinPolicy.FINE,
        defer: bool | None = None,
    ) -> Query:
        ...

    def join_external_data_ids(
        self,
        dimensions: DimensionGroup,
        data_ids: Iterable[DataCoordinate],
        defer: bool | None = None,
    ) -> Query:
        ...

    def join_external_uuids(
        self,
        dataset_type: DatasetType | DatasetTypeName,
        uuids: Iterable[uuid.UUID],
        defer: bool | None = None,
    ) -> Query:
        ...

    def where(
        self,
        expression: str = "",
        data_id: DataId | None = None,
        uuid: uuid.UUID | None = None,
        bind: Mapping[str, Any] | None = None,
        defer: bool | None = None,
    ) -> Query:
        ...

    def run(self) -> Query:
        ...

    def materialized(self) -> Query:
        ...

    def projected(
        self,
        dimensions: Iterable[DimensionName] | None = None,
        unique: bool = True,
        columns: Iterable[ColumnTag] | None = None,
        drop_postprocessing: bool = False,
        keep_record_columns: bool = True,
        defer: bool | None = None,
    ) -> Query:
        ...

    def with_dimension_record_columns(
        self, element: DimensionElementName | None = None, defer: bool | None = None
    ) -> Query:
        ...

    def with_datastore_record_columns(
        self, dataset_type_name: DatasetTypeName | None = None, defer: bool | None = None
    ) -> Query:
        ...

    def sliced(
        self,
        start: int = 0,
        stop: int | None = None,
        defer: bool | None = None,
    ) -> Query:
        ...

    def sorted(
        self,
        order_by: Iterable[str],
        defer: bool | None = None,
    ) -> Query:
        ...

    def count(self, *, exact: bool = True, discard: bool = False) -> int:
        ...

    def any(self, *, execute: bool = True, exact: bool = True) -> bool:
        ...
