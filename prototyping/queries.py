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
    DataCoordinate,
    DataId,
    DatasetRef,
    DatasetType,
    DimensionRecord,
    StorageClass,
)
from lsst.daf.relation import ColumnTag

from .aliases import (
    CollectionDocumentation,
    CollectionName,
    ColumnName,
    DatasetTypeName,
    DimensionElementName,
    DimensionName,
)
from .primitives import DimensionGroup


class OverlapJoinPolicy(enum.Enum):
    """Enum that can be used how/whether overlap joins are added to queries."""

    FINE = enum.auto()
    """Automatically add spatial and/or temporal joins between the most
    fine-grained elements on each side (e.g. ``{visit, detector}`` to
    ``{patch}``).
    """

    COARSE = enum.auto()
    """Automatically add spatial and/or temporal joins between the most
    coarse-grained elements on each side (e.g. ``{visit}`` to ``{tract}``).
    """

    REQUIRE_MANUAL = enum.auto()
    """Raise if a spatial or temporal join is possible but no explicit join
    was provided (via another argument).
    """

    CARTESIAN = enum.auto()
    """Return the Cartesian-product join (do not constrain on overlaps at all)
    when explicit joins are not provided.
    """


_S = TypeVar("_S", bound="QueryAdapter")


class Query(AbstractContextManager["Query"], Iterable[Mapping[ColumnTag, Any]]):
    """Prototype of an extension to `lsst.daf.butler.registry.queries.Query`
    for use as a direct (albeit "advanced use") public interface.

    This will have essentially the same state as the current `Query` class and
    many of the same methods; I'd always planned to go back and tweak those
    interfaces to make them more user-friendly and complete, and this is a
    first draft of that.

    This will be specialized for different Registries (i.e. SQL vs. http) via
    specialized implementations of the QueryBackend and QueryContext classes
    that are already present.

    Query is used directly to back queries over datasets, data IDs, and
    dimension records.  These can be iterated over directly via various
    `QueryAdapter` subclasses (these are what the dedicated simpler
    `Butler.query_*` methods return).  Iteration over Query itself yields
    Mappings as rows.  The latter is necessary for use cases that involve
    iterating over multiple related things, like ``exposure`` dimension records
    and the CALIBRATION-collection datasets with corresponding validity ranges.

    Queries for dataset types and collections are not backed by `Query`
    directly (see `DatasetTypeQuery` and `CollectionQuery`) as the query system
    tends to resolve those in advance using client-side cached information,
    since it also needs to do that to generate performant queries for datasets.
    `Query` does provide methods that act as factories for `DatasetTypeQuery`
    and `CollectionQuery`.

    Query is immutable, and all modifier methods return new updated Query
    instances that do not invalidate or otherwise modify the original.

    Query is also a context manager; entering the context can be used to manage
    temporary table lifetimes used by `materialize` and the `join_external_*`
    methods if possible (i.e. with a direct SQL backend).  When not possible
    (i.e. http backend, at least at first), the context manager would do
    nothing, and instead temp table content would be held locally and
    re-uploaded every time it's needed, which would be less efficient but still
    yield the same results.
    """

    @property
    def dimensions(self) -> DimensionGroup:
        """The dimensions whose keys are currently joined into the query."""
        raise NotImplementedError()

    @property
    def has_dimension_record_columns(self) -> bool | DimensionElementName:
        """Whether this query has:

        `False`
            No dimension record columns, or only those needed to support a
            ``where`` or ``order_by`` clause.
        `True`
            All dimension record columns needed to return expanded data IDs for
            its dimensions.
        `DimensionElementName`
            All dimension record columns for the named element.

        Notes
        -----
        Dimensions whose records are cached on the client are always considered
        to have all of their columns present in the query, even if they aren't
        actually part of the result mapping.
        """
        raise NotImplementedError()

    @property
    def has_datastore_record_columns(self) -> bool | DatasetTypeName:
        """Whether this query has:

        `False`
            No datastore record columns, or only those needed to support a
            ``where`` or ``order_by`` clause.
        `True`
            All datastore record columns needed to return expanded DatasetRefs
            for its dataset types.
        `DatasetType`
            All datastore record columns for the named dataset type.
        """
        raise NotImplementedError()

    def join_datasets(
        self,
        dataset_type: DatasetType | DatasetTypeName,
        collections: Sequence[CollectionName],
        *,
        find_first: bool = True,
        columns: Iterable[ColumnName] = frozenset(("uuid", "run")),
        temporal_join: OverlapJoinPolicy | DimensionElementName = OverlapJoinPolicy.FINE,
        defer: bool | None = None,
    ) -> Query:
        """Join the query to a search for datasets.

        Parameters
        ----------
        dataset_type
            Dataset type to search for.
        collections
            Collections to search, in order if find_first=True.
        find_first, optional
            Whether to return only the first dataset for each data ID in the
            sequence of collections.
        columns, optional
            Columns to include in the query from the dataset search.  By
            default only those necessary to return DatasetRefs are included.
            If empty, this dataset search provides a constraint on the data IDs
            of query's result rows, but no more.
        temporal_join, optional
            A temporal dimension element to overlap-join CALIBRATION collection
            timespans to, or a policy object indicating how to handle this
            automatically.  Datasets in other collection types are treated as
            having unbounded timespans.
        defer, optional
            See `Butler.query`.

        Notes
        -----
        This method cannot perform spatial joins or temporal joins unless a
        CALIBRATION collection search is involved; use `join_dimensions` first
        if the dataset type's dimensions are related spatially or temporally to
        the query's current dimensions.
        """
        raise NotImplementedError()

    def join_dimensions(
        self,
        dimensions: Iterable[DimensionName],
        spatial_joins: Iterable[tuple[DimensionElementName, DimensionElementName]] = (),
        temporal_joins: Iterable[tuple[DimensionElementName | DatasetTypeName, DimensionElementName]] = (),
        overlap_policy: OverlapJoinPolicy = OverlapJoinPolicy.FINE,
        defer: bool | None = None,
    ) -> Query:
        """Join the query to one or more dimension tables.

        Parameters
        ----------
        dimensions
            Dimension keys to join against.
        spatial_joins, optional
            Explicit spatial joins between dimension elements.  First element
            in each tuple is an element already in the query, while the second
            must be in (or implied by) the ``dimensions`` argument.
        temporal_joins, optional
            Explicit temporal joins between dimension elements or dataset
            searches in CALIBRATION collections.  First element in each tuple
            is an element or dataset type already in the query, while the
            second must be in (or implied by) the ``dimensions`` argument.
        overlap_policy, optional
            Enum describing how/whether to add spatial and temporal joins that
            were not given explicitly.
        defer, optional
            See `Butler.query`.
        """
        raise NotImplementedError()

    def join_external_data_ids(
        self,
        dimensions: DimensionGroup,
        data_ids: Iterable[DataCoordinate],
        defer: bool | None = None,
    ) -> Query:
        """Join the query to an externally-provided set of data IDs.

        Notes
        -----
        This generally uploads the given data IDs to a temporary table.  If the
        query context manager has been entered, the temporary table *may* be
        kept alive until it is exited,  rather than re-uploaded on every
        execution, depending on the backend's capabilities.

        This method cannot perform spatial joins or temporal joins; use
        `join_dimensions` first if the new dimensions are related spatially or
        temporally to the query's current dimensions.
        """
        raise NotImplementedError()

    def join_external_uuids(
        self,
        dataset_type: DatasetType | DatasetTypeName,
        uuids: Iterable[uuid.UUID],
        defer: bool | None = None,
    ) -> Query:
        """Join the query to an externally-provided set of dataset UUIDs.

        Notes
        -----
        This generally uploads the given UUIDs to a temporary table.  If the
        query context manager has been entered, the temporary table *may* be
        kept alive until it is exited,  rather than re-uploaded on every
        execution, depending on the backend's capabilities.

        This method requires the given dataset type's UUIDs to have already
        included in the query via `join_datasets`.
        """
        raise NotImplementedError()

    def where(
        self,
        expression: str = "",
        data_id: DataId | None = None,
        uuid: tuple[DatasetType | DatasetTypeName, uuid.UUID] | None = None,
        bind: Mapping[str, Any] | None = None,
        defer: bool | None = None,
    ) -> Query:
        """Apply a filter to the query.

        Parameters
        ----------
        expression, optional
            Expression string, parsed just like our current expressions, though
            I'd like to provide more support for spatial and temporal filtering
            there (relevant here only because that's why there are no args for
            spatial/temporal constraints).
        data_id, optional
            Data ID constraint.
        uuid, optional
            Dataset UUID constraint.
        bind, optional
            Mapping of values for extra identifiers in ``expression``.
        defer, optional
            See `Butler.query`.
        """
        raise NotImplementedError()

    def run(self) -> Query:
        """Run the query, fetch all results, and return a new `Query` holding
        them.

        This also changes the default for ``defer`` to `False` for all methods
        on the returned object.
        """
        raise NotImplementedError()

    def materialized(self, defer: bool | None = None) -> Query:
        """Execute the query into a temporary table.

        If the query's context manager has been entered, this *may* insert
        directly into the temporary table and maintain its lifetime until the
        context manager is exited, but implementations may materialize locally
        and upload every time a derived query is executed.
        """
        raise NotImplementedError()

    def projected(
        self,
        dimensions: Iterable[DimensionName] | None = None,
        unique: bool = True,
        columns: Iterable[ColumnTag] | None = None,
        drop_postprocessing: bool = False,
        keep_record_columns: bool = True,
        defer: bool | None = None,
    ) -> Query:
        """Drop some columns from the Query.

        Parameters
        ----------
        dimensions, optional
            New dimensions.  Must be a subset of the current dimensions.
        columns, optional
            Specific columns to include in the query.
        drop_postprocessing, optional
            If this operation would drop columns that are used by
            postoprocessing operations that occur in the client (usually this
            means further spatial filtering based on region overlaps), drop
            those filtering operations as well; this typically means result
            rows will have less-precise overlap constraints than usual.  If
            `False`, these columns will not be dropped, and at least some of
            the projection and any deduplication will occur client-side.
        keep_record_columns, optional
            If `True`, do not drop dimension record or datastore record columns
            that would be needed to return expanded data IDs or DatasetRefs
            from the columns that are being retained.
        defer, optional
            See `Butler.query`.
        """
        raise NotImplementedError()

    def with_dimension_record_columns(
        self, element: DimensionElementName | None = None, defer: bool | None = None
    ) -> Query:
        """Add dimension record columns to the query that are sufficient to
        make sure it can return either expanded data IDs (if
        ``element is None``) or dimension records for ``element``).

        Notes
        -----
        Dimensions whose records are cached on the client are always considered
        to have all of their columns present in the query, even if they aren't
        actually part of the result mapping.
        """
        raise NotImplementedError()

    def with_datastore_record_columns(
        self, dataset_type_name: DatasetTypeName | None = None, defer: bool | None = None
    ) -> Query:
        """Add opaque table columns to the query that are sufficient to make
        sure it can return expanded DatasetRefs for either the given dataset
        types or all dataset types that have been joined in.
        """
        raise NotImplementedError()

    def sliced(self, start: int = 0, stop: int | None = None, defer: bool | None = None) -> Query:
        """Return an index-sliced version of Query.

        When the query has not been executed, this will perform the slice via
        SQL LIMIT and OFFSET clauses.  If the query has been executed, regular
        sequence indexing will be used.

        The behavior of slicing depends on both the presence or absence of
        duplicates and the ordering of results, and neither of these come with
        any guarantees unless they are explicitly requested (by calling
        `projected` with ``unique=True`` and `sorted`), respectively.
        """
        raise NotImplementedError()

    def sorted(self, order_by: Iterable[str], defer: bool | None = None) -> Query:
        """Return a sorted version of the query.

        When the query has not been executed, this will perform the sort via an
        ORDER BY clause.  If the query has been executed, an in-memory sort
        will be performed.
        """
        raise NotImplementedError()

    def count(self, *, exact: bool = True, discard: bool = False) -> int:
        """Return the number of rows in the query results.

        The count includes duplicates, which the query system makes no
        guarantees about unless specifically requested to (by passing
        ``unique=True`` to `projected`).

        See current Query.count docs for parameter details.
        """
        raise NotImplementedError()

    def any(self, *, execute: bool = True, exact: bool = True) -> bool:
        """Return whether there are any rows in the query results.

        See current Query.any docs for parameter details.
        """
        raise NotImplementedError()


@dataclasses.dataclass
class QueryAdapter(AbstractContextManager[_S]):
    """A base class for `Query` adapt that change the iterated-over type."""

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
    """A query adapter for DatasetRefs of a single type."""

    def expanded(self) -> DatasetQueryAdapter:
        return dataclasses.replace(
            self, query=self.query.with_dimension_record_columns().with_datastore_record_columns()
        )

    def uuids(self) -> Iterable[uuid.UUID]:
        raise NotImplementedError()


@dataclasses.dataclass
class DatasetQueryChain(Iterable[DatasetRef]):
    """A chain of `DatasetQueryAdapter` instance for different DatasetTypes.

    This is not itself a DatasetQueryAdapter, but it mimics the
    DatasetQueryAdapter interface so heterogeneous query results look similar
    to heterogeneous query results when possible.
    """

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

    def uuids(self) -> Iterable[uuid.UUID]:
        raise NotImplementedError()

    def count(self, *, exact: bool = True, discard: bool = False) -> int:
        return sum(q.count(exact=exact, discard=discard) for q in self.by_dataset_type.values())

    def any(self, *, execute: bool = True, exact: bool = True) -> bool:
        return any(q.any(execute=execute, exact=exact) for q in self.by_dataset_type.values())


@dataclasses.dataclass
class DimensionRecordQueryAdapter(QueryAdapter, Iterable[DimensionRecord]):
    """A query adapter for DimensionRecords."""


class DataCoordinateQueryAdapter(QueryAdapter, Iterable[DimensionRecord]):
    """A query adapter for data IDs."""

    def expanded(self) -> DataCoordinateQueryAdapter:
        return dataclasses.replace(self, query=self.query.with_dimension_record_columns())


class CollectionQuery(Sequence[CollectionName]):
    """A query result object for collections.

    I expect this to be backed by a regular Python list or tuple, but haven't
    tried to work out the state in detail.
    """

    def details(
        self,
    ) -> Iterator[
        tuple[CollectionName, CollectionType, CollectionDocumentation, Sequence[CollectionName] | None]
    ]:
        raise NotImplementedError()

    def summarize_types(self) -> Iterable[CollectionType]:
        raise NotImplementedError()


class DatasetTypeQuery(Mapping[DatasetTypeName, DatasetType]):
    """A query result object for dataset types.

    This is a Mapping (a major difference from the result type of the current
    `Registry.queryDatasetTypes`) to provide easy and idiomatic access to just
    the names, which are often all that people care about.

    I expect this to be backed by `dict`, but haven't tried to work out the
    details yet.
    """

    def summarize_storage_classes(self) -> Iterable[StorageClass]:
        raise NotImplementedError()
