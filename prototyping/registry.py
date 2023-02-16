from __future__ import annotations

from abc import abstractmethod

from lsst.daf.butler import DimensionUniverse, StorageClassFactory, DatasetIdFactory
from lsst.daf.butler.registry import RegistryDefaults

from .queries import Query
from .batched_edit import BatchedEdit


class Registry:

    dimensions: DimensionUniverse
    defaults: RegistryDefaults
    is_writeable: bool
    storage_class_factory: StorageClassFactory
    dataset_id_factory: DatasetIdFactory

    @abstractmethod
    def query(self) -> Query:
        raise NotImplementedError()

    @abstractmethod
    def apply_edit(self, edit: BatchedEdit) -> None:
        raise NotImplementedError()

    @abstractmethod
    def clear_caches(self) -> None:
        raise NotImplementedError()
