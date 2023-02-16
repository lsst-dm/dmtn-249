from __future__ import annotations

from abc import abstractmethod

from lsst.daf.butler import DatasetIdFactory, DimensionUniverse, StorageClassFactory
from lsst.daf.butler.registry import RegistryDefaults

from .batched_edit import BatchedEdit
from .queries import Query


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
