from __future__ import annotations

import enum


class DatasetExistence(enum.Flag):
    UNRECOGNIZED = 0
    REGISTRY = 1
    DATASTORE = 2
    BOTH = 3
