:tocdepth: 1

.. py:currentmodule:: lsst.daf.butler

.. sectnum::

.. Metadata such as the title, authors, and description are set in metadata.yaml

Introduction
============

The current boundaries between the Butler and its registry and datastore components are under strain in a number of different ways.
Failure recovery during deletion operations has long been in bad shape, and much of the current "trash"-based system is currently just unused complexity.
Butler client/server will require new approaches to atomic operations and managing operation latency (including caching), and :jira:`RFC-888` has recently shown that we may want to move away from the registry component providing public APIs even outside of the client/server.
This technical note will propose a new high-level organization of butler interfaces and responsibilities to address these concerns.

The current boundary between the registry and datastore components was set up with two principles in mind:

- a dataset has to be added to the registry first, so it can take responsibility for generating a (at the time autoincrement integer) unique dataset ID;
- we should use database transactions that are not committed until datastore operations are completed to maintain consistency across the two components.

The first is no longer true - we've switched to UUIDs specifically to support writing to datastore first, via BPS database-free schemes like execution butler and quantum-backed butler :cite:`DMTN-177`.
Using database transactions has never really worked for deletes, fundamentally because datastore operations cannot reliably be rolled back if the database commit itself fails.
Our attempt to work around this with a system that moves datasets to a "trash" table had considerable problems of its own, leaving us with no real attempt to maintain integrity between datastore and registry.
Even for ``put`` calls, starting a transaction before the datastore write begins is problematic because it keeps database transactions open longer than we'd like.

The upcoming client/server (also referred to as the "remote" or "http" butler)
work is the impetus for most of the changes we propose here, even though the
consistency issues we are trying to solve are long-standing.
We will need to consider the client/server architecture in the design work to
fix those issues, and a major piece of this is that we only trust the server to maintain our consistency model.
Since any consistency model will necessarily involve both database and datastore content, enforcing consistency will have to be a `Butler``responsibility, not a `Registry` or `Datastore` responsibility.
In order to ensure that the right parts of that enforcement occur on the server, we are pushed strongly towards making `Butler` itself polymorphic (with direct/SQL and client/server implementations) rather `Registry` (with `Datastore` remaining polymorphic for other reasons).

If :ref:`component-overview`, we describe the planned high-level division of responsibilities for `Butler`, `Registry`, and `Datastore` in the client/server era.
:ref:`consistency-model` describes the new consistency model and introduces the *artifact transaction* as a new, limited-lifetime butler component that plays an important role in maintaining consistency.
In :ref:`use-case-details`, we work through a few important use cases in detail to show how components interact in both client/server and direct-connection contexts.
:ref:`prototype-code` serves as an appendix of sorts with code listings that together form a thorough prototyping of the changes being proposed here.
It is not expected to be read directly, but will be frequently linked to in :ref:`use-case-details` in particular.

In addition, we note that DMTN-271 :cite:`DMTN-271` provides an in-depth description of changes to pipeline execution we expect to occur on a similar timescale, both enabling and benefiting from the lower-level changes described here.
DMTN-242 :cite:`DMTN-242` will provide more detail about how we will actually implement the changes described, which will have to involve providing backwards-compatible access to heavily-used data repositories while standing up a minimal client/server butler as quickly as possible.

.. _component-overview:

Component Overview
==================

TODO

.. _consistency-model:

Consistency Model
=================

A data repository has both a *database* and *storage* that are expected to remain consistent at all times.

1. A dataset may be both *registered* in the database and be *stored* ("have artifacts (e.g. files) in storage") if there are *datastore records* present in the database related to that dataset, *or* if those artifacts are managed by an *artifact transaction*.

2. A dataset may be *registered* in the database only (and not be stored) only if there are no datastore records in the database related to that dataset.

3. A dataset may be stored without being registered in the database only if it is managed by an artifact transaction.

*Datastore records* are rows in special database tables whose schemas are defined by the datastore configured with the repository.
These must have the dataset ID as part of their primary key.
They typically contain information like the formatter class used to read and write the dataset and a URI that points to the artifact, but aside from the dataset ID, the schema is fully datastore-dependent.

An *artifact transaction* is a limited-duration but persistent manifest of
changes to be made to both the database and storage. All open artifact
transactions are registered in the database and can be *committed* or
*abandoned* to continue or (at least partially) undo an operation, even in the presence of unexpected hard errors (to the extent underlying the database and low-level storage is recoverable, of course).
An artifact transaction does not correspond to a database transaction - in practice there will be one database transaction used when opening a transaction and another used when closing (committing/abandoning) it.

This consistency model means that we *only* write new artifacts with the following pattern:

1. Open a new artifact transaction.
2. Perform writes to storage.
3. Commit the transaction at the same time that datastore records are inserted.

Deleting artifacts is not quite symmetric, because we do not expect this to be reversible at a low level.
For these the pattern is:

1. Open a new artifact transaction and delete datastore records at the same time.
2. Perform the actual artifact deletions.
3. Commit the transaction (which does not modify datastore records in the database at all).

Abandoning a deletion transaction would attempt to re-insert the datastore records for any artifacts that had not yet been deleted.

While most artifact transactions will have very brief durations, and are persisted only for fault-tolerance, we have at least one very important use case for long-lived transactions: no-database execution of quantum graphs while writing directly to data repository storage.
For this use case we will open a long-lived artifact transaction before execution begins and commit it when execution completes (with the low-level operations performed by the commit operation corresponding to the batch "merge jobs" today).
DMTN-271 :cite:`DMTN-271` will cover this use case in much greater detail, including our plan to use this "workspace" approach for all task execution, not just batch.

.. _use-case-details:

Use Case Details
================

TODO

.. _prototype-code:

Prototype Code
==============

TODO


.. rubric:: References

.. bibliography:: local.bib lsstbib/books.bib lsstbib/lsst.bib lsstbib/lsst-dm.bib lsstbib/refs.bib lsstbib/refs_ads.bib
   :style: lsst_aa
