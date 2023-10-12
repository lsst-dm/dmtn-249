:tocdepth: 1

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
Since any consistency model will necessarily involve both database and datastore content, enforcing consistency will have to be a :py:class:`~lsst.daf.butler.Butler` responsibility, not a :py:class:`~lsst.daf.butler.Registry` or :py:class:`~lsst.daf.butler.Datastore` responsibility.
In order to ensure that the right parts of that enforcement occur on the server, we are pushed strongly towards making :py:class:`~lsst.daf.butler.Butler` itself polymorphic (with direct/SQL and client/server implementations) rather :py:class:`~lsst.daf.butler.Registry` (with :py:class:`~lsst.daf.butler.Datastore` remaining polymorphic for other reasons).

In :ref:`component-overview`, we describe the planned high-level division of responsibilities for :py:class:`~lsst.daf.butler.Butler`, :py:class:`~lsst.daf.butler.Registry`, and :py:class:`~lsst.daf.butler.Datastore` in the client/server era.
:ref:`consistency-model` describes the new consistency model and introduces the *artifact transaction* as a new, limited-lifetime butler component that plays an important role in maintaining consistency.
In :ref:`use-case-details`, we work through a few important use cases in detail to show how components interact in both client/server and direct-connection contexts.
:ref:`prototype-code` serves as an appendix of sorts with code listings that together form a thorough prototyping of the changes being proposed here.
It is not expected to be read directly, but will be frequently linked to in :ref:`use-case-details` in particular.

Throughout this technote, links to code objects may refer to the existing ones (e.g. :py:class:`~lsst.daf.butler.Butler`) or, more frequently, the prototypes of their replacements defined here (e.g. :py:class:`Butler`).
Existing types that are not publicly documented (e.g. ``SqlRegistry``) and future types that were not prototyped in detail (e.g. ``RemoteButler``) are not linked.
Unfortunately Sphinx formatting highlights linked vs. unlinked much more strongly than old vs. new.

In addition, we note that DMTN-271 :cite:`DMTN-271` provides an in-depth description of changes to pipeline execution we expect to occur on a similar timescale, both enabling and benefiting from the lower-level changes described here.
DMTN-242 :cite:`DMTN-242` will provide more detail about how we will actually implement the changes described, which will have to involve providing backwards-compatible access to heavily-used data repositories while standing up a minimal client/server butler as quickly as possible.

.. _component-overview:

Component Overview
==================

Our plan for the components of the butler system is shown at a high level in :ref:`fig-repository-clients`.


.. figure:: /_static/repository-clients.svg
   :name: fig-repository-clients
   :target: _images/repository-clients.svg
   :alt: Data repositories and their clients

   Data repositories and their clients

The identities and roles of these components is *broadly* unchanged: a :py:class:`Butler` is still a data repository client, and it still delegates SQL database interaction to a ``Registry`` and artifact (e.g. file) storage to a :py:class:`Datastore`.
Many details are changing, however, including which types are polymorphic:

- The current :py:class:`~lsst.daf.butler.Butler` will be split into a :py:class:`Butler` abstract base class and the ``DirectButler`` implementation.
  :py:class:`Butler` will implement much of its public interface itself, while delegating to a few mostly-protected (in the C++/Java sense) abstract methods that must be implemented by derived class.

- The current :py:class:`~lsst.daf.butler.Registry` and ``SqlRegistry`` classes will be merged into a single concrete final ``Registry``, while ``RemoteRegistry`` will be dropped.

- The new ``RemoteButler`` class will provide a new full :py:class:`Butler` implementation that uses a :py:class:`Datastore` directly for ``get``, ``put``, and transfer operations, but it communicates with the database only indirectly via the new Butler REST Server, and also obtains the signed URLs needed to interact with its :py:class:`Datastore` from that server.
  The Butler REST server will also have a :py:class:`Datastore` (used to verify and delete artifacts only).

- In this design the ``Registry`` is just the database-interaction code shared by ``DirectButler`` and the Butler REST Server, and it may ultimately cease to exist in favor of its components being used directly by :py:class:`Butler` implementations.

.. note::

  Note that the :py:attr:`Butler.registry <lsst.daf.butler.Butler.registry>` attribute is *already* a thin shim that will increasingly delegate more and more to public methods on its :py:class:`Butler`, until ultimately all butler functionality will be available without it and its continued existence will depend only on our need for backwards compatibility.

Note that the same data repository may have both ``DirectButler`` and ``RemoteButler``, corresponding to trusted and untrusted users, respectively.
This means the Butler REST Server may not have persistent state (other than caching) that is not considered part of the data repository itself.
This includes locks - we have to rely on SQL database and low-level artifact storage primitives to guarantee consistency in the presence of concurrency.
This implies that a single data repository may interact with multiple Butler REST Servers as well, which is something we definitely want for scalability.

:py:class:`Datastore` will remain an abstract base class with largely the same concrete implementations as today, but instead of being able to fetch and store datastore-specific metadata records in the SQL database itself (currently mediated by a :py:class:`~lsst.daf.butler.registry.interfaces.DatastoreRegistryBridge` instance provided by :py:class:`~lsst.daf.butler.Registry`), it will return those records to :py:class:`Butler` on write and receive it (often as part of a :py:class:`~lsst.daf.butler.DatasetRef`) on read, and its interface will change significantly as a result.
By making it unnecessary for a :py:class:`Datastore` to communicate with the database we make it possible to use the same :py:class:`Datastore` objects in all kinds of :py:class:`Butler` implementations, preserving :py:class:`Datastore` inheritance as an axis for customizing how datasets are actually stored instead.

.. note::

   It is not clear that :py:class:`Datastore` inheritance is *actually* usable for customizing how datasets are actually stored - we have repeatedly found it much easier to add new functionality to :py:class:`~lsst.daf.butler.datastores.fileDatastore.FileDatastore` than to add a new :py:class:`Datastore` implementation.
   And all other concrete datastores are unusual in one sense or another:

   - :py:class:`~lsst.daf.butler.datastores.inMemoryDatastore.InMemoryDatastore` doesn't actually correspond to a data repository (and is now slated for removal);
   - ``SasquatchDatastore`` only exports; it cannot ``get`` datasets back and cannot manage their lifetimes.
   - :py:class:`~lsst.daf.butler.datastores.chainedDatastore.ChainedDatastore` might work better as a layer between :py:class:`Butler` and other datastores if it didn't have to satisfy the :py:class:`Datastore` interface itself.

   As a result, we may be due for a larger rethink of the :py:class:`Datastore` concept and its relationship with :py:class:`Butler` as well, but we will consider that out of scope for this technote, as it isn't necessary for either ``RemoteButler`` development or establishing a data repository consistency model.

:ref:`fig-repository-clients` also includes *workspaces*, a new concept introduced here that will be expanded upon in DMTN-271 :cite:`DMTN-271`.
Workspaces formalize and generalize our use of :py:class:`~lsst.daf.butler.QuantumBackedButler` to provide a limited butler interface to :py:class:`~lsst.pipe.base.PipelineTask` execution that does not require continuous access to the central SQL database :cite:`DMTN-177`, by using (in this case) a :py:class:`~lsst.pipe.base.QuantumGraph` stored in a file to provide metadata instead.
An internal workspace writes processing outputs directly to locations managed by a data repository, and at a low level should be considered an extension of that data repository that defers and batches up database access.
An external workspace has similar high-level behavior, but since it does not write directly to the central data repository, it is more like an indepedent satellite repository that remembers its origin and can (when its work is done) transfer ownership of its datasets back to the central repository.
An external workspace can also be converted into a complete standalone data repository in-place, by creating a SQL database (typically SQLite) from the metadata it holds.
Internal workspaces can only interact with a ``DirectButler``, because they are also a trusted entity that requires unsigned URI access to artifact storage.
External workspaces can be used with any :py:class:`Butler`.
Workspaces are expected to have lifetimes of days or perhaps weeks, and cease to exist when their outputs are committed to a data repository.
Workspaces that use something other than a persisted :py:class:`~lsst.pipe.base.QuantumGraph` for dataset metadata will be supported, but no other concrete workspace implementations are currently planned.

The inheritance relationships for different kinds of butlers is shown in :ref:`fig-butler-inheritance`.
A workspace is associated with a :py:class:`PersistentLimitedButler` implementation, while data repositories must have full :py:class:`Butler` clients.
Direct subclasses :py:class:`LimitedButler` are expected to be useful primarily as a way to add in-memory behaviors (caching, statistics-gathering) while proxying a persistent butler implementation of some kind.

.. figure:: /_static/butler-inheritance.svg
   :name: fig-butler-inheritance
   :target: _images/butler-inheritance.svg
   :alt: Butler inheritance relationships

   Butler inheritance relationships

.. _consistency-model:

Consistency Model
=================

A full data repository has both a SQL *database* and artifact *storage* that are expected to remain consistent at all times.
We define consistency as follows:

1. A dataset may be both *registered* in the database and be *stored* ("have artifacts - e.g. files - in storage") if there are *datastore records* present in the database related to that dataset, *or* if those artifacts are managed by an *artifact transaction*.

2. A dataset may be registered in the database only (and not be stored) only if there are no datastore records in the database related to that dataset.

3. A dataset may be stored without being registered in the database only if it is managed by an artifact transaction.

*Datastore records* are rows in special database tables whose schemas are defined by the datastore configured with the repository.
These must have the dataset ID as at least part of their primary key.
They typically contain information like the formatter class used to read and write the dataset and a URI that points to the artifact, but aside from the dataset ID, the schema is fully datastore-dependent.

An *artifact transaction* is a limited-duration but persistent manifest of
changes to be made to both the database and storage.
All open artifact transactions are registered in the database and can be *committed* or *abandoned* to continue or (at least partially) undo an operation, even in the presence of unexpected hard errors (to the extent underlying the database and low-level storage is recoverable, of course).
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

While most artifact transactions will have very brief durations, and are persisted only for fault-tolerance, internal workspaces open an artifact transaction when created, and thy commit or abandon that transaction only when the workspace itself is committed or abandoned; this is what gives an internal workspace "permission" to write processig-output artifacts directly to data repository locations while deferring the associated database inserts.
External workspaces create (and commit) an artifact transaction only when the processing is complete workspace is committed by transferring artifacts back to the data repository - from the perspective of data repository consistency, this is no different from any other transfer operation.

.. _use-case-details:

Use Case Details
================

TODO

.. _prototype-code:

Prototype Code
==============

.. py:class:: LimitedButler

   .. literalinclude:: prototyping/limited_butler.py
      :language: py
      :pyobject: LimitedButler

.. py:class:: PersistentLimitedButler

   .. literalinclude:: prototyping/persistent_limited_butler.py
      :language: py
      :pyobject: PersistentLimitedButler

.. py:class:: Butler

   .. py:method:: begin_transaction

      .. literalinclude:: prototyping/butler.py
         :language: py
         :pyobject: Butler.begin_transaction

   .. py:method:: commit

      .. literalinclude::  prototyping/butler.py
         :language: py
         :pyobject: Butler.commit

   .. py:method:: abandon

      .. literalinclude::  prototyping/butler.py
         :language: py
         :pyobject: Butler.abandon

   .. py:method:: list_transactions

      .. literalinclude::  prototyping/butler.py
         :language: py
         :pyobject: Butler.list_transactions

   .. py:method:: vacuum_transactions

      .. literalinclude::  prototyping/butler.py
         :language: py
         :pyobject: Butler.vacuum_transactions

.. py:class:: Datastore

   .. py:attribute:: tables

      .. literalinclude:: prototyping/datastore.py
         :language: py
         :pyobject: Datastore.tables

   .. py:method:: extract_existing_uris

      .. literalinclude:: prototyping/datastore.py
         :language: py
         :pyobject: Datastore.extract_existing_uris

   .. py:method:: predict_new_uris

      .. literalinclude:: prototyping/datastore.py
         :language: py
         :pyobject: Datastore.predict_new_uris

   .. py:method:: get_many

      .. literalinclude:: prototyping/datastore.py
         :language: py
         :pyobject: Datastore.get_many

   .. py:method:: initiate_transfer_from

      .. literalinclude:: prototyping/datastore.py
         :language: py
         :pyobject: Datastore.initiate_transfer_from

   .. py:method:: interpret_transfer_to

      .. literalinclude:: prototyping/datastore.py
         :language: py
         :pyobject: Datastore.interpret_transfer_to

   .. py:method:: execute_transfer_to

      .. literalinclude:: prototyping/datastore.py
         :language: py
         :pyobject: Datastore.execute_transfer_to

   .. py:method:: serialize_transfer_to

      .. literalinclude:: prototyping/datastore.py
         :language: py
         :pyobject: Datastore.serialize_transfer_to


   .. py:method:: deserialize_transfer_to

      .. literalinclude:: prototyping/datastore.py
         :language: py
         :pyobject: Datastore.deserialize_transfer_to

   .. py:method:: put

      .. literalinclude:: prototyping/datastore.py
         :language: py
         :pyobject: Datastore.put

   .. py:method:: verify

      .. literalinclude:: prototyping/datastore.py
         :language: py
         :pyobject: Datastore.verify

   .. py:method:: unstore

      .. literalinclude:: prototyping/datastore.py
         :language: py
         :pyobject: Datastore.unstore

.. py:class:: ArtifactTransaction

   .. py:method:: from_header_data

      .. literalinclude:: prototyping/artifact_transaction.py
         :language: py
         :pyobject: ArtifactTransaction.from_header_data

   .. py:method:: get_header_data

      .. literalinclude:: prototyping/artifact_transaction.py
         :language: py
         :pyobject: ArtifactTransaction.get_header_data

   .. py:method:: get_operation_name

      .. literalinclude:: prototyping/artifact_transaction.py
         :language: py
         :pyobject: ArtifactTransaction.get_operation_name

   .. py:method:: get_runs

      .. literalinclude:: prototyping/artifact_transaction.py
         :language: py
         :pyobject: ArtifactTransaction.get_runs

   .. py:method:: get_initial_batch

      .. literalinclude:: prototyping/artifact_transaction.py
         :language: py
         :pyobject: ArtifactTransaction.get_initial_batch

   .. py:method:: get_unstores

      .. literalinclude:: prototyping/artifact_transaction.py
         :language: py
         :pyobject: ArtifactTransaction.get_unstores

   .. py:method:: get_uris

      .. literalinclude:: prototyping/artifact_transaction.py
         :language: py
         :pyobject: ArtifactTransaction.get_uris

   .. py:method:: commit_phase_one

      .. literalinclude:: prototyping/artifact_transaction.py
         :language: py
         :pyobject: ArtifactTransaction.commit_phase_one

   .. py:method:: commit_phase_two

      .. literalinclude:: prototyping/artifact_transaction.py
         :language: py
         :pyobject: ArtifactTransaction.commit_phase_two

   .. py:method:: abandon_phase_one

      .. literalinclude:: prototyping/artifact_transaction.py
         :language: py
         :pyobject: ArtifactTransaction.abandon_phase_one

   .. py:method:: abandon_phase_two

      .. literalinclude:: prototyping/artifact_transaction.py
         :language: py
         :pyobject: ArtifactTransaction.abandon_phase_two



.. rubric:: References

.. bibliography:: local.bib lsstbib/books.bib lsstbib/lsst.bib lsstbib/lsst-dm.bib lsstbib/refs.bib lsstbib/refs_ads.bib
   :style: lsst_aa
