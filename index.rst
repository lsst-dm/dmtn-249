:tocdepth: 2

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
It is not expected to be read directly, but will be frequently linked to in other sections.

Throughout this technote, links to code objects may refer to the existing ones (e.g. :py:class:`~lsst.daf.butler.Butler`) or, more frequently, the prototypes of their replacements defined here (e.g. :py:class:`Butler`).
Existing types that are not publicly documented (e.g. ``SqlRegistry``) and future types that were not prototyped in detail (e.g. ``RemoteButler``) are not linked.
Unfortunately Sphinx formatting highlights linked vs. unlinked much more strongly than old vs. new, which is the opposite of what we want - but it should not be necessary to follow most linked code entities at all anyway.

In addition, we note that DMTN-271 :cite:`DMTN-271` provides an in-depth description of changes to pipeline execution we expect to occur on a similar timescale, both enabling and benefiting from the lower-level changes described here.
DMTN-242 :cite:`DMTN-242` may be updated in the future to provide more detail about how we will actually implement the changes described, which will have to involve providing backwards-compatible access to heavily-used data repositories while standing up a minimal client/server butler as quickly as possible.

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

- The new ``RemoteButler`` class will provide a new full :py:class:`Butler` implementation that uses a :py:class:`Datastore` directly for ``get``, ``put``, and transfer operations.
  It will communicate with the database only indirectly via a new Butler REST Server.
  It also obtains the signed URLs needed to interact with its :py:class:`Datastore` from that server.
  The Butler REST server will have a :py:class:`Datastore` as well, but will use it only to verify and delete artifacts.

- In this design the ``Registry`` is just the database-interaction code shared by ``DirectButler`` and the Butler REST Server, and it may ultimately cease to exist in favor of its components being used directly by :py:class:`Butler` implementations.

.. note::

  Note that the :py:attr:`Butler.registry <lsst.daf.butler.Butler.registry>` attribute is *already* a thin shim that will increasingly delegate more and more to public methods on its :py:class:`Butler`, until ultimately all butler functionality will be available without it and its continued existence will depend only on our need for backwards compatibility.

A single data repository may have both ``DirectButler`` and ``RemoteButler`` clients, corresponding to trusted and untrusted users.
This means the Butler REST Server may not have persistent state (other than caching) that is not considered part of the data repository itself.
This includes locks - we have to rely on SQL database and low-level artifact storage primitives to guarantee consistency in the presence of concurrency.
This also implies that a single data repository may interact with multiple Butler REST Servers, which is something we definitely want for scalability.

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
An external workspace has similar high-level behavior, but since it does not write directly to the central data repository, it is more like an independent satellite repository that remembers its origin and can (when its work is done) transfer ownership of its datasets back to the central repository.
An external workspace can also be converted into a complete standalone data repository in-place, by creating a SQL database (typically SQLite) from the metadata it holds.
Internal workspaces can only interact with a ``DirectButler``, because they are also a trusted entity that requires unsigned URI access to artifact storage.
External workspaces can be used with any :py:class:`Butler`.
Workspaces are expected to have lifetimes up to days or perhaps weeks, and cease to exist when their outputs are committed to a data repository.
Workspaces that use something other than a persisted :py:class:`~lsst.pipe.base.QuantumGraph` for dataset metadata will be supported, but no other concrete workspace implementations are currently planned.

.. _consistency-model:

Consistency Model
=================

Definitions and Overview
------------------------

A full data repository has both a SQL database and artifact storage that are expected to remain consistent at all times.
A dataset is considered *registered* in the repository if its UUID is associated with a dataset type, data ID, and :py:attr:`~lsst.daf.butler.CollectionType.RUN` collection in the database.
It is considered *stored* in the repository if its UUID is associated with one or more *datastore records* in the database and all artifacts (e.g. files) necessary to fully read it are present.

*Datastore records* are rows in special database tables whose schemas are defined by the datastore configured with the repository.
These must have the dataset ID as at least part of their primary key.
They typically contain information like the formatter class used to read and write the dataset and a URI that points to the artifact, but aside from the dataset ID, the schema is fully datastore-dependent.

Each dataset in a data repository must be in one of the following states at all times:

1. both registered and stored;

2. registered but not stored;

3. managed by an *artifact transaction*.

An *artifact transaction* is a limited-duration but persistent manifest of
changes to be made to both the database and storage.
All open artifact transactions are registered in the database and are closed by *committing*, *reverting*, or *abandoning* them (see :ref:`artifact-transaction-details`).
A dataset that is managed by an artifact transaction:

- may not have any datastore records associated with its UUID;

- may or may not be registered;

- may or may not have associated artifacts present.

An artifact transaction does not correspond to a database transaction - there will actually be one database transaction used to open each artifact transaction and another used to close it.

While most artifact transactions will have very brief durations, and are persisted only for fault-tolerance, internal workspaces open an artifact transaction when created, and they commit, revert, or abandon that transaction only when the workspace itself is committed, reverted, or abandoned; this is what gives an internal workspace "permission" to write processing-output artifacts directly to data repository locations while deferring the associated database inserts.
External workspaces create (and commit) an artifact transaction only when the processing is complete and the workspace is committed by transferring artifacts back to the data repository.
From the perspective of data repository consistency, this is no different from any other transfer operation.

The artifact transaction system relies on low-level database and artifact storage having their own mechanisms to guard against internal corruption and data loss (e.g. backups, replication, etc.), and it assumes that the data committed by successful database transactions and successful artifact writes can be always restored by those low-level mechanisms.
The role of the artifact transaction system is to provide synchronization between two independently fault-tolerant persistent storage systems.

.. _storage-tables:

Storage tables
--------------

The current butler database schema includes ``database_location`` and ``database_location_trash`` tables that this proposal has no need for.

The former was intended as a way to make it possible to query (using the database alone) for whether a dataset is stored by a particular datastore.
The ability to query this table was never implemented, and it is not clear that users should actually care which of several chained datastores actually store a dataset.
Going forward, we intend for the query system to test whether a dataset is stored (in both results and ``where`` expressions) by checking for the presence of the dataset's UUID in any datastore-record table.
The set of which tables to include in that query could be restricted at query-construction time by asking the datastore whether it would ever store a particular dataset type, but at present this would probably be a premature optimization.

The ``database_location_trash`` was intended to aid with consistency when deleting datasets, but it never worked and no longer serves any real purpose.

.. _artifact-transaction-details:

Artifact Transaction Details
----------------------------

Opening a transaction
"""""""""""""""""""""

Artifact transactions are opened by calling :py:meth:`Butler.begin_transaction` with an :py:class:`ArtifactTransaction` instance.
This will frequently happen inside other butler methods, but :py:meth:`~Butler.begin_transaction` is a public method precisely so external code (such as the pipeline execution middleware) can define specialized transaction types - though this may only ever happen in practice in ``DirectButler``, since ``RemoteButler`` will only support transaction types that have been vetted in advance.

Open artifact transactions are represented in the repository database primarily by the ``artifact_transaction`` table:

.. code:: sql

   CREATE TABLE artifact_transaction (
      name VARCHAR PRIMARY KEY,
      data JSON NOT NULL
   );

The ``artifact_transaction`` table has one entry for each open transaction.
In addition to the transaction name, it holds a serialized description of the transaction (:py:class:`ArtifactTransaction` instances are subclasses of :py:class:`pydantic.BaseModel`) directly in its ``data`` column.
Additional tables that provide locking for concurrency are described in :ref:`concurrency-and-locking`.

The database inserts that open an artifact transaction occur within a single database transaction, and :py:class:`ArtifactTransaction.begin` is first given an opportunity to execute certain database operations in this transaction as well (see :ref:`database-only-operations`).

The ``RemoteButler`` implementation of :py:meth:`Butler.begin_transaction` will serialize the transaction on the client and send this serialized form of the transaction to the Butler REST Server, which performs all other interactions with the transaction.
This includes checking whether the user is permitted to run this transaction.

Closing a transaction
"""""""""""""""""""""

A transaction can only be closed by calling :py:meth:`Butler.commit_transaction`, :py:meth:`~Butler.revert_transaction`, or :py:meth:`~Butler.abandon_transaction`.

:py:meth:`Butler.commit_transaction` delegates to :py:meth:`ArtifactTransaction.commit`, and it always attempts to accomplish the original goals of the transaction.
It raises (keeping the transaction open and performing no database operations) if it cannot fully succeed after doing as much as it can.
Commit implementations are given an opportunity to perform additional database-only operations in the same database transaction that deletes the ``artifact_transaction`` rows (see :ref:`database-only-operations`).

:py:meth:`Butler.revert_transaction` (delegating to :py:meth:`ArtifactTransaction.revert`) is the opposite - it attempts to undo any changes made by the transaction (including any changes made when opening it), and it also raises if this is impossible, preferably after undoing all that it can first.
Revert implementations are also given an opportunity to perform additional database-only operations in the same database transaction that deletes the ``artifact_transaction`` rows, but the set of supported database operations supported here is limited to those that invert operations that can be performed in :py:meth:`ArtifactTransaction.begin`.

:py:meth:`Butler.abandon_transaction` (delegating to :py:meth:`ArtifactTransaction.abandon`) reconciles database and artifact state while minimizing the chance of failure; its goal is to only fail if low-level database or artifact storage operations fail.
This means:

 - inserting datastore records for datasets that are already registered and whose artifacts are all present;
 - deleting artifacts that do not comprise a complete and valid dataset.

In ``RemoteButler`` the :py:class:`ArtifactTransaction` closing methods are always run on the server, since this is the only place consistency guarantees can be safely verified.
The transaction definitions are also read by the server.
The server may need to re-check user permission to run the transaction if there's a chance that may have changed, but we may also be able to handle this possibility by requiring the user to have no open artifact transactions when changing their access to various entities.

.. _workspace-transactions:

Workspace transactions
""""""""""""""""""""""

The set of datasets and related artifacts managed by an artifact transaction is usually fixed when the transaction is opened, allowing all dataset metadata needed to implement the transaction to be serialized to an ``artifact_transaction`` row at that time.
A transaction can also indicate that new datasets will be added to the transaction over its lifetime by overriding :py:attr:`ArtifactTransaction.is_workspace` to :py:obj:`True`.
This causes the transaction to be assigned a *workspace root*, a directory or directory-like location where the transaction can write files that describe these new datasets before the artifacts for those datasets are actually written.
The driving use case is :py:class:`~lsst.pipe.base.PipelineTask` execution, for which these files will include the serialized :py:class:`~lsst.pipe.base.QuantumGraph`.
At present we expect only ``DirectButler`` to support workspace transactions - using signed URLs for workspace files is a complication we'd prefer to avoid, and we want to limit the set of concrete artifact transaction types supported by ``RemoteButler`` to a few hopefully-simple critical ones anyway.

A workspace transaction may also provide access to a transaction-defined client object by implementing :py:meth:`ArtifactTransaction.make_workspace_client`; this can be used to provide a higher-level interface for adding datasets (like building and executing quantum graphs).
User code should obtain a client instance by calling :py:meth:`Butler.make_workspace_client` with the transaction name.

When a workspace transaction is opened, the serialized transaction is written to a JSON file in the workspace as well as the ``artifact_transaction`` database table.
This allows :py:meth:`Butler.make_workspace_client` to almost always avoid any database or server calls (it is a :py:func:`classmethod`, so even :py:class:`Butler` startup server calls are unnecessary).
If the transaction JSON file does not exist, :py:meth:`Butler.make_workspace_client` *will* have to query the ``artifact_transaction`` table to see if the transaction does, and recreate the file if it does.
This guards against two rare failure modes in workspace construction:

- When a workspace transaction is opened, we register the transaction with the database before creating the workspace root and the transaction JSON file there; this lets us detect concurrent attempts to open the same transaction and ensure only one of those attempts tries to perform the workspace writes.
  But it also makes it possible that a failure will occur after the transaction has already been registered, leaving the workspace root missing.

- When a workspace transaction is closed, we delete the transaction JSON file just before removing the transaction from the database.
  This prevents calls to :py:meth:`Butler.make_workspace_client` from succeeding during or after its deletion (since deleting the transaction JSON file can fail).

This scheme does not protect against concurrency issues occurring within a single workspace, which are left to transaction and workspace client implementations and the higher-level code that uses them.
For example, a workspace client obtained before the transaction is closed can still write new workspace files and datastore artifacts without any way of knowing that the transaction has closed.
This is another reason internal workspaces will be not be supported by ``RemoteButler``.

The workspace root will be recursively deleted by the :py:class:`Butler` after its transaction closes, with the expectation that its contents will have already been translated into database content or artifacts (or are intentionally being dropped).
This can only be done after the closing database transaction concludes, since we need to preserve the workspace state in case the database transaction fails.
In the rare case that workspace root deletion fails after the artifact transaction has been removed from the database, we still consider the transaction closed, and we provide :py:meth:`Butler.vacuum_workspaces` as a way to scan for and remove those orphaned workspace roots.

.. _concurrency-and-locking:

Concurrency and locking
"""""""""""""""""""""""

The artifact transaction system described in previous sections is sufficient to maintain repository consistency only when the changes made by concurrent transactions are disjoint.
To guard against race conditions, we need to introduce some locking.
Database tables that associate datasets or :py:attr:`~lsst.daf.butler.CollectionType.RUN` collections with a single transaction (enforced by unique constraints) are an obvious choice.

Per-dataset locks would be ideal for maximizing parallelism, but expensive to implement in the database - to prevent competing writes to the same artifacts, we would need the lock tables to implement the full complexity of the ``dataset_tags_*`` tables to prevent ``{dataset type, data ID, run}`` conflicts as well as UUID conflicts, since the former are what provide uniqueness to artifact URIs.
Coarse per-:py:attr:`~lsst.daf.butler.CollectionType.RUN` locking is much cheaper, but a major challenge for at least one major use case and possibly a few others:

- Prompt Processing needs each worker to be able to transfer its own outputs back to the same :py:attr:`~lsst.daf.butler.CollectionType.RUN` collection in parallel.

- Service-driven raw-ingest processes may need to ingest each file independently and in parallel, and modify a single, long-lived :py:attr:`~lsst.daf.butler.CollectionType.RUN`.

- Transfers between data facilities triggered by Rucio events may also need to perform multiple ingests into the same :py:attr:`~lsst.daf.butler.CollectionType.RUN` in parallel.

It is important to note that what is missing from Prompt Processing (and possibly the others) is sequence-point hook that could run :py:meth:`Butler.commit_transaction` to modify the database, close the transaction, and then possibly open a new one.
When such a sequence-point hook is available, a single transaction could be used to wrap parallel *artifact* transfers that do the vast majority of the work, with only the database operations and artifact verification run in serial.
This is what we expect batch- and user-driven ingest/import/transfer operations to do (to the extent those need parallelism at all).

To address these use cases we propose using two tables to represent locks:

.. code:: sql

   CREATE TABLE artifact_transaction_modified_run (
      transaction_name VARCHAR NOT NULL REFERENCES artifact_transaction (name),
      run_name VARCHAR PRIMARY KEY
   );

   CREATE TABLE artifact_transaction_insert_only_run (
      transaction_name VARCHAR PRIMARY KEY REFERENCES artifact_transaction (name),
      run_name VARCHAR PRIMARY KEY
   );

The ``artifact_transaction_modified_run`` table provides simple locking that associates a :py:attr:`~lsst.daf.butler.CollectionType.RUN` with at most one artifact transaction.
It would be populated from the contents of :py:meth:`ArtifactTransaction.get_modified_runs` when the transaction is opened, preventing the opening database transaction from succeeding if there are any competing artifact transactions already open.

The ``artifact_transaction_insert_only_run`` table is populated by :py:meth:`ArtifactTransaction.get_insert_only_runs`, which should include only :py:attr:`~lsst.daf.butler.CollectionType.RUN` collections whose datasets are inserted via call to the :py:class:`ArtifactTransactionOpenContext.insert_new_datasets <ArtifactTransactionOpenContext>` method, and not modified by the transaction in any other way.
Inserting new datasets in exactly this way will also cause the opening database transaction to fail (due to a unique constraint violation) if any dataset already exists with the same ``{dataset type, data ID, run}`` combination, and it happens to be exactly what the challenging use cases would naturally do.
This allows us to drop the unique constraint on ``run_name`` alone in this table, and permit multiple artifact transactions writing to the same run to coexist.

We do still need to track the affected :py:attr:`~lsst.daf.butler.CollectionType.RUN` collections to ensure they do not appear in ``artifact_transaction_modified_run``, which is why ``artifact_transaction_insert_only_run`` still needs to exist.
When an artifact transaction is opened, the butler should verify that ``run_name`` values in those two tables are disjoint.
This may require the opening database transaction to be performed with ``SERIALIZABLE`` isolation.

.. _database-only-operations:

Database-only operations
""""""""""""""""""""""""

Artifact transactions are given an opportunity to perform certain database-only operations both in :py:meth:`~ArtifactTransaction.begin` and in their closing methods, to make high-level operations that include both artifact and database-only modifications atomic.
The set of operations permitted when opening and closing artifact transactions reflects an attempt to balance a few competing priorities:

- Inserting datasets early is necessary for our limited per-dataset locking scheme, but datasets can only be inserted if their :py:attr:`~lsst.daf.butler.CollectionType.RUN` collection and all dimension records for their data IDs already exist.

- Performing likely-to-fail database operations early causes those failures to prevent the artifact transaction from being opened, before any expensive and hard-to-revert artifact writes are performed.

- Performing database operations early makes it a challenge (at best) to implement to implement :py:meth:`~ArtifactTransaction.revert`.
  Idempotent database operations like ``INSERT ... ON CONFLICT IGNORE`` and ``DELETE FROM ... WHERE ...`` cannot know which rows they actually affected and hence which modifications to undo - at least not until after the initial database transaction is committed, which is too late to modify the serialized artifact transaction.
  This defeats the purpose of including these operations with the artifact transaction at all.

- Even non-idempotent database operations performed early must reckon with the possibility of another artifact transaction (or database-only butler method) performing overlapping writes before the artifact transaction is closed, unless we prohibit them with our own locking.

.. note::

   Dataset type registration is never included as part of an artifact transaction because it can require new database tables to be created, and that sometimes needs to be done in its own database transaction.

:py:class:`ArtifactTransactionOpenContext` defines the operations available to :py:meth:`~ArtifactTransaction.begin` to be limited to non-idempotent dataset and collection registration (i.e. raising if those entities already exist) and datastore record removal.
Dataset insertion sometimes has to occur early for fine-grained locking, and in other cases we want it to run early because its typical failure modes - foreign key violations (invalid data IDs) and ``{dataset type, data ID, run}`` unique constraint violations - are problems we want to prevent us from writing artifacts as early as possible.
In order to insert datasets early, we also need to provide the ability to add :py:attr:`~lsst.daf.butler.CollectionType.RUN` collections early.
Dataset insertion can also depend on dimension record presence, but since these usually require idempotent inserts and are problematic to remove, we require dimension-record insertion to occur in a separate database transaction before the artifact transaction begins.
Removing datastore records at the start of an artifact transaction is not really a "database-only" operation; it is required in order to remove the the associated artifacts during that transaction.

:py:class:`ArtifactTransactionCloseContext` supports only datastore record insertion, since that is all :py:meth:`~ArtifactTransaction.abandon` is permitted to do.
It also provides a convenience method for calling :py:meth:`Datastore.verify` on a mapping of datasets, since that proved quite useful in prototyping.

:py:class:`ArtifactTransactionRevertContext` extends the options available to
:py:meth:`~ArtifactTransaction.revert` to removing dataset registrations and removing collection registrations; these are the inverses of the only operations supported by :py:meth:`~ArtifactTransaction.begin`.

:py:class:`ArtifactTransactionCommitContext` extends this further to also allow :py:meth:`~ArtifactTransaction.commit` to create and modify :py:attr:`~lsst.daf.butler.CollectionType.CHAINED`, :py:attr:`~lsst.daf.butler.CollectionType.TAGGED`, and :py:attr:`~lsst.daf.butler.CollectionType.CALIBRATION` collections, and to register new datasets.
The only reason we might want to perform those collection operations early would be to fail early if they violate constraints, but this is outweighed by the fact that they are impossible to manually undo safely (most are idempotent) and (unlike datasets) are not protected from concurrent modifications by locking.
And unlike dataset-insertion constraint violations, errors in these operations rarely suggest problems that need to block artifacts from being written.
Inserting new datasets when committing rather than when opening an artifact transaction is support specifically for internal workspaces, which generally do not have a complete list of datasets whose artifacts they will write when their transaction is opened.
Ensuring consistency in :py:attr:`~lsst.daf.butler.CollectionType.CHAINED` collection operations in particular may require the closing database transaction to use ``SERIALIZABLE`` isolation.

Adding further support for dimension-record insertion in :py:meth:`~ArtifactTransaction.commit` would not be problematic, but it's not obviously useful, since dimension records usually need to be present before datasets are inserted.

.. _use-case-details:

Use Case Details
================

.. _use-case-butler-put:

``Butler.put``
--------------

.. note::

   Almost all ``put`` calls today happen in the context of pipeline execution, but our intent is to ultimately make all task execution go through the workspace concept introduced in :ref:`component-overview` (and described more fully in DMTN-271 :cite:`DMTN-271`).
   The remaining use cases for ``put`` include RubinTV's best-effort image processing (which should probably use workspaces as well), certain curated-calibration ingests, and users puttering around in notebooks.

The prototype implementation of :py:class:`Butler.put_many` (a new API we envision ``put`` delegating to) begins by expanding the data IDs of all of the given :py:class:`~lsst.daf.butler.DatasetRef` objects is given.
A dictionary mapping UUID to :py:class:`~lsst.daf.butler.DatasetRef` is then used to construct a :py:class:`PutTransaction` instance to pass to :py:meth:`Butler.begin_transaction`.

The transaction state is just a mapping of :py:class:`~lsst.daf.butler.DatasetRef` objects.
The :py:meth:`~ArtifactTransaction.begin` implementation for this transaction registers the new datasets.
This provides fine-grained locking (as described in :ref:`concurrency-and-locking`) on success and forces the operation to fail early and prevent the transaction from ever being opened if this violates a constraint, such as an invalid data ID value or a ``{dataset type, data ID, collection}`` uniqueness failure.
If the artifact transaction is opened successfully, the new datasets appear *registered* but *unstored* to queries throughout the transactions' lifetime.

After the transaction has been opened, :py:class:`Butler.put_many` calls :py:meth:`Datastore.predict_new_uris` and :py:meth:`Butler._get_resource_paths` to obtain all signed URLs needed for the writes.
In ``DirectButler``, :py:meth:`~Butler._get_resource_paths` just concatenates the datastore root with the relative path instead.
These URLs are passed to :py:meth:`Datastore.put_many` to write the actual artifacts directly to their permanent locations.
If these datastore operations succeed, :py:meth:`Butler.commit_transaction` is called.
This calls the transaction's :py:meth:`~ArtifactTransaction.commit` method (on the server for ``RemoteButler``, in the client in ``DirectButler``), which calls :py:class:`Datastore.verify` on all datasets in the transaction.
Because these :py:class:`~lsst.daf.butler.DatasetRef` objects do not have datastore records attached, :py:class:`Datastore.verify` is responsible for generating them (e.g. regenerating URIs, computing checksums and sizes) as well as checking that these artifacts all exist.
The datastore records are inserts into the database as the artifact transaction is closed, with no additional database operations performed.

All operations after the transaction's opening occur in a ``try`` block that calls :py:class:`Butler.revert_transaction` if an exception is raised.
The :py:meth:`~ArtifactTransaction.revert` implementation calls :py:meth:`Datastore.unstore` to remove any artifacts that may have been written.
If this succeeds, it provides strong exception safety; the repository is left in the same condition it was before the transaction was opened.
If it fails - as would occur if the database or server became unavailable or artifact storage became unwriteable - a special exception is raised (chained to the original error) notifying the user that the transaction has been left open and must be cleaned up manually.
The datasets registered when the transaction was opened are then removed.

In the case of :py:class:`PutTransaction`, a revert should always be possible as long as the database and artifact storage systems are working normally, and the new datasets have not been added to any :py:attr:`~lsst.daf.butler.CollectionType.TAGGED` or :py:attr:`~lsst.daf.butler.CollectionType.CALIBRATION` collections.

.. note::

   This technote assumes we will actually implement :jira:`DM-33635` and make it an error to attempt to remove a dataset while it is still referenced by a :py:attr:`~lsst.daf.butler.CollectionType.TAGGED` or :py:attr:`~lsst.daf.butler.CollectionType.CALIBRATION` collection.

As always, abandoning the failed transaction is another option.
The :py:meth:`~ArtifactTransaction.abandon` implementation for ``put`` is quite similar to the :py:meth:`~ArtifactTransaction.commit` implementation; it differs only in that it exits without error instead of raising an exception when some artifacts are missing.
It still inserts datastore records only for the datasets whose artifacts are already present (as is necessary for consistency guarantees), and it deletes the rest completely.
This leaves all dataset registrations in place (stored or unstored as appropriate), ensuring that :py:meth:`~ArtifactTransaction.abandon` can succeed even when those datasets have already been referenced in :py:attr:`~lsst.daf.butler.CollectionType.TAGGED` or :py:attr:`~lsst.daf.butler.CollectionType.CALIBRATION` collections.

.. _use-case-removing-artifacts:

Removing artifacts
------------------

The prototype includes a :py:meth:`Butler.remove_datasets` method that can either fully remove datasets (``purge=True``) or merely unstore them (``purge=False``).
This method begins by expanding all given :py:class:`~lsst.daf.butler.DatasetRef` objects, which includes both expanding their data IDs and attaching existing datastore records.
These are used to construct and begin a :py:class:`RemovalTransaction`.
The state for this transaction is again a mapping of :py:class:`~lsst.daf.butler.DatasetRef` objects, along with the boolean ``purge`` flag.

The :py:class:`RemovalTransaction` :py:meth:`~ArtifactTransaction.begin` implementation removes all datastore records for its artifacts, as required for datasets managed by an artifact transaction.
As in :py:class:`PutTransaction`, we want the datasets managed by the artifact transaction to appear as registered but unstored while the artifact transaction is open.
Because :py:class:`RemovalTransaction` performs modifications other than dataset insertions, it must use coarse :py:attr:`~lsst.daf.butler.CollectionType.RUN` locking and implements to :py:meth:`ArtifactTransaction.get_modified_runs` to return all :py:attr:`~lsst.daf.butler.CollectionType.RUN` collections that hold any of the datasets it intends to delete.

In this case there is nothing to do with the transaction after it has been opened besides commit it.
The :py:meth:`~ArtifactTransaction.commit` implementation delegates to :py:meth:`Datastore.unstore` to actually remove all artifacts, and if ``purge=True`` it also fully removes these those datasets from the database.
In addition to the ever-present possibility low-level failures, :py:meth:`Butler.commit_transaction` can also fail if (``purge=True``) and any dataset is part of a :py:attr:`~lsst.daf.butler.CollectionType.TAGGED` or :py:attr:`~lsst.daf.butler.CollectionType.CALIBRATION` collection.

If the commit operation fails, a ``try`` block in :py:meth:`Butler.remove_datasets` attempts a :py:meth:`~ArtifactTransaction.revert` in order to try to provide strong exception safety, but this will frequently fail, since it requires all artifacts to still be present, and hence works only if the error occurred quite early *and* the :py:meth:`Datastore.verify` calls in :py:meth:`~ArtifactTransaction.revert` still succeed.
More frequently we expect failures in removal that occur after the transaction is opened to result in the transaction being left open and resolution left to the user, again with a special exception raised to indicate this state.

Removals due to low-level failures can be retried by calling :py:meth:`Butler.commit_transaction`; this can also be used after removing references to the dataset in :py:attr:`~lsst.daf.butler.CollectionType.TAGGED` or :py:attr:`~lsst.daf.butler.CollectionType.CALIBRATION` collections to complete a purge.

The :py:meth:`~ArtifactTransaction.abandon` implementation for removals is almost identical to the one for ``put``: :py:meth:`Datastore.verify` is used to identify which datasets still exist and which have been removed, and the datastore records for those still present are returned so they can be inserted into the database when the transaction is closed.
When abandoning a removal we leave datasets as registered but unstored when their artifacts are missing, since this is closer to the state or the repository when the transaction was opened and avoids any chance of failure due to :py:attr:`~lsst.daf.butler.CollectionType.TAGGED` or :py:attr:`~lsst.daf.butler.CollectionType.CALIBRATION` associations.

A subtler difference between ``put`` and removal is that the :py:class:`~lsst.daf.butler.DatasetRef` objects held by :py:class:`RemovalTransaction` include their original datastore records, allowing :py:meth:`Datastore.verify` (in both :py:meth:`~ArtifactTransaction.abandon` and :py:meth:`~ArtifactTransaction.revert`) to guard against unexpected changes (e.g. by comparing checksums), while in :py:class:`PutTransaction` all :py:meth:`Datastore.verify` can do is generate new records.

.. _use-case-transfers:

Transfers
---------

Transfers, ingests, and imports are not fully prototyped here because they're broadly similar to ``put`` from the perspective of the transaction system - a transaction is opened, artifacts are written by code outside the transaction system by direct calls to :py:class:`Datastore` methods, and then the transaction is committed with revert and abandon also behaving similarly.
In particularly simple cases involving new-dataset transfers only, the :py:class:`PutTransaction` implementation prototyped here may even be usable as-is, with a datastore ingest operation swapped in for the call to :py:class:`Datastore.put_many` that occurs within the transaction lifetime but outside the :py:class:`ArtifactTransaction` object itself.

.. _prototype-code:

Prototype Code
==============

.. py:class:: LimitedButler

   .. literalinclude:: prototyping/limited_butler.py
      :language: py
      :pyobject: LimitedButler

.. py:class:: Butler

   .. py:method:: get_many

      .. literalinclude:: prototyping/butler.py
         :language: py
         :pyobject: Butler.get_many

   .. py:method:: put_many

      .. literalinclude:: prototyping/butler.py
         :language: py
         :pyobject: Butler.put_many

   .. py:method:: expand_existing_dataset_refs

      .. literalinclude:: prototyping/butler.py
         :language: py
         :pyobject: Butler.expand_existing_dataset_refs

   .. py:method:: expand_data_coordinates

      .. literalinclude:: prototyping/butler.py
         :language: py
         :pyobject: Butler.expand_data_coordinates

   .. py:method:: remove_datasets

      .. literalinclude:: prototyping/butler.py
         :language: py
         :pyobject: Butler.remove_datasets

   .. py:method:: begin_transaction

      .. literalinclude:: prototyping/butler.py
         :language: py
         :pyobject: Butler.begin_transaction

   .. py:method:: commit_transaction

      .. literalinclude::  prototyping/butler.py
         :language: py
         :pyobject: Butler.commit_transaction

   .. py:method:: revert_transaction

      .. literalinclude::  prototyping/butler.py
         :language: py
         :pyobject: Butler.revert_transaction

   .. py:method:: abandon_transaction

      .. literalinclude::  prototyping/butler.py
         :language: py
         :pyobject: Butler.abandon_transaction

   .. py:method:: list_transactions

      .. literalinclude::  prototyping/butler.py
         :language: py
         :pyobject: Butler.list_transactions

   .. py:method:: make_workspace_client

      .. literalinclude::  prototyping/butler.py
         :language: py
         :pyobject: Butler.make_workspace_client

   .. py:method:: vacuum_workspaces

      .. literalinclude::  prototyping/butler.py
         :language: py
         :pyobject: Butler.vacuum_workspaces

   .. py:method:: _get_resource_paths

      .. literalinclude::  prototyping/butler.py
         :language: py
         :pyobject: Butler._get_resource_paths

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

   .. py:method:: put_many

      .. literalinclude:: prototyping/datastore.py
         :language: py
         :pyobject: Datastore.put_many

   .. py:method:: verify

      .. literalinclude:: prototyping/datastore.py
         :language: py
         :pyobject: Datastore.verify

   .. py:method:: unstore

      .. literalinclude:: prototyping/datastore.py
         :language: py
         :pyobject: Datastore.unstore

.. py:class:: ArtifactTransaction

   .. py:attribute:: is_workspace

      .. literalinclude:: prototyping/artifact_transaction.py
         :language: py
         :pyobject: ArtifactTransaction.is_workspace

   .. py:method:: make_workspace_client

      .. literalinclude:: prototyping/artifact_transaction.py
         :language: py
         :pyobject: ArtifactTransaction.make_workspace_client

   .. py:method:: get_operation_name

      .. literalinclude:: prototyping/artifact_transaction.py
         :language: py
         :pyobject: ArtifactTransaction.get_operation_name

   .. py:method:: get_insert_only_runs

      .. literalinclude:: prototyping/artifact_transaction.py
         :language: py
         :pyobject: ArtifactTransaction.get_insert_only_runs

   .. py:method:: get_modified_runs

      .. literalinclude:: prototyping/artifact_transaction.py
         :language: py
         :pyobject: ArtifactTransaction.get_modified_runs

   .. py:method:: begin

      .. literalinclude:: prototyping/artifact_transaction.py
         :language: py
         :pyobject: ArtifactTransaction.begin

   .. py:method:: commit

      .. literalinclude:: prototyping/artifact_transaction.py
         :language: py
         :pyobject: ArtifactTransaction.commit

   .. py:method:: revert

      .. literalinclude:: prototyping/artifact_transaction.py
         :language: py
         :pyobject: ArtifactTransaction.revert

   .. py:method:: abandon

      .. literalinclude:: prototyping/artifact_transaction.py
         :language: py
         :pyobject: ArtifactTransaction.abandon

.. py:class:: ArtifactTransactionOpenContext

   .. literalinclude:: prototyping/artifact_transaction.py
      :language: py
      :pyobject: ArtifactTransactionOpenContext

.. py:class:: ArtifactTransactionCloseContext

   .. literalinclude:: prototyping/artifact_transaction.py
      :language: py
      :pyobject: ArtifactTransactionCloseContext

.. py:class:: ArtifactTransactionRevertContext

   .. literalinclude:: prototyping/artifact_transaction.py
      :language: py
      :pyobject: ArtifactTransactionRevertContext

.. py:class:: ArtifactTransactionCommitContext

   .. literalinclude:: prototyping/artifact_transaction.py
      :language: py
      :pyobject: ArtifactTransactionCommitContext

.. py:class:: PutTransaction

   .. literalinclude:: prototyping/put_transaction.py
      :language: py
      :pyobject: PutTransaction

.. py:class:: RemovalTransaction

   .. literalinclude:: prototyping/removal_transaction.py
      :language: py
      :pyobject: RemovalTransaction


.. rubric:: References

.. bibliography:: local.bib lsstbib/books.bib lsstbib/lsst.bib lsstbib/lsst-dm.bib lsstbib/refs.bib lsstbib/refs_ads.bib
   :style: lsst_aa
