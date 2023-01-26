:tocdepth: 1

.. sectnum::

.. Metadata such as the title, authors, and description are set in metadata.yaml

.. TODO: Delete the note below before merging new content to the main branch.

.. note::

   **This technote is a work-in-progress.**

Abstract
========

The current boundaries between the Butler and its Registry and Datastore components are under strain in a number of different ways.
Failure recovery during deletion operations has long been in bad shape, and much of the current "trash"-based system is currently just unused complexity.
Butler client/server will require new approaches to atomic operations and managing operation latency (including caching), and RFC-901 has recently shown that we may want to move away from the Registry component providing public APIs even outside of the client/server.
The provenance system proposed in :cite:`DMTN-205` can also impact these boundaries, especially if we want to extend its notion of QuantumGraph storage in data repositories to graphs that have not yet been fully executed; one possibility here is to add a third component that acts as a more graph-oriented Registry.
This technote will propose a new high-level organization of Butler interfaces and responsibilities to address these concerns.

Consistency across Registry and Datastore
=========================================

Current defects
---------------

The current boundary between Registry and Datastore was set up with two principles in mind:

- a dataset has to be added to Registry first, so it can take responsibility for generating a (at the time autoincrement integer) unique dataset ID;
- we should use database transactions that are not committed until Datastore operations are completed to maintain consistency across the two components.

The first is no longer true - we've switched to UUIDs instead - specifically to support writing to Datastore first, via BPS database-free schemes like execution butler (see :cite:`DMTN-177`).
Using database transactions has never really worked for deletes, fundamentally because Datastore operations cannot reliably be rolled back if the database commit itself fails.
Our attempt to work around this with a system that moves datasets to a "trash" table had considerable problems of its own, leaving us with no real attempt to maintain integrity between Datastore and Registry.
Even for ``put`` calls, starting a transaction before the Datastore write begins is problematic because it keeps database transactions open longer than we'd like.

High-level proposal
-------------------

I propose we adopt the following consistency principles instead.

1. A dataset can be present in either Registry or Datastore without being present in the other.

2. A dataset present in Datastore alone must have a data ID that is valid in the Registry for that data repository (i.e. it use valid dimension values) and it must not have any Datastore records in the Registry database.
   This state is expected to be transitory, either intentionally (e.g. during batch execution, before datasets are transferred back), or as a result of failures we cannot rigorously prevent.
   Datasets in this state as a result of failures or abandoned batch runs are considered undesirable but tolerable, and an approach to minimizing them will be introduced later in :ref:`adding_journal_files`.

2. When a dataset is present in both Registry and Datastore, the Registry is fully responsible for storage of Datastore records.
   It will be the job of the Butler to accept records from Datastore and pass them to Registry on ``put``, and to fetch them from Registry and pass them to Datastore on ``get``.
   Datasets in this state must always have Datastore records present in the registry, even if the Datastore otherwise has no need for records; this allows a database query to reliably return only datasets that actually exist in a Datastore via a join against the record tables.

3. A dataset present in Registry alone must have no Datastore records.
   This is expected to be a long-term state for datasets that were temporary intermediates during processing that nevertheless need to be present in the Registry for provenance recording.

This would allow us to completely remove the ``DatastoreRegistryBridge`` interface and the ``dataset_location`` and ``dataset_location_trash`` tables it manages.
Instead, we would add a new method to get record schema information from a Datastore instance (which Butler would pass to Registry when repositories are created), which would always be required to include a dataset UUID column.
We could use that information with the new ``daf_relation`` classes to easily integrate them with the query system, allowing user queries to not just test for Datastore existence, but query on and report Datastore specific-fields like file size.
We'd also of course provide a way for users to inspect which such fields are available, since Datastore record fields can change from implementation to implementation.

Datastore methods that add new datasets to the repository would be modified to return a collection of records describing those datasets, again for Butler to pass to Registry.
Datastore methods that read datasets or interpret the records describing them would be modified to accept those records from Butler (which fetches them from Registry).
Some Datastore existence-check methods would go away entirely (e.g. ``knows``), as their functionality is subsumed by Registry dataset queries, while others would change their behavior to checking for artifact existence *given* records.
``Registry.insertDatasets`` would be modified to accept datastore records for storage, and ``Registry.findDataset`` would be modified to return dataset records as well as a ``DatasetRef``.

.. note::
   All ``DatasetRef`` objects in this technote are assumed to be resolved; unresolved ``DatasetRef`` objects are already slated to go away per :jira:`RFC-901`.

This proposal formalizes what we are already doing during no-database batch execution, while taking advantage of new developments - UUIDs and ``daf_relation`` - to simplify the Registry/Datastore boundary.
It would involve considerable code changes, but more removals than additions, and the vast majority of thee would be behind the scenes or of minimal impact to users (e.g. ``Butler.datastore`` and ``Registry.insertDatasets`` are not formally private, but they should be, and are already widely recognized as for internal use only).

Implementation of important butler operations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In order for this proposal to be considered viable, we need to look carefully at how we would implement joint Registry/Datastore operations that we want to maintain consistency.
Note that later sections in this technical note will expand upon these and ultimately take the form of prototype Python code, as further changes are added to the proposal.

``Butler.put``
""""""""""""""

1. Obtain a valid expanded data ID from the Registry.
   In the vast majority of cases (i.e. QuantumGraph execution) this done well in advance of the actual ``put`` call.

2. Construct a ``DatasetRef`` by generating an appropriate UUID and using an existing or soon-to-exist RUN name.
   This will also typically occur well before the rest of the ``put``, as part of QuantumGraph generation.

2. Perform the ``Datastore.put`` operation, writing the file artifacts associated with the dataset and returning records to the Butler.
   Datastore can be expected to make this operation atomic, either because it naturally for its storage backing or via writing a temporary and moving it.
   We do have to accept the possibility of failures leaving partially-written temporary files around.

3. If the butler has a Registry, either held directly (as in a "full butler" today) or as a client of a butler server, call ``Registry.insertDatasets`` with both the ``DatasetRef`` and the records returned by the Datastore.
   Database transactions can be used to ensure that all tables in the Registry (including those for datastore records) are updated consistently or left unchanged.
   If this operation fails, or the butler does not have a Registry (e.g. ``QuantumBackedButler``), the dataset is left in a valid state: it is in the Datastore, but not the Registry.

This has two major advantages over our current ``put`` implementation:

- there is no database transaction over the Datastore write, keeping transactions small and reducing contention for database connections;

- for a client/server butler, there is no alternation between object-store and http operations, just one and then the other, reducing latency (assuming the data ID has indeed been obtained in advance), and increasing the possiblity that the client Datastore can just be a regular ``FileDatastore``.
  Any database transactions needed can also happen entirely in a single server operation.

``Butler.import`` and ``Butler.transfer_from``
""""""""""""""""""""""""""""""""""""""""""""""

These operations would behave like vectorized versions of ``Butler.put``, with all Datastore writes (if nontrivial transfers are required) occurring before a single Registry or butler server operation that (within a transaction) adds datasets and the associated datastore records to the database.

For ``Butler.import``, however, we also need to add dimension data, and register collections and dataset types, and not all of these can be performed in transactions.
When these are present, I propose we add them first, prior to the Datastore operations, and do not attempt to make them atomic.
These are already idempotent operations, which already allows users to retry a failed import without concern that the previous one will get in the way, and that's what's most important here.

These operations have a greater chance than a single ``put`` of leaving us with Datastore-only files due to failures, since either a late Datastore copy or link failure or a Registry failure will leave all previous Datastore copy or link successes in place.

``Butler.get``
""""""""""""""

1. If given a data ID, dataset type name, and collection search path instead of a ``DatasetRef``, obtain both the ``DatasetRef`` and all related datastore records from the database in a single Registry or butler server call.
   If given a ``DatasetRef``, use this to obtain the datastore records. again via a single Registry or a butler server call.
   ``QuantumBackedButler`` will look up datastore records directly in the quantum.

2. Call ``Datastore.get`` with both the resolved ``DatasetRef`` and the bundle of records, returning the result to the caller.

Because this is a write operation consistency in the presence of failures is not a concern, but this still has a major advantage over the current approach for client-server in particular, as it bundles all http server access into a single call, followed by a direct object-store call, reducing latency and again allowing the Datastore to be a regular ``FileDatastore``.

``Butler.unstore``
""""""""""""""""""

This is a proposed new interface for removing multiple datasets from the Datastore without removing them from the Registry - one part of a replacement for ``Butler.pruneDatasets``, and part of a reimplementation for ``Butler.removeRuns``.

1. Pass the inputs to Registry and/or butler server to obtain ``DatasetRefs`` and datastore records, instructing it to delete those records at the same time.
   ``QuantumBackedButler`` may not need to implement this operation at all, but if it does (e.g. for clobbering), it already has everything it needs in the quantum.
   Deletion in the Registry can be made consistent via transactions, and in the client/server these can be started and committed entirely in the server.

2. Pass the records to the Datastore and tell it to delete those artifacts.
   Failures at this stage would not restore the Registry records for already-deleted datasets, leaving them in our undesirable-but-tolerable Datastore-only state.
   As usual, Datastore would ignore artifacts outside of its root instead of deleting them.

Once again, we've eliminated any alternation between database/server calls and Datastore operations, reducing latency.
We've also avoided any database transactions over datastore operations.


``Butler.purge``
""""""""""""""""

This is another new proposed interface, this time for fully removing multiple datasets from both Registry and Datastore.
It's the rest of the replacement for ``Butler.pruneDatasets`` and reimplementation for ``Butler.removeRuns``.

The approach here is essentially identical to ``Butler.unstore``, but in the first step we would instruct the Registry to remove all references to the datasets, not just the datastore records.

.. _adding_journal_files:

Adding journal files
--------------------

The main flaw in the proposal above is that it can leave artifacts in the Datastore root that are untracked and hard to find, due to both I/O failures and abandoned batch runs.
This is not a new flaw - it already a problem that we are very much subject to.
These orphaned artifacts are a problem for two reasons: they waste space, and they block new Datastore writes to their locations.

.. note::
   We could have Datastore always clobber whenever it writes, and this may be worth doing anyway as a defensive measure for storage backends that allow it to be efficient.
   I could imagine it introducing a lot of latency to writes (that would in most cases be unnecessary) for some storage backends, if it involves explicit existence checks before handing off work to the ``Formatter``.

To mitigate this, we propose using *journal files* - special files written to configured locations at the start of a Datastore write operation and deleted only when the operation completes successfully.
These files would contain sufficient information to find all artifacts that might be present in the Datastore without any associated Registry content, allowing us to much more efficiently clean up after any failures.
Interpreting the content in those files must not require any Registry queries, which for ``FileDatastore`` usually means the URI must be included, though predicting a URI from information that is stored is also permitted.
Journal files may (and often will) list datasets that do not exist anywhere (e.g. were deleted successfully, or were never written), and will need to be compared to actual filesystem or object store artifact existence to be used.

All journal files should start with a timestamp and include random characters in their filenames (only the directories that might contain these files are configured and static) to avoid clashes.
Their contents and locations might take a few different forms, which will be discussed when we revisit the implementation of major butler operations below.

In the Python interface, creation and deletion of journal files would live naturally as context-manager methods on Datastore, replacing the failure-intolerant Datastore transaction system we have at present.
This would allow non-file Datastore methods to implement their own replacements.
A SQL-backed Datastore that transforms in-memory datasets fully into Datastore records would not need to use journal files at all, and a purely ephemeral in-memory Datastore could use in-memory objects to store journal content instead of files.

One unique and particularly important type of journal file is one that signals an ongoing QuantumGraph execution that has not yet been transferred back.
This could be a pointer to the QuantumGraph file or even the QuantumGraph file itself, since a QuantumGraph already carries all the information needed to find all datasets that may have been written and not transferred back as part of its execution.
This will be discussed in greater detail in a later section; for now the important criteria is that at the start of any QuantumGraph execution with ``QuantumBackedButler`` (I'm assuming Execution Butler will not exist soon) we will create a journal file that is the QuantumGraph, points to the QuantumGraph, or contains a list of all datasets the QuantumGraph's execution might produce.
When the transfer job for that execution completes successfully, that journal file is removed.

Changing the journal file format should be considered a data repository migration, and all migrations should require that the data repository have no active journal files.

Implementation of important butler operations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``Butler.put``
""""""""""""""

As before, but a journal file will be written (sometime) before the ``Datastore.put`` begins and deleted after the Registry operation succeeds.

``QuantumBackedButler`` will not write or delete journal files; it will rely entirely on a higher-level one for the full QuantumGraph.

For a client/server Butler the journal creation and deletion could happen in the client or in the server, but the former continues to permit a regular ``FileDatastore`` to be used.

``Butler.import`` and ``Butler.transfer_from``
""""""""""""""""""""""""""""""""""""""""""""""

As with ``put``, we would write a journal file before the Datastore operations begin and delete it after Registry dataset writes succeed.
Imports of dimension data, collections, and dataset types would happen before the journal file is written.
``transfer_from`` (at least) would need to turn off its own journaling in favor of a higher-level one representing a QuantumGraph execution, when it is used in the transfer job for that execution.
This suggests that we might want to make the public interfaces for transfer-from-graph and transfer-from-other-butler more distinct, and possibly make the former private.

``Butler.get``
""""""""""""""

No journaling is needed, as this is a read-only operation.

``Butler.unstore`` and ``Butler.purge``
"""""""""""""""""""""""""""""""""""""""

The journal file should be written before ``Registry`` transaction is committed and deleted only after all Datastore deletions succeed.
This is slightly problematic for client/server, because the journal file will need to be populated with information we get from the Registry database; this means the client cannot be responsible for creating the journal file unless we make fetching the datastore records and deleting them separate operations.
That isn't too bad - it's just a slight increase in latency and a bit more http traffic.
Another alternative would be to have the server take responsibility for creating the journal file, and then either returning responsibility for its deletion to the client or taking responsibility for both the deletion of the Datastore artifacts and the deletion of the journal file.
Which of these is preferable probably depends on whether we want these operations to block until completion and whether we have reasons to perform other Datastore operations on the server (up to this point, having the client use a vanilla ``FileDatastore`` and perform all Datastore operations still seems viable).

.. note::
   TODO: think about how all of this interacts with DECam raws.

.. note::
   TODO: think about how all of this affects disassembled composites.
   Or get rid of them!  I am pretty confident now that we'll never need them, and dropping that before we embark on other major changes to Datastore seems wise (sorry, Tim).

Public interface changes
========================

.. note::
   It's all stubs and outlines from here on.

Bundling DatasetRef with Datastore records
------------------------------------------

- If we're passing around DatasetRefs and bundles of datastore records together a lot, we should have a class for that.
- This would logically be what a QuantumGraph stores.
- This + a datastore would be what backs a DeferredDatasetHandle, if we don't want that hitting a butler server or Registry unnecessarily.
- This might replace FileLocation.

Butler methods vs. Butler.registry methods
------------------------------------------

- I've totally come around to the idea of Butler being the only provider of public interfaces, and we should move all useful Registry methods there, at the same time we define a richer set of ABCs for various aspects of Butler interfaces.
- Registry would become a nonpolymorphic concrete class used by one Butler subclass and the Butler server.
- A lot of Registry's convenience functionality should move to Butler.
- I think Registry's caching should move to Butler.
- Registry's default collections and governor data ID values should definitely move to Butler.
- Some of the convenience logic in the butler CLI scripts should also move to Butler.
- We should modernize the query interfaces when we invent their Butler versions and deprecate the Registry ones, to take advantage of new ``daf_relation`` functionality.
- I still like the idea of having a all Butler objects hold a Datastore, with one Datastore ABC adequate for all different Butlers.

QuantumDirectory
================

- If journal files point to QuantumGraphs sometimes, those QuantumGraphs should be considered part of the data repository.

- This naturally flows into having pipetask (or a replacement, so we can deprecate a lot of stuff at once instead of piecemeal) use QuantumBackedButler.


.. Make in-text citations with: :cite:`bibkey`.
.. Uncomment to use citations
.. rubric:: References

.. bibliography:: local.bib lsstbib/books.bib lsstbib/lsst.bib lsstbib/lsst-dm.bib lsstbib/refs.bib lsstbib/refs_ads.bib
   :style: lsst_aa
