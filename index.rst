:tocdepth: 1

.. sectnum::

.. Metadata such as the title, authors, and description are set in metadata.yaml

.. TODO: Delete the note below before merging new content to the main branch.

.. note::

   **This technote is a work-in-progress.**

Introduction
============

The current boundaries between the Butler and its Registry and Datastore components are under strain in a number of different ways.
Failure recovery during deletion operations has long been in bad shape, and much of the current "trash"-based system is currently just unused complexity.
Butler client/server will require new approaches to atomic operations and managing operation latency (including caching), and :jira:`RFC-888` has recently shown that we may want to move away from the Registry component providing public APIs even outside of the client/server.
This technical note will propose a new high-level organization of Butler interfaces and responsibilities to address these concerns.

The most important changes proposed here are either wholly or largely invisible to users, but some major public interface changes are included as well (see :ref:`public-interface-changes`).
Some of these are sufficiently disruptive that we imagine an extended transition period for them, such as adding new versions of interfaces well before the old ones are deprecated.
Other major changes here may never happen as proposed, as our timeline for enacting them is sufficiently long that circumstances will have inevitably have changed at least somewhat by the time we get to them.
In a few cases, we may decide that deprecating an old interface is never worthwhile even if it has been superseded.
Regardless, any backwards-incompatible API changes proposed here will be RFC'd directly when we are ready to make them.
This technote is not itself such a proposal; it should serve at most as reference/background material for future RFCs.
This is especially true of the Python prototyping stubs present in the ``git`` repository for this technote (again discussed more fully in :ref:`public-interface-changes`).

The upcoming client/server (also referred to as the "remote" or "http" butler or registry) work is the impetus for most of these changes, though many of the issues we are trying to solve are long-standing.
It is certainly true that implementing a client/server butler naively without first addressing at least our existing consistency issues (see :ref:`consistency_across_registry_and_datastore`) would put it on a very uncertain foundation.
By considering the client/server architecture in the design work to fix those issues - a huge contrast from when the current Registry and Datastore boundaries were defined - we also hope to make the development of the client/server butler much faster (after the proposed refactoring paves the way).

This technote is very much organized sequentially - each section builds on the previous one.

- In :ref:`consistency_across_registry_and_datastore`, we describe our current consistency problem and present an incomplete proposal to fix it, by clearly enumerating states that datasets are allowed to be in and reworking our internal interfaces accordingly.

- In :ref:`adding_journal_files`, we extend the proposal to deal with its most important flaw (which is actually preexisting, in a slightly different form): datasets that are present only in the Datastore.

- In :ref:`including-signed-urls-for-access-control` we amend the proposal to also handle access control problems in the client/server butler.

- In :ref:`public-interface-changes` we discuss the public interface changes required, enabled, or otherwise related to this the proposal.

- :ref:`implications_for_quantumgraph_generation_and_execution` is a stub that will be expanded in a future ticket.

.. _consistency_across_registry_and_datastore:

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

2. A dataset present in Datastore alone must have a data ID that is valid in the Registry for that data repository (i.e. it uses valid dimension values) and it must not have any *Datastore records* in the Registry database.

   .. note::
      Datastore records are tabular data whose schema is largely set by a particular Datastore, with the only requirement being that the dataset UUID be a part of the primary key (in `FileDatastore`, the component is also part of the primary key, to support disassembly).
      For `FileDatastore`, the datastore records hold the URIs for all files and the fully-qualified name of the formatter that should be used to read the dataset, along with additional metadata.
      A datastore can have more than one record table.
      Multiple datastores do not share a single record table, though this may be a `FileDatastore` limitation, not a general one, and we could probably relax this rule if a need arose.

   This state is expected to be transitory, either intentionally (e.g. during batch execution, before datasets are transferred back), or as a result of failures we cannot rigorously prevent.
   Datasets in this state as a result of failures or abandoned batch runs are considered undesirable but tolerable, and an approach to minimizing them will be introduced later in :ref:`adding_journal_files`.

2. When a dataset is present in both Registry and Datastore, the Registry is fully responsible for storage of Datastore records.
   Transferring records may be mediated by Butler or via some other direct Registry-Datastore interface (see :ref:`including-signed-urls-for-access-control`).
   Datasets in this state must always have Datastore records present in the registry, even if the Datastore otherwise has no need for records; this allows a database query to reliably return only datasets that actually exist in a Datastore via a join against the record tables.

3. A dataset present in Registry alone must have no Datastore records.
   This is expected to be a long-term state for datasets that were temporary intermediates during processing that nevertheless need to be present in the Registry for provenance recording.

This would allow us to completely remove the ``DatastoreRegistryBridge`` interface and the ``dataset_location`` and ``dataset_location_trash`` tables it manages.
Instead, we would add a new method to get record schema information from a Datastore instance (which Butler would pass to Registry when repositories are created), which would always be required to include a dataset UUID column.
We could use that information with the new ``daf_relation`` classes to easily integrate them with the query system, allowing user queries to not just test for Datastore existence, but query on and report Datastore specific-fields like file size.
We'd also of course provide a way for users to inspect which such fields are available, since Datastore record fields can change from implementation to implementation.

Datastore methods that add new datasets to the repository could be modified to return a collection of records describing those datasets, again for Butler to pass to Registry.

.. note::
   Later, in :ref:`including-signed-urls-for-access-control`, we will actually propose that Datastore records should be created by a Registry call to a helper object passed to it by Butler, which obtains that helper from Datastore, but for the purposes of the discussion up to that point, this distinction is unimportant.

Datastore methods that read datasets or interpret the records describing them would be modified to accept those records from Butler (which fetches them from Registry).
Some Datastore existence-check methods would go away entirely (e.g. ``knows``), as their functionality is subsumed by Registry dataset queries, while others would change their behavior to checking for artifact existence *given* records.
``Registry.insertDatasets`` would be modified to accept datastore records for storage, and ``Registry.findDataset`` would be modified to return dataset records as well as a ``DatasetRef``.

.. note::
   All ``DatasetRef`` objects in this technote are assumed to be resolved; unresolved ``DatasetRef`` objects are already slated to go away per :jira:`RFC-888`.

This proposal formalizes what we are already doing during no-database batch execution, while taking advantage of new developments - UUIDs and ``daf_relation`` - to simplify the Registry/Datastore boundary.
It would involve considerable code changes, but more removals than additions, and the vast majority of these would be behind the scenes or of minimal impact to users (e.g. ``Butler.datastore`` and ``Registry.insertDatasets`` are not formally private, but they should be, and are already widely recognized as for internal use only).

Implementation of important butler operations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In order for this proposal to be considered viable, we need to look carefully at how we would implement joint Registry/Datastore operations that we want to maintain consistency.
Note that later sections in this technical note will expand upon these and ultimately take the form of prototype Python code, as further changes are added to the proposal.

``Butler.put``
""""""""""""""

1. Obtain a valid expanded data ID from the Registry.
   In the vast majority of cases (i.e. QuantumGraph execution) this is done well in advance of the actual ``put`` call.

2. Construct a ``DatasetRef`` by generating an appropriate UUID and using an existing or soon-to-exist RUN name.
   This will also typically occur well before the rest of the ``put``, as part of QuantumGraph generation.

3. Perform the ``Datastore.put`` operation, writing the file artifacts associated with the dataset and returning records to the Butler.
   Datastore can be expected to make this operation atomic, either because it is naturally atomic for its storage backing or via writing a temporary and moving it.
   We do have to accept the possibility of failures leaving partially-written temporary files around.

4. If the butler has a Registry, either held directly (as in a "full butler" today) or as a client of a butler server, call ``Registry.insertDatasets`` with both the ``DatasetRef`` and the records returned by the Datastore.
   Database transactions can be used to ensure that all tables in the Registry (including those for datastore records) are updated consistently or left unchanged.
   If this operation fails, or the butler does not have a Registry (e.g. ``QuantumBackedButler``), the dataset is left in a valid state: it is in the Datastore, but not the Registry.
   This must happen before the database changes are committed.

This has two major advantages over our current ``put`` implementation:

- there is no database transaction over the Datastore write, keeping transactions small and reducing contention for database connections;

- for a client/server butler, there is little alternation between object-store and http operations, reducing latency (assuming the data ID has indeed been obtained in advance) and increasing the possibility that the client Datastore can just be a regular ``FileDatastore``.
  Any database transactions needed can also happen entirely in a single server operation.

.. note::
   Allowing Datastore to clobber whenever it writes is not safe under the new proposal, because Datastore will now see racing conflicting writes before Registry.
   A POSIX-backed Datastore could handle these races by writing to a temporary random location and hard-linking into the final location, since hard-links are atomic and do not clobber by default.
   Object-store Datastores do not have this option, and do not in general provide the kind of one-writer-succeeds behavior we'd need.
   This is largely mitigated by the fact that QuantumGraph execution provides high-level management of possible races (as long as there is only one QuantumGraph for any RUN collection).
   It could be further mitigated by including the UUID in the URI template, making it far less likely that the Datastore writes will clash; when the Registry transactions do clash, at most one will be committed (and associated with a valid Datastore write), while the others will roll back, leaving some Datastore-only datasets (which, again, are permitted albeit undesirable).
   This leaves competing writes that also use the same UUID as a problem, but this is sufficiently difficult to do accidentally that I think we can just guard against it via documentation; the biggest concern is probably logic bugs in QuantumGraph execution (especially involving retries), not user code.

``Butler.import`` and ``Butler.transfer_from``
""""""""""""""""""""""""""""""""""""""""""""""

These operations would behave like vectorized versions of ``Butler.put``, with all Datastore writes (if nontrivial transfers are required) occurring before a single Registry or butler server operation that (within a transaction) adds datasets and the associated datastore records to the database.

For ``Butler.import``, however, we also need to add dimension data, and register collections and dataset types, and not all of these can be performed in transactions.
These are already idempotent operations, which already allows users to retry a failed import without concern that the previous one will get in the way, and that's what's most important here.

These operations have a greater chance than a single ``put`` of leaving us with Datastore-only files due to failures, since either a late Datastore copy or link failure or a Registry failure will leave all previous Datastore copy or link successes in place.

``Butler.get``
""""""""""""""

1. If given a data ID, dataset type name, and collection search path instead of a ``DatasetRef``, obtain both the ``DatasetRef`` and all related datastore records from the database in a single Registry or butler server call.
   If given a ``DatasetRef``, use this to obtain the datastore records. again via a single Registry or a butler server call.
   ``QuantumBackedButler`` will look up datastore records directly in the quantum.

2. Call ``Datastore.get`` with both the resolved ``DatasetRef`` and the bundle of records, returning the result to the caller.

Because this is a read-only operation, consistency in the presence of failures is not a concern, but this still has a major advantage over the current approach for client-server in particular, as it bundles all http server access into a single call, followed by a direct object-store call, reducing latency and again allowing the Datastore to be a regular ``FileDatastore``.

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

When fully removing multiple datasets from both Registry and Datastore (interfaces for this will be described later), we would follow the same approach, but in the first step we would instruct the Registry to remove all references to the datasets, not just the datastore records.

.. _adding_journal_files:

Adding journal files
--------------------

The main flaw in the proposal above is that it can leave artifacts in the Datastore root that are untracked and hard to find, due to both I/O failures and abandoned batch runs.
This is not a new flaw - it already a problem that we are very much subject to.
These orphaned artifacts are a problem for two reasons: they waste space, and they block new Datastore writes to their locations.

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
This will be discussed in greater detail in a later section; for now the important criteria is that at the start of any QuantumGraph execution with ``QuantumBackedButler`` (I'm assuming Execution Butler will not exist soon) we will create a journal file that either is the QuantumGraph, points to the QuantumGraph, or contains a list of all datasets the QuantumGraph's execution might produce.
When the transfer job for that execution completes successfully, that journal file is removed.

Changing the journal file format should be considered a data repository migration, and all migrations should require that the data repository have no active journal files unless they are able to migrate those files as well.

Journal files need to be readable and writeable in the same contexts that the Datastore itself is.
This rules out storing them in client-side locations, but having Datastore client code write them to and read them from a remote object store is viable, and storing them inside the Datastore root itself is probably the simplest approach.
If a Datastore server is present, it could take responsibility for writing them, and they could even be store in a database (though if they are stored in the Registry database, we need to be careful to resist the temptation to include them in Registry transactions when they should not be).

Implementation of important butler operations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``Butler.put``
""""""""""""""

As before, but a journal file will be written (sometime) before the ``Datastore.put`` begins and deleted after the Registry operation succeeds.

``QuantumBackedButler`` will not write or delete journal files; it will rely entirely on a higher-level one for the full QuantumGraph.

``Butler.import`` and ``Butler.transfer_from``
""""""""""""""""""""""""""""""""""""""""""""""

As with ``put``, we would write a journal file before the Datastore operations begin and delete it after Registry writes succeed.

``Butler.get``
""""""""""""""

No journaling is needed, as this is a read-only operation.

``Butler.unstore`` and other removals
"""""""""""""""""""""""""""""""""""""

The journal file should be written before the ``Registry`` transaction is committed and deleted only after all Datastore deletions succeed.
This is slightly problematic for client/server, because the journal file will need to be populated with information we get from the Registry database; this means the client cannot be responsible for creating the journal file unless we make fetching the datastore records and deleting them separate operations.
That isn't too bad - it's just a slight increase in latency and a bit more http traffic.

.. note::
   This proposal should not affect our ability support disassembled composites, though we may be able to make further simplifications if we drop that support.
   It may have implications for our ability to support multiple datasets in a single file, at least in terms of safeguarding against premature deletions
   of those files.
   An easy way to mitigate that would be to limit that support to "unmanaged" datasets that are ingested with absolute URIs, since those are never deleted by butler code, and this satisfies Rubin's only current use case (ingesting DECam raws) for this functionality.

.. _including-signed-urls-for-access-control:

Including signed URLs for access control
----------------------------------------

Our access control model for the official Rubin Observatory data repositories (see :cite:`DMTN-169`) is based on information stored in the Registry - collection names, whenever possible, and new naming conventions or new columns in contexts (e.g. dimension records or dataset type names) that are not associated with collections.
Access to the associated files managed by a Datastore is mediated by signed URLs; once the server side of the remote Butler has determined that an API call is permitted (based on that the Registry-side information), it can generate one or more signed URLs to pass to Datastore that provide direct access to the controlled files.
A seemingly natural place to include URL-signing in this proposal is inside the Datastore implementation, since only the Datastore knows where any URLs might exist in the opaque-to-Registry records it uses, and only the Datastore ever uses and kind of URLs.
This approach has two major drawbacks, however:

- It requires the Datastore to have a server component; in every other respect we can abstract the differences between a SQL-backed full Butler and a client/server full Butler via a different Registry implementation (see :ref:`public-interface-changes`).

- Because the information used to determine whether a URL *should* be signed lives in the Registry, a Datastore server cannot easily perform this job.
  It cannot trust information provided to it via the `DatasetRef` and opaque table records passed to it from Butler in terms of access control, since those come from a public http interface; instead it'd have to use a Registry of its own to re-obtain Datastore records and collection information for each UUID it is passed in order to determine whether to sign URLs embedded in the records.
  This is a potential efficiency concern, and while we could probably use caching to mitigate that, caching often increases complexity is subtle ways.

Our alternative proposal here is to instead make Registry responsible for signing URLs, using a small piece of server-side Datastore-provided logic to interpret the opaque records just enough for it to perform this job.
Registry already needs to be told about the schemas of the of the opaque tables enough to create SQL tables, insert rows into them, and query for those rows, and that information can only come from Datastore, so it's a small leap from that to also having Datastore tell Registry (in these schema-definition objects) where to find URLs that must be signed.

This is most straightforward for read and deletion operations, for which unsigned URLs are already stored in opaque tables in the Registry database, and we can transform them into signed URLs before we send them back to Butler client for use.

For `Butler.put`, it would be most efficient to have the Registry generate signed URLs at the same time it expands data IDs for (potential) use in URI templates, since both of these need to be done on the server.
We also need to generate UUIDs for new datasets, and have thus far been vague about which component has that responsibility.
Doing all of this in the Registry makes sense, which amounts to essentially making it *indirectly* responsible not just for storing Datastore records, but for creating at least the initial versions of them as well (including URI templates), by delegating that work to the same schema-definition objects it already receives from Datastore.
This means a substantial fraction of a Datastore's logic will actually be executed on the server, as part of the the Registry, and that these schema-definition objects have hence really evolved into something more: they are the new Datastore-Registry "bridge" interface.

Access control for journal files
--------------------------------

Journal files for Datastores with access controls will also need to take those controls into account.

First, journal files *may* contain signed URLs, but they must include the UUID and any additional information needed to fully identify the artifact (e.g. the component).
Unsigned URIs may be present but may not be directly usable.

It is at least desirable for journal files written by one user to only reference datasets that are writeable by that user in the manner represented by that journal file, but this cannot be guaranteed if a client Datastore writes the journal file directly, even if it does so via a signed URLs.
It may be acceptable to proceed with client-written remote journal files without this guarantee, because presence in a journal file is not on its own sufficient to cause a modification to any dataset.

Having client code write journal files to a remote location would also require a signed URL for the journal file location.
Given that dataset controls are mediated by collections, it would make sense for the client to obtain from the server one signed journal file URL for each RUN collection it wants to modify, and for the true locations of those files to be very clearly mapped to those RUN collections.
This could be done at the same time the client requests records (and signed URLs) for the datasets themselves.
Administrative code that cleans up journal files could then ignore (or complain about) any entries that correspond to datasets that are not in the corresponding RUN collection.

This still seems to be the simplest approach, in that it keeps all I/O in the client, using signed URIs obtained from the server to perform remote operations.
The alternative is to have a server API for writing and removing journal files.
This might give us more flexibility in where the journal files are actually stored, as well as the ability to directly vet what goes into them.
But it would another server API to maintain and another server call to make, and it might be a subtly difficult one to write, given that it is a core part of our error-handling.


.. _public-interface-changes:

Public interface changes
========================

This package's source repository includes a ``prototyping`` directory full of Python files (mostly just interface stubs) that attempt to work out the proposal in detail.
This section further motivate and describe that detailed proposal at a high level and occasionally include snippets from it, but it should be inspected directly to see the complete picture.
The ``README.rst`` file in that directory includes important caveats that should be read first.
The most important is that this is in many respects a "maximal" proposal or "vision document" - it represent an attempt to envision how future Butler, Registry, and Datastore interfaces would ideally look (including a full switch to ``snake_case`` naming), with the expectation that many of these changes will never come to pass.

The changes summarized here are those that we believe are most important in a broad sense, though the details may change and in some cases we believe an unusually extended transition period (in which both old and new APIs are present) would make sense.

Bundling Datastore records with DatasetRef
------------------------------------------

The proposed changes to how datastore records are handled means that we will be passing `DatasetRef` instances and their associated datastore records to datastore together, essentially all of the time.
But obtaining a `DatasetRef` from the the Registry is not just something done by Butler code; it's also something users do directly via query methods.

This suggests that we should attach those datastore records to the `DatasetRef` objects themselves, both to simplify the signatures of methods that accept or return them together, and to allow the queries used to obtain a `DatasetRef` to provide everything needed to actually retrieve the associated dataset.

This constitutes a new definition of an "expanded" `DatasetRef`: one that holds not just an expanded data ID, but a bundle of datastore records as well.

Combined with the conclusion of :ref:`including-signed-urls-for-access-control`, this means we'd be returning signed URLs in the `DatasetRef` objects returned by query methods.
This is mostly a good thing - it makes those refs usable for reading datasets directly and it completely avoids redundant registry lookups in usual workflows.
But it does increase the duration we'd want a typical signed URI to be valid for - instead of the time it takes to do a single operation, it'd be more like the time it takes the results of a query in one cell of a notebook to be used in another cell.
While that's arbitrarily long in general, I don't think it's unreasonable to either tell users that refs will get stale after a while, or just add timestamps to signed URIs so we can spot them and refresh them as necessary when ``get`` is called.

Butler methods vs. Butler.registry methods
------------------------------------------

One outcome of :jira:`RFC-888` was that users disliked having to remember which aspects of the butler public interface were on the `Butler` class vs. the `Registry` it holds.
It's also confusing that `Butler.registry` and `Butler.datastore` both appear to be public attributes, but only the former really is (and some of its methods are not really intended for external use, either).
Moving all of the public `Registry` interface to `Butler` and making `Butler.registry` (and `Butler.datastore`) private would be a major change, but it's the kind of change that would also help us with other changes:

- It lets us repurpose `Registry` as an internal polymorphic interface focused on abstracting over the differences between a direct SQL backend and an http backend, while leaving common user-focused client code to `Butler`.

- It gives us a clear boundary and deprecation point for other needed (or at least desirable) API changes, in that new versions of methods can differ from their current ones without having to work out a deprecation path that allows new and old behavior to be coexist in the same signature.

In addition to moving convenience code out of `Registry` and into `Butler`, we'll also need to move our caching (of collections, dataset types, and certain dimension records) to the client, and it'll certainly be better to put that in one client class (i.e. `Butler`) that replicate it across both `Registry` client implementations.

At some point, we may opt to continue backwards compatibility support for `Butler.registry` methods by making `Butler.registry` return a lightweight proxy that forwards back to `Butler` instead of a real `Registry` instance.

Batch operations and unifying bulk transfers
--------------------------------------------

The current Butler provides a `transaction` method that returns a context manager that attempts to guarantee consistency for all operations performed in that context.
This only works rigorously for Registry operations at present, as the Datastore transaction system we have at present is not fault tolerant.
Much of the rest of this proposal is motivated by trying to address this, but our current transaction interface is not really viable even for Registry-only operations when an http client/server implementation is required.

Instead, the prototype includes a `RawBatch` class that represents a low-level serializable batch of multiple `Registry` operations to be performed together within one transaction, and a `Butler.batched` method and `BatchHelper` class to provide a high-level interface for constructing them.
Unlike operations performed inside the current `Butler.transaction`, applying operations to `BatchHelper` does nothing until its context manager closes.

`RawBatch` also turns out to be a very useful way to describe the transfer of content from one data repository to another, whether that's directly via `Butler.transfer_from` or with an export file/directory in between.
The prototype includes another helper context manager (`ButlerExtractor`, which inherits from `LimitedButlerExtractor`) for constructing these batches, and a sketch for how they might be used to define a new more efficient (and less memory-constrained) export file format.
The prototype's `Butler.export` and `Butler.transfer_from` both use the `ButlerExtractor`, unifying those interface.

Opportunistic API changes
-------------------------

Moving methods from `Registry` to `Butler` will break existing code - eventually, once the removal of the `Registry` interface actually occurs.
In the meantime, adding new methods to `Butler` gives us an opportunity to address existing issues and take advantage of new possibilities without introducing breakage.
In addition to solving this problems, this can help ease migration to the new interfaces by giving users reasons to switch even before the `Registry` methods are deprecated.

The prototype includes a few examples of this kind of opportunistic API change, including:

- Our myriad methods for removing datasets and collections have been replaced by the extremely simple `Butler.unstore` method, the more powerful `Butler.removal` method, and the `RemovalHelper` class the latter returns.
  This provides more direct control over and visibility into the relationships that would be broken by removals (CHAINED collection links, dataset associations with TAGGED and CALIBRATION collections).

- The new relation-based query system can do already do some things our current query interfaces have no ways to express, and with a bit more work it could do more still.
  At the same time, the current `Registry.queryDataIds` and `Registry.queryDimensionRecords` methods take ``datasets`` and ``collections`` arguments whose behavior has consistently been confusing to users (who should usually use `queryDatasets` instead).
  The prototype proposes both a new power-user `Butler.query` method and more-or-less like-for-like replacements for our current methods, but the replacements for `queryDataIds` and `queryDimensionRecords` drop those arguments, since the subtle functionality they provided is now available via `Butler.query`.

- `Registry.setCollectionChain` has been replaced by `Butler.edit_collection_chain`, which ports convenience functionality from our command-line scripts to the Python interface.

.. _implications_for_quantumgraph_generation_and_execution:

Implications for QuantumGraph generation and execution
======================================================

.. note::
   This section is a stub.  It may be expanded on a future ticket.

- Attaching Datastore records to DatasetRef objects makes it more natural to for QuantumGraph to hold Datastore records, which it currently does for overall-inputs only, in separate per-quantum containers.
  Including predicted Datastore records for intermediate and output datasets may also help with storage class conversions, by allowing us to also drop special mapping of dataset type definitions recently added on :jira:`DM-37995` - the key question is whether `Datastore` needs to know the Registry storage class for for a particular dataset type if it also has the `Datastore` records.
  If it does not, then this may also open up a path to sidestepping storage class migration problems - the Registry storage class for a dataset type could become merely the default for when a storage class is not provided, as we'd always use Datastore records to identify what is on disk on a dataset-by-dataset basis.

- If journal files point to QuantumGraphs sometimes, those QuantumGraphs should be considered part of the data repository.
  This will require additional design work.

- This naturally flows into having pipetask (or a replacement, so we can deprecate a lot of stuff at once instead of piecemeal) use QuantumBackedButler.

.. Make in-text citations with: :cite:`bibkey`.
.. Uncomment to use citations
.. rubric:: References

.. bibliography:: local.bib lsstbib/books.bib lsstbib/lsst.bib lsstbib/lsst-dm.bib lsstbib/refs.bib lsstbib/refs_ads.bib
   :style: lsst_aa
