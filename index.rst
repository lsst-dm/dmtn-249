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
The provenance system proposed in DMTN-205 can also impact these boundaries, especially if we want to extend its notion of QuantumGraph storage in data repositories to graphs that have not yet been fully executed; one possibility here is to add a third component that acts as a more graph-oriented Registry.
This technote will propose a new high-level organization of Butler interfaces and responsibilities to address these concerns.

Add content here
================

Add content here.
See the `reStructuredText Style Guide <https://developer.lsst.io/restructuredtext/style.html>`__ to learn how to create sections, links, images, tables, equations, and more.

.. Make in-text citations with: :cite:`bibkey`.
.. Uncomment to use citations
.. .. rubric:: References
..
.. .. bibliography:: local.bib lsstbib/books.bib lsstbib/lsst.bib lsstbib/lsst-dm.bib lsstbib/refs.bib lsstbib/refs_ads.bib
..    :style: lsst_aa
