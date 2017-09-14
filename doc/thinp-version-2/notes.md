It's time for a major update to the thin provisioning target.  This is a chance
to add new features, and address deficiencies in the current version.

Features
========

Features that we should consider (some are more realistic than others).

- Performance enhancements for solid state storage.  eg, streaming writes.
  Take erasure size into consideration.

- Compression.

- Resilience in the face of damaged metadata.  Measure potential data loss
  compared to size of damage.

- Support zeroed data in the metadata to avoid storing zeroes on disk.

- Get away from the fixed block size.

  Since it's always a compromise between provisioning performance and snapshot
  efficiency.

- Performance improvement for metadata.

  Space maps are too heavy.

- Performance improvement for multicore.

- Reduce metadata size.

- Efficient use of multiple devices.

  Currently thinp is totally unaware of how the data device is built up.

Anti-features
=============

Not considering these at all:

- Dedup.

Metadata
========

Problems with the existing metadata
-----------------------------------

- Btrees are fragile
  Either use a different data structure, or add enough info that trees can be
  inferred and rebuilt.

- metadata is huge
  Start using ranges.

- space maps

  Reference counting ranges will be more tedious.  Find free now needs to find
  ranges quickly.

Ideas
-----

- What could we use instead of btrees?
  Skip lists.  Difficult to make these fit the persistent-data scheme I think
  these are better as an in core data structure (where spacial locality is less
  important).

- Drop reference counting from space maps completely.

  This would allow them to be implemented with a simpler data structure, like a
  radix tree or a trie.  It would be impossible to ascertain which blocks were
  free without a complete walk of the metadata.  This is possibly ok if the
  metadata shrinks drastically through the use of ranges.

- Space maps do not need to be 'within' the persistent-data structure system
  since we never snapshot them.


Blob abstraction
================

A storage abstraction, a bit different from a block device.  Presents a virtual
address space.

  (read dev-id begin end data)
  (write dev-id begin end data)
  (erase dev-id begin end)
  (copy src-dev-id src-begin src-end dest-dev-id dest-begin)

How do we cope with a device being split across different blobs?  We need a
data structure to hold this metadata information:

  (map dev-id begin end) -> [(blob begin end)]

Could we use bloom filters in some way? (can't see how, we'd need to cope with
erasure and false positives).

Write:

We always want to write into the highest priority blob (ie. SSD), so we need to
write to new blob, commit, then we can erase from old blobs.

Read:

Look up blobs, issue IOs and wait for all to complete.

Erase:

Look up blobs, issue erase.

Dealing with atomicity
----------------------

Blobs store their metadata in different ways, do they individually implement
transactions, or can we enforce transactionality from above?  I think the
address space has to be managed for all blobs in one space.  So each blob
presents a *physical* address space, and the core maps thin devices to physical
spaces.

Journal blob: records changes in a series, efficient for SSDs, slow start up
since we need to walk the journal to build an in core map.

Transparent blob: no smarts phys addresses are translated to the data dev with a
linear mapping.  This suggests we have to have pretty much all of current thinp
metadata in the core.

Compression blob: Adds an additional layer of remapping to provide compression.

Aging
-----

Data ages from one blob to another.  Because the journal blob is held in
temporal order it's trivial to work out what should be archived.  But the
transparent one?  Perhaps this should be another instance of the journal blob?


ALL blobs now mix metadata and data.  Core metadata needs to go somewhere
(special dev id for fast blob)?


Temp btrees
-----------

If we're journalling we can relax the way we use btrees.  There's a couple of
options:

 - Treat the btree as totally expendable, use no shadowing at all.

 - Commit period for btree can be controlled by the journal, avoiding commits
   whenever a REQ_FLUSH comes in.




