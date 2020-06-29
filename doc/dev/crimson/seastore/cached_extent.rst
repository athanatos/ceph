============
CachedExtent
============

CachedExtent (crimson/os/seastore/cached_extent.h) is the common base class and interface
for all in-memory extents.  It encompasses several broad aspects of cache management:

- dirty/clean state tracking
- interpretation of deltas during replay
- extent ref counting
- extent relocation


States
======

A particular CachedExtent may be in one of several states:

- INITIAL_WRITE_PENDING: In Transaction::write_set and fresh_block_list 
- MUTATION_PENDING: In Transaction::write_set and mutated_block_list
- CLEAN: In Cache::extent_index, Transaction::read_set during transaction,
  contents match original block contents, version == 0
- DIRTY: Same as CLEAN, but contents only match original block contents
  after applying journaled deltas, version > 0
- INVALID: Part of no ExtentIndex set, not a valid extent

Because a transaction needs to be able harmlessly fail, extents aren't
directly modified.  Instead, prior to modification, users obtain a new,
mutable instance of the extent via TransactionManager::get_mutable_extent
(which calls ultimately into CachedExtent::duplicate_for_write).  Thus,
the actual transition sequence for an extent being mutated is:


                 MUTATION_PENDING   -commit-> DIRTY
                        ^
                        |
               duplicate_for_write
                        |
CLEAN|DIRTY ->          _           -commit-> INVALID

The original extent beigns as either CLEAN|DIRTY, a new instance is created
in state MUTATION_PENDING, and the two end up as DIRTY and INVALID when the
transaction commits.

Pinning
=======

Because we always write sequentially to a segment and release a whole
segment at a time, we must be able to cheaply and efficiently relocate
an extent.  In particular, rewriting a DIRTY extent to trim the journal
requires an efficient rewrite operation.  In order to relocate an extent,
we need any extents which hold the physical address.  By design, there
is a single extent with the physical extent of any particular extent, so
we'll choose to ensure that if extent E is in cache, the unique extent
with a physical reference to E is also in cache.  There are three
cases here to note:

1. LogicalCachedExtent: always referenced by the LBA tree leaf node,
   references nothing
2. LBANode: always referenced by parent LBANode (or root)
   references LBANodes if internal, LogicalCacehdExtent otherwise
3. RootBlock: references LBANode, referenced by nothing (has no physical
   address)

