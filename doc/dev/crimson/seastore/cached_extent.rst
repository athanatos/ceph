============
CachedExtent
============

CachedExtent (crimson/os/seastore/cached_extent.h) is the common base class and interface
for all in-memory extents.  It encompasses several broad aspects of cache management:

- dirty/clean state tracking
- interpretation of deltas during replay
- extent ref counting
- extent relocation


