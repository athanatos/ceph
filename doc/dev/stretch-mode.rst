============
Stretch Mode
============

The goal
========
We want to support running Ceph in 2 sites, with a tiebreaker monitor
elsewhere, while guaranteeting ongoing availability if a site breaks.

The general plan
================
We introduce a new "stretch mode" that the cluster can enter. This comes with
requirements, updates the monmap and osdmap, and changes peering.

Requirements
============
We target 2 sites, with 2 monitors in each site and a fifth tiebreaker monitor
elsewhere. In order to deal with OSD restarts/failures while still staying
available on that single site, each site has to store 2 replicas, for a total
of 4.
The monitors must run in connectivity election mode, as it was designed to
support this stretch mode.

Monitor changes
===============
Each monitor can be assigned a location, which should match CRUSH map
locations (though this is not really enforced). The monmap also stores
whether we are in stretch mode and the given "tiebreaker" monitor.

The tiebreaker is not eligible to be elected leader, and exists
to make sure both sites can go active if the other fails (or to pick
a winner if there's a netsplit).

Once stretch mode is engaged, monitors will not accept connections from
OSDs in other sites. This ensures clean behavior in netsplit scenarios,
so that we can decide sites are down as a single unit (when all their
monitors and their OSDs are not in the active cluster).

OSD changes
==============
The OSDMap records if stretch mode is enabled, the number of sites (always 2),
whether we're in degraded mode (0 if not, or
1 -- the remaining site count -- if yes), whether we're recovering (0 or 1,
not exclusive with degraded mode), and the bucket type we split across.

pg_pool_t gains a set of peering_crush_bucket_* members. These record
the required number of buckets (1 when degraded or recovering, 2 when healthy),
the target number of buckets (1 when degraded, 2 when recovering or healthy),
and a mandatory bucket member (set to the surviving site when we go
degraded, and cleared when healthy).

The OSD changes its peering algorithm somewhat. PeeringState::choose_acting()
counts the number of stretch split buckets and ensures the number meets
the minimum requirement, in addition to enforcing min_size rules.

PeeringState::calc_replicated_acting() is also modified to try and meet
these requirements when choosing extra acting members. It calculates a
bucket_min and bucket_max by dividing the pool size by the target bucket count,
and will not insert additional OSDs if they exceed the bucket_max.
It will continue trying to add new candidate OSDs into the acting set until
it meets the minimum bucket requirement and each of those buckets meets
the bucket_min number of OSDs.
