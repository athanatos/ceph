========
Paxos PG
========

This is a proposal for a pool type which can satisfy two aditional
requirements over the semantics of the current PrimarLogPG based
pools.

1. Quroum writes should be possible without a change to the acting set.
2. It should be possible to ensure that writes to objects to which the
	 client can rule out concurrent writes from other clients never
	 block.  An osd should always be able to perform or reject such
	 writes without consulting any other nodes.

The goal is to attack the tail latency issue present in ceph during
node failure in general without sacrificing common case performance.

Requirement 2. is about addressing specifically the use case attacked
by the bookkeeper project.  The goal is to be able to perform very low
latency writes to append only single writer logs.  Notably absent is a
requirement that *reads* be fast in such cases.  It would be ok if
reads to such an object did not immediately return the newly committed
write as long as once they do they always do.  There does need to be
some form of primitive to wait until a particular write is readable
(and presumably force it to happen).  This primitive may have high
tail latency.

My hope is that we can come up with a more generally parameterized
commit protocol which has the current tradeoffs as a special case,
but is also capable trading off read latency for write latency.

Design Space
============

Bookkeeper
----------

The Bookkeeper project provides an extreme example of trading commit
latency for read recency (or latency).  A client begins writing by
opening a new *ledger* for writes by committing the existence of this
ledger as well as the *ensemble* of bookies to which it will be
written to a ZooKeeper instance.  Note that this ledger cannot have
existed prior to this, ledgers cannot be reopened for writes.  The
client can then begin writing *entries* (numbered variably sized
extents) to the ensemble written into ZooKeeper.  An entry e is
considered committed when for all entries e' <= e a subset of the
ensemble of size equal to at least *ackQuroum* (which can be
configured on a per-ledger basis) report that e' has been persisted.
In the event that the client considers a bookie failed (note that this
is a purely local decision by any writer), the client can write a new
ensemble into ZooKeeper for entries starting at the first one the
client considers uncommitted and retry to the new target set.

To make all of this work, reads and client failure are a little
complicated.  Observing that a particular bookie has entry e isn't
sufficient to prove that it's been written since there may not yet be
ackQurorum copies (or even that there ever will be since the client
could have died).  Thus, there are two ways of establishing which
entries have actually been committed.  First, when a client "closes" a
ledger, the last committed entry is written down for future reads to
use.  Second, in the case that clients are "tailing" a ledger which is
being written to, the client can periodically update a
*LastAppliedCommitted* (LAC) value on the bookies.  If a client
observes that a bookie reports an LAC of e on any bookie, all entries
e' <= e must have been committed and are safe to read.  Client failure
requires that the current ensemble be fenced (in case the client isn't
actually dead) and the tail of the ledger on the bookies to be read in
order to determine where the ledger actually ends.

The upshot of all of this is that a writer needn't do much to perform
a write.  To open a ledger (or switch the set of bookies), the writer
performs a write to ZooKeeper.  For each write, the writer simply
considers the write committed when a quorum of the write targets
responds with success.  Reads are more complicated and either must
wait for the writer to publish a new LAC value or in the worst case
must fence the writer and recover the ledger by checking all of the
bookies to which the ledger was being written (essentially a quorum
read) -- but that isn't really a problem for many use cases.

PrimaryLogPG Pools
------------------

By contrast, current ceph pools require considerably more pomp and
ceremony before a write can be performed.  Generally, a write or a
read to an osd requires:

1. The osd must have a map at least as new as the client, and the
primary for the write cannot have changed since the client's map.  In
the worst case, the OSD may have to wait for a new map from a peer or
from the mon (the client would get an updated map immediately from the
osd).
2. The primary must ensure that at least one member of every prior
acting set is no longer accepting writes and must obtain a
representation of a superset of all completed writes from at least one
member of every prior acting set.
3. The primary and all members of the acting set must complete the
write.

Between when an OSD receives a map making it no longer the primary and
when the next primary completes 2, there can be no writes or reads.
Failure of a member of the acting set blocks writes until a new map
declaring the osd dead is published by the mons and the new primary
(even if it's the same primary) goes through 1 and 2.  The big win is
that we can safely perform a read by reading from the current
primary in the normal case without having to do additional work to
ensure that the value can't become divergent.

FPaxos
------

There's a paxos variant called FPaxos
(https://arxiv.org/pdf/1608.06696.pdf) which suggests a way of
thinking of PrimaryLogPG commits as a special case.  The key
observation seems to be that paxos doesn't require all quorums to
overlap, just quorums from the same phase.  From this persective, we
can think of the PrimaryLogPG protocol as FPaxos where for each PG,
the monitors publish a numbered sequence of Q2 (the acting set) and
the primary for each Q2 performs a single phase 1 using a Q1
consisting of at least one member of every prior Q2 by looking back
through old OSDMaps establishing it as the leader for the duration of
the interval.

Ceph doesn't permit cross-object transactions.  Thus, we don't
actually need all updates to the PG to be serialized in a single
command stream.  If we permit any osd in the acting set, or even any
client, to perform a phase 1 election for a particular object, we can
tolerate failures (even intermittent ones) without a map change.
Further, we can get quorum writes by making the Q2 any size (let's
say) 2 subset of the size 3 acting set.  Of course, phase 1s now
require us to talk to at least 2 members of any previous acting set.
Of course, now writes require a second round trip for the phase one
election.

There are of course standard tricks for amortizing that election round
trip.  In the event that the mapping of write targets to clients is
many to one, we can optimistically let the client remain the leader
without having to go through an election for each operation.  In the
event that we know a-priori that a particular client will be the only
writer to a particular object, we should be able to dispense with the
election entirely.  If furthermore we don't require that write
completion imply that reads can proceed, I think we can get BookKeeper
style commits. [Note: there's a whole metadata problem here I haven't
addressed.  With a single commit stream and a single leader, there's
no need to remember who is leader for what, or per-object election
metadata.]

