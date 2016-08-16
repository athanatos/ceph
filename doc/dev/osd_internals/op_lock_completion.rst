==================
OP Lock Completion
==================

Motivation
--------
Current write operation needs acquiring and releasing the PG lock frequently. 
It causes a huge overhead in the case of heavy random write workload.
Therefore, this patch focus on minimizing the PG lock contention.

Concepts
--------
The main Concept is removing the PG lock when OSD handles completion processing 
such as committed, applied. OSD will send ack to client with simple counting 
under the OP lock and updating metadatas (delayed works) which need the PG are 
delayed. Proceesing delay works will be handled by completion worker or issuing 
write operation (already have the PG lock).


*Write Flow*
A. when a write request is issued (Primary)

    1. PG lock (acquire) -> 2. do_lazy_completion() ->  3. add repop_queue -> 
      4. Ordering lock (ReplicatedBackend & ECBackend::internal_ops_lock, acquire) 
      -> 5. insert InProgressOP (ReplicateBackend) or Op (ECBackend) -> 
      6. Ordering lock (release) -> 7. queue_transaction() -> 8. PG lock (release)

B. when a write request is issued (Not primary)

    1. PG lock (acquire) -> 2. add repop_sub_queue -> 3. insert RepModify 
      (ReplicateBackend) or Op (ECBackend) -> 4. queue_transaction() -> 6. PG lock 
      (release)

B. when commit, apply are completed

    1. complete commit or apply. -> 2. Repop(OP) lock (acquire) -> 
      3. If all commit or apply are finished, send ack to client -> 
      4. Repop(OP) lock (release) -> 
      5. delay.. (6 will happen a few second later in the batching manner) -> 
      6. queue_for_completion (see D.)

C. when osd receives MSG_OSD_REPOPREPLY or MSG_OSD_EC_WRITE_REPLY

    1. Reply Ordering lock (ReplicatedPG::seq_ops_lock, acquire) -> 
      2. Ordering lock (ReplicatedBackend & ECBackend::internal_ops_lock, acquire) -> 
      3. Get InProgressOp or Op -> 4. Ordering lock (release) -> 
      5. Repop(OP) lock (acquire) -> 6. check completion (applied) -> 
      7. check completion (committed) -> 
      8. If all commit or apply are finished, send ack to client -> 
      9. Repop(OP) lock (release) -> 10. Reply Ordering lock (release) -> 
      11. delay..(12 will happen a few second later in the batching manner) -> 
      12. queue_for_completion (see D.)

D. queue_for_completion (OSDService::comp_worker, update PG metadatas such as lcod)

    1. PG lock (acquire) -> 2. do_lazy_completion() (iterate repop_queue and 
      repop_sub_queue for lazy completion and remove repop from repop_queue) -> 
      3. PG lock (release)

*Data Structure*

A. CompletionItem
  See src/osd/PG.h
  
  The CompletioItem abstracts the RepGather and the RepSub for op lock completion.

B. CompletionWorker
  See src/osd/OSD.h

  Call do_lazy_completion after a few (configured) seconds.

C. PGBackend
  
  Shared structures (in_progress_ops, writing, tid_to_op_map) need protection. 
  Because it can be refered without the PG lock.
  
  

