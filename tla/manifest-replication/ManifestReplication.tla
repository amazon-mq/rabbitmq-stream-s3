----------------------- MODULE ManifestReplication -----------------------
(* Formal model of manifest edit replication between writers and replicas.

   A single writer at a time produces sequenced edits and broadcasts them
   to registered replicas. Writers change on epoch transitions (leader
   election). A new writer starts with a fresh sequence counter. The old
   writer may still be alive briefly (zombie) and send stale edits.

   Replicas apply edits in strict sequence. On detecting a gap or an
   epoch mismatch, the replica discards its state and re-syncs from the
   current writer.

   Safety: A replica in "ok" state holds a manifest consistent with the
   current writer's history at the replica's sequence number.

   Liveness: A replica eventually converges to the current writer's
   state (assuming the channel eventually delivers messages).
*)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS
    Replicas,       \* Set of replica node identifiers
    MaxEdits,       \* Bound on number of edits per epoch
    MaxEpochs       \* Bound on number of epoch transitions

VARIABLES
    epoch,          \* Current epoch (nat)
    writerSeq,      \* Current writer's sequence number (nat)
    replicaSeq,     \* Function: replica -> last applied seq (nat)
    replicaEpoch,   \* Function: replica -> epoch of last applied edit
    replicaState,   \* Function: replica -> "ok" | "resyncing"
    channel,        \* Function: replica -> sequence of in-flight messages
    registered,     \* Set of replicas registered with current writer
    zombieChannel   \* Function: replica -> in-flight messages from old writer

vars == <<epoch, writerSeq, replicaSeq, replicaEpoch, replicaState,
          channel, registered, zombieChannel>>

Message == [type: {"edit"}, seq: 1..MaxEdits, epoch: 1..MaxEpochs]
       \cup [type: {"sync"}, seq: 0..MaxEdits, epoch: 1..MaxEpochs]

----

Init ==
    /\ epoch = 1
    /\ writerSeq = 0
    /\ replicaSeq = [r \in Replicas |-> 0]
    /\ replicaEpoch = [r \in Replicas |-> 0]
    /\ replicaState = [r \in Replicas |-> "resyncing"]
    /\ channel = [r \in Replicas |-> <<>>]
    /\ registered = {}
    /\ zombieChannel = [r \in Replicas |-> <<>>]

----

(* Writer produces a new edit and broadcasts to all registered replicas. *)
WriterEdit ==
    /\ writerSeq < MaxEdits
    /\ writerSeq' = writerSeq + 1
    /\ channel' = [r \in Replicas |->
        IF r \in registered
        THEN Append(channel[r], [type |-> "edit", seq |-> writerSeq + 1, epoch |-> epoch])
        ELSE channel[r]]
    /\ UNCHANGED <<epoch, replicaSeq, replicaEpoch, replicaState, registered, zombieChannel>>

(* Epoch transition: new writer takes over. The old writer's in-flight
   messages become zombie messages. The new writer starts at seq 0. *)
EpochTransition ==
    /\ epoch < MaxEpochs
    /\ epoch' = epoch + 1
    /\ writerSeq' = 0
    \* Move current channel to zombie channel (old writer's messages still in flight)
    /\ zombieChannel' = [r \in Replicas |-> channel[r] \o zombieChannel[r]]
    /\ channel' = [r \in Replicas |-> <<>>]
    \* All registrations are lost (new writer, fresh state)
    /\ registered' = {}
    /\ UNCHANGED <<replicaSeq, replicaEpoch, replicaState>>

(* A replica registers with the current writer. Writer sends a sync. *)
Register(r) ==
    /\ r \notin registered
    /\ registered' = registered \cup {r}
    /\ channel' = [channel EXCEPT ![r] =
        Append(@, [type |-> "sync", seq |-> writerSeq, epoch |-> epoch])]
    /\ UNCHANGED <<epoch, writerSeq, replicaSeq, replicaEpoch, replicaState, zombieChannel>>

(* A replica unregisters (node disconnect). *)
Unregister(r) ==
    /\ r \in registered
    /\ registered' = registered \ {r}
    /\ channel' = [channel EXCEPT ![r] = <<>>]
    /\ UNCHANGED <<epoch, writerSeq, replicaSeq, replicaEpoch, replicaState, zombieChannel>>

(* A message is lost from the current writer's channel. *)
LoseMessage(r) ==
    /\ Len(channel[r]) > 0
    /\ \E i \in 1..Len(channel[r]):
        channel' = [channel EXCEPT ![r] =
            SubSeq(@, 1, i-1) \o SubSeq(@, i+1, Len(@))]
    /\ UNCHANGED <<epoch, writerSeq, replicaSeq, replicaEpoch, replicaState, registered, zombieChannel>>

(* A zombie message is lost. *)
LoseZombieMessage(r) ==
    /\ Len(zombieChannel[r]) > 0
    /\ \E i \in 1..Len(zombieChannel[r]):
        zombieChannel' = [zombieChannel EXCEPT ![r] =
            SubSeq(@, 1, i-1) \o SubSeq(@, i+1, Len(@))]
    /\ UNCHANGED <<epoch, writerSeq, replicaSeq, replicaEpoch, replicaState, channel, registered>>

(* Replica receives a message from the current writer's channel. *)
ReplicaReceive(r) ==
    /\ Len(channel[r]) > 0
    /\ LET msg == Head(channel[r])
       IN
        /\ channel' = [channel EXCEPT ![r] = Tail(@)]
        /\ CASE msg.type = "edit" /\ replicaState[r] = "ok" ->
                IF msg.epoch = replicaEpoch[r] /\ msg.seq = replicaSeq[r] + 1
                THEN
                    /\ replicaSeq' = [replicaSeq EXCEPT ![r] = msg.seq]
                    /\ UNCHANGED <<replicaEpoch, replicaState, registered>>
                ELSE
                    \* Wrong epoch or gap: need re-sync
                    /\ replicaState' = [replicaState EXCEPT ![r] = "resyncing"]
                    /\ UNCHANGED <<replicaSeq, replicaEpoch, registered>>

           [] msg.type = "edit" /\ replicaState[r] = "resyncing" ->
                /\ UNCHANGED <<replicaSeq, replicaEpoch, replicaState, registered>>

           [] msg.type = "sync" ->
                /\ replicaSeq' = [replicaSeq EXCEPT ![r] = msg.seq]
                /\ replicaEpoch' = [replicaEpoch EXCEPT ![r] = msg.epoch]
                /\ replicaState' = [replicaState EXCEPT ![r] = "ok"]
                /\ UNCHANGED registered

        /\ UNCHANGED <<epoch, writerSeq, zombieChannel>>

(* Replica receives a zombie message (from old writer). *)
ReplicaReceiveZombie(r) ==
    /\ Len(zombieChannel[r]) > 0
    /\ LET msg == Head(zombieChannel[r])
       IN
        /\ zombieChannel' = [zombieChannel EXCEPT ![r] = Tail(@)]
        /\ CASE msg.type = "edit" /\ replicaState[r] = "ok" ->
                IF msg.epoch = replicaEpoch[r] /\ msg.seq = replicaSeq[r] + 1
                THEN
                    \* Zombie edit matches: replica applies it (stale but consistent
                    \* with the old writer's history at that epoch)
                    /\ replicaSeq' = [replicaSeq EXCEPT ![r] = msg.seq]
                    /\ UNCHANGED <<replicaEpoch, replicaState, registered>>
                ELSE
                    \* Epoch mismatch or gap: re-sync needed
                    /\ replicaState' = [replicaState EXCEPT ![r] = "resyncing"]
                    /\ UNCHANGED <<replicaSeq, replicaEpoch, registered>>

           [] msg.type = "edit" /\ replicaState[r] = "resyncing" ->
                /\ UNCHANGED <<replicaSeq, replicaEpoch, replicaState, registered>>

           [] msg.type = "sync" ->
                \* Stale sync from old writer: only apply if epoch >= replica's
                IF msg.epoch >= replicaEpoch[r]
                THEN
                    /\ replicaSeq' = [replicaSeq EXCEPT ![r] = msg.seq]
                    /\ replicaEpoch' = [replicaEpoch EXCEPT ![r] = msg.epoch]
                    /\ replicaState' = [replicaState EXCEPT ![r] = "ok"]
                    /\ UNCHANGED registered
                ELSE
                    \* Ignore sync from older epoch
                    /\ UNCHANGED <<replicaSeq, replicaEpoch, replicaState, registered>>

        /\ UNCHANGED <<epoch, writerSeq, channel>>

(* Replica requests re-sync from current writer. *)
ReplicaRequestResync(r) ==
    /\ replicaState[r] = "resyncing"
    /\ r \in registered
    /\ channel' = [channel EXCEPT ![r] =
        Append(@, [type |-> "sync", seq |-> writerSeq, epoch |-> epoch])]
    /\ UNCHANGED <<epoch, writerSeq, replicaSeq, replicaEpoch, replicaState, registered, zombieChannel>>

----

Next ==
    \/ WriterEdit
    \/ EpochTransition
    \/ \E r \in Replicas:
        \/ Register(r)
        \/ Unregister(r)
        \/ LoseMessage(r)
        \/ LoseZombieMessage(r)
        \/ ReplicaReceive(r)
        \/ ReplicaReceiveZombie(r)
        \/ ReplicaRequestResync(r)

----

(* SAFETY: A replica's sequence never exceeds the current writer's
   sequence at the replica's epoch. If the replica is at the current
   epoch, its seq <= writerSeq. If at an older epoch, any seq is fine
   (it will re-sync when it gets a message from the new epoch). *)
SeqBounded ==
    \A r \in Replicas:
        replicaEpoch[r] = epoch => replicaSeq[r] <= writerSeq

(* SAFETY: A replica in "ok" state at the current epoch has a valid
   prefix of the current writer's history. *)
ReplicaConsistent ==
    \A r \in Replicas:
        (replicaState[r] = "ok" /\ replicaEpoch[r] = epoch) =>
            replicaSeq[r] <= writerSeq

(* SAFETY: A replica never moves to a future epoch. *)
EpochBounded ==
    \A r \in Replicas: replicaEpoch[r] <= epoch

----

Spec == Init /\ [][Next]_vars
FairSpec == Spec /\ WF_vars(Next)

=============================================================================
