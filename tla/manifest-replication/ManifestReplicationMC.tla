---- MODULE ManifestReplicationMC ----
EXTENDS ManifestReplication, TLC

MCReplicas == {"r1", "r2"}
MCMaxEdits == 3
MCMaxEpochs == 3

StateConstraint ==
    /\ writerSeq <= MCMaxEdits
    /\ epoch <= MCMaxEpochs
    /\ \A r \in MCReplicas: Len(channel[r]) <= 2
    /\ \A r \in MCReplicas: Len(zombieChannel[r]) <= 2

Invariant == SeqBounded /\ ReplicaConsistent /\ EpochBounded

=============================================================================
