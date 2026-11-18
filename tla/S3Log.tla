-------------------------------- MODULE S3Log --------------------------------
(* Commands as S3 objects, a watermark object and an effect log of S3 objects.
   Command names are 1..N, standing for UUIDv7 names in key order. *)
EXTENDS Naturals, Sequences

CONSTANTS N, Producers

VARIABLES
    files,      \* command objects in the bucket
    acked,      \* commands whose writer was told they will be effected
    wm,         \* the watermark object
    effects,    \* the effect objects, in order
    prod        \* each producer's local state

vars == <<files, acked, wm, effects, prod>>

Names == 1..N
Last == effects[Len(effects)].last
Effected == UNION {effects[i].ran : i \in 1..Len(effects)}

Init ==
    /\ files = {}
    /\ acked = {}
    /\ wm = 0
    /\ effects = <<[ran |-> {}, last |-> 0]>>
    /\ prod = [p \in Producers |-> [pc |-> "idle", base |-> 0, wm |-> 0, cursor |-> 0, seen |-> {}]]

\* A writer's PUT lands, whenever: before or after the writer gave up.
Put(n) ==
    /\ n \notin files
    /\ files' = files \cup {n}
    /\ UNCHANGED <<acked, wm, effects, prod>>

\* After its PUT succeeded, the writer reads the watermark.
Ack(n) ==
    /\ n \in files
    /\ n > wm
    /\ acked' = acked \cup {n}
    /\ UNCHANGED <<files, wm, effects, prod>>

\* Anyone raises the watermark, by compare-and-set.
Raise ==
    /\ \E w \in wm + 1 .. N : wm' = w
    /\ UNCHANGED <<files, acked, effects, prod>>

\* A producer reads the latest effect and the watermark.
Read(p) ==
    /\ prod[p].pc = "idle"
    /\ Last < wm
    /\ prod' = [prod EXCEPT ![p] = [pc |-> "scan", base |-> Len(effects), wm |-> wm, cursor |-> Last, seen |-> {}]]
    /\ UNCHANGED <<files, acked, wm, effects>>

\* It lists the next name.
Scan(p) ==
    /\ prod[p].pc = "scan"
    /\ prod[p].cursor < prod[p].wm
    /\ LET n == prod[p].cursor + 1 IN
        prod' = [prod EXCEPT ![p].cursor = n,
                             ![p].seen = IF n \in files THEN @ \cup {n} ELSE @]
    /\ UNCHANGED <<files, acked, wm, effects>>

\* It writes the next effect, if it found commands and no other producer has.
\* Finding none, it gives up once it has listed up to its watermark.
Write(p) ==
    /\ prod[p].pc = "scan"
    /\ prod[p].seen # {} \/ prod[p].cursor = prod[p].wm
    /\ effects' = IF Len(effects) = prod[p].base /\ prod[p].seen # {}
                  THEN Append(effects, [ran |-> prod[p].seen, last |-> prod[p].cursor])
                  ELSE effects
    /\ prod' = [prod EXCEPT ![p].pc = "idle"]
    /\ UNCHANGED <<files, acked, wm>>

Producer(p) == Read(p) \/ Scan(p) \/ Write(p)

Next ==
    \/ \E n \in Names : Put(n) \/ Ack(n)
    \/ Raise
    \/ \E p \in Producers : Producer(p)

Spec == Init /\ [][Next]_vars /\ WF_vars(Raise) /\ WF_vars(Producer(CHOOSE p \in Producers : TRUE))

\* No command is effected twice.
AtMostOnce == \A i, j \in 1..Len(effects) : i # j => effects[i].ran \cap effects[j].ran = {}

\* The effects never pass an acked command without running it.
NoSkip == \A n \in acked : n <= Last => n \in Effected

\* Every acked command is effected eventually.
Picked == \A n \in Names : n \in acked ~> n \in Effected
=============================================================================
