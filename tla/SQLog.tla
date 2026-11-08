-------------------------------- MODULE SQLog --------------------------------
(* SQLog's log protocol: writers append command batches, producers race to append
   effects, every node applies the effect log in order, nodes snapshot to a third log,
   announce snapshots on the effect log, trim the logs, and restore from snapshots.

   The database is abstracted to the sequence of batches executed. An effect carries the
   batches its commands added, as a page delta carries what changed: a node applies it to
   whatever it holds, so a node holding the wrong state would stay wrong.

   Logs are never shortened here; a trim moves the log's start, and as on S2, a read from
   before the start returns the records from the start on. Invariants see the whole history.
   Records keep their 0-based sequence numbers: record s is log[s+1]. *)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Nodes,          \* every node applies the effect log and may snapshot
    Producers,      \* the nodes that execute commands
    Writers,
    Batches,        \* batches per writer
    MaxCrashes,
    MaxFx, MaxCmd, MaxSnap, \* log length bounds for model checking
    SnapTrim        \* "chunk": trim the snapshot log to the previous snapshot's chunk;
                    \* "version": to the first chunk of a version the effect log trim keeps

VARIABLES
    cmd, fx, snap,                  \* the logs
    cmdStart, fxStart, snapStart,   \* first sequence number each log still returns
    trims,          \* trims written to a log but not yet in effect
    node,           \* each node's local state
    wnext,          \* each writer's next batch number
    inflight,       \* appends whose outcome the writer didn't learn; they may land later
    crashes

vars == <<cmd, fx, snap, cmdStart, fxStart, snapStart, trims, node, wnext, inflight, crashes>>

Rec(log, s) == log[s + 1]
Max(a, b) == IF a > b THEN a ELSE b
Min(a, b) == IF a < b THEN a ELSE b

\* The first record a read from s returns, on a log starting at `start`.
First(start, s) == Max(start, s)

NoCapture == [phase |-> "none", version |-> 0, cmdNext |-> 0, db |-> <<>>, chunk |-> 0]
Fresh == [version |-> 0, db |-> <<>>, cmdNext |-> 0, known |-> <<>>, capture |-> NoCapture, toTrim |-> {}]

-----------------------------------------------------------------------------
(* Execution. A batch runs unless its writer already ran this batch or a later one. *)

Ran(db, r) == \E i \in DOMAIN db : db[i].w = r.w /\ db[i].b >= r.b

Exec(db, r) == IF r.kind = "batch" /\ ~Ran(db, r) THEN Append(db, [w |-> r.w, b |-> r.b]) ELSE db

RECURSIVE ExecRange(_, _, _)
ExecRange(db, from, to) == IF from >= to THEN db ELSE ExecRange(Exec(db, Rec(cmd, from)), from + 1, to)

\* What executing the command log before `to` must produce.
Truth(to) == ExecRange(<<>>, 0, to)

\* The state after effect log records before version v.
RECURSIVE DbAt(_), CmdNextAt(_)
DbAt(v) == IF v = 0 THEN <<>>
           ELSE IF Rec(fx, v - 1).kind = "effect" THEN DbAt(v - 1) \o Rec(fx, v - 1).delta ELSE DbAt(v - 1)
CmdNextAt(v) == IF v = 0 THEN 0
                ELSE IF Rec(fx, v - 1).kind = "effect" THEN Rec(fx, v - 1).to ELSE CmdNextAt(v - 1)

Newest(s) == IF s.known = <<>> THEN 0 ELSE s.known[Len(s.known)].version

\* Where to trim the snapshot log, for a snapshot trim to snapshot p's chunk, issued with the
\* effect log's trim to p's version. Chunks don't land in version order: a slow node may
\* write an old snapshot's chunk after newer ones.
SnapTrimPoint(t) ==
    IF SnapTrim = "chunk" THEN t.seq
    ELSE LET keep == {i \in snapStart .. t.seq - 1 : Rec(snap, i).kind = "chunk" /\ Rec(snap, i).version >= t.version}
         IN IF keep = {} THEN t.seq ELSE CHOOSE i \in keep : \A j \in keep : i <= j

-----------------------------------------------------------------------------
(* Writers. An append either lands and is acknowledged, or its outcome is unknown: the
   writer retries or gives up, and the lost append may still land later. *)

WriterAppend(w) ==
    /\ wnext[w] <= Batches /\ Len(cmd) < MaxCmd
    /\ cmd' = Append(cmd, [kind |-> "batch", w |-> w, b |-> wnext[w]])
    /\ wnext' = [wnext EXCEPT ![w] = @ + 1]
    /\ UNCHANGED <<fx, snap, cmdStart, fxStart, snapStart, trims, node, inflight, crashes>>

WriterUnsure(w) ==
    /\ wnext[w] <= Batches
    /\ inflight' = inflight \cup {[w |-> w, b |-> wnext[w]]}
    /\ \/ UNCHANGED wnext
       \/ wnext' = [wnext EXCEPT ![w] = @ + 1]
    /\ UNCHANGED <<cmd, fx, snap, cmdStart, fxStart, snapStart, trims, node, crashes>>

LateLand(a) ==
    /\ Len(cmd) < MaxCmd
    /\ cmd' = Append(cmd, [kind |-> "batch", w |-> a.w, b |-> a.b])
    /\ inflight' = inflight \ {a}
    /\ UNCHANGED <<fx, snap, cmdStart, fxStart, snapStart, trims, node, wnext, crashes>>

-----------------------------------------------------------------------------
(* Producers execute the command records after their state and append the effect only if
   the effect log's tail is still the version they executed on. *)

Produce(n) ==
    LET s == node[n]
        from == First(cmdStart, s.cmdNext)
    IN
    /\ Len(fx) < MaxFx
    /\ Len(fx) = s.version
    /\ from = s.cmdNext \* otherwise the next commands were trimmed
    /\ \E to \in from + 1 .. Len(cmd) :
        LET db == ExecRange(s.db, from, to) IN
        fx' = Append(fx, [kind |-> "effect", base |-> s.version, from |-> from, to |-> to,
                          delta |-> SubSeq(db, Len(s.db) + 1, Len(db))])
    /\ UNCHANGED <<cmd, snap, cmdStart, fxStart, snapStart, trims, node, wnext, inflight, crashes>>

-----------------------------------------------------------------------------
(* Applying the effect log. Trims and snapshot announcements are versions that change
   nothing. A node that sees its own snapshot announced, newer than any before, trims each
   log to the snapshot before it. *)

Announce(n, s, m) ==
    IF s.known # <<>> /\ s.known[Len(s.known)].version >= m.version THEN s
    ELSE LET prev == s.known
             known == IF prev = <<>> THEN <<m>> ELSE <<prev[Len(prev)], m>>
             toTrim == IF prev # <<>> /\ m.writer = n
                       THEN LET p == prev[Len(prev)] IN
                            {[log |-> "fx", seq |-> p.version, version |-> p.version],
                             [log |-> "cmd", seq |-> p.cmdNext, version |-> p.version],
                             [log |-> "snap", seq |-> p.chunk, version |-> p.version]}
                       ELSE {}
         IN [s EXCEPT !.known = known, !.toTrim = @ \cup toTrim]

Apply(n) ==
    LET s == node[n]
        i == First(fxStart, s.version)
        r == Rec(fx, i)
        s1 == [s EXCEPT !.version = i + 1]
    IN
    /\ i < Len(fx)
    /\ i = s.version \* otherwise the next record was trimmed: restore
    /\ node' = [node EXCEPT ![n] =
                  CASE r.kind = "effect" -> [s1 EXCEPT !.db = s.db \o r.delta, !.cmdNext = r.to]
                    [] r.kind = "manifest" -> Announce(n, s1, r)
                    [] OTHER -> s1]
    /\ UNCHANGED <<cmd, fx, snap, cmdStart, fxStart, snapStart, trims, wnext, inflight, crashes>>

\* A trim is a record on its log; the log's start moves some time after it lands, never back.
WriteTrim(n) ==
    \E t \in node[n].toTrim :
        /\ node' = [node EXCEPT ![n].toTrim = @ \ {t}]
        /\ \/ /\ t.log = "fx" /\ Len(fx) < MaxFx
              /\ fx' = Append(fx, [kind |-> "trim"])
              /\ trims' = trims \cup {[log |-> "fx", seq |-> t.seq, at |-> Len(fx)]}
              /\ UNCHANGED <<cmd, snap>>
           \/ /\ t.log = "cmd" /\ Len(cmd) < MaxCmd
              /\ cmd' = Append(cmd, [kind |-> "trim"])
              /\ trims' = trims \cup {[log |-> "cmd", seq |-> t.seq, at |-> Len(cmd)]}
              /\ UNCHANGED <<fx, snap>>
           \/ /\ t.log = "snap" /\ Len(snap) < MaxSnap
              /\ snap' = Append(snap, [kind |-> "trim"])
              /\ trims' = trims \cup {[log |-> "snap", seq |-> SnapTrimPoint(t), at |-> Len(snap)]}
              /\ UNCHANGED <<cmd, fx>>
           \/ /\ UNCHANGED <<cmd, fx, snap, trims>> \* the trim failed
        /\ UNCHANGED <<cmdStart, fxStart, snapStart, wnext, inflight, crashes>>

TrimTakesEffect(t) ==
    LET to == Min(t.seq, t.at + 1) IN
    /\ trims' = trims \ {t}
    /\ CASE t.log = "fx" -> fxStart' = Max(fxStart, to) /\ UNCHANGED <<cmdStart, snapStart>>
         [] t.log = "cmd" -> cmdStart' = Max(cmdStart, to) /\ UNCHANGED <<fxStart, snapStart>>
         [] t.log = "snap" -> snapStart' = Max(snapStart, to) /\ UNCHANGED <<cmdStart, fxStart>>
    /\ UNCHANGED <<cmd, fx, snap, node, wnext, inflight, crashes>>

-----------------------------------------------------------------------------
(* Snapshots. A node captures its state, writes it as a chunk, then announces it; it gives
   up once it knows of a snapshot as new. Which versions get captured is left open. *)

Capture(n) ==
    LET s == node[n] IN
    /\ s.capture.phase = "none"
    /\ Newest(s) < s.version
    /\ node' = [node EXCEPT ![n].capture = [phase |-> "chunk", version |-> s.version, cmdNext |-> s.cmdNext, db |-> s.db, chunk |-> 0]]
    /\ UNCHANGED <<cmd, fx, snap, cmdStart, fxStart, snapStart, trims, wnext, inflight, crashes>>

WriteChunk(n) ==
    LET c == node[n].capture IN
    /\ c.phase = "chunk"
    /\ IF Newest(node[n]) >= c.version
       THEN node' = [node EXCEPT ![n].capture = NoCapture] /\ UNCHANGED snap
       ELSE /\ Len(snap) < MaxSnap
            /\ snap' = Append(snap, [kind |-> "chunk", version |-> c.version, db |-> c.db])
            /\ node' = [node EXCEPT ![n].capture = [c EXCEPT !.phase = "manifest", !.chunk = Len(snap)]]
    /\ UNCHANGED <<cmd, fx, cmdStart, fxStart, snapStart, trims, wnext, inflight, crashes>>

WriteManifest(n) ==
    LET c == node[n].capture IN
    /\ c.phase = "manifest"
    /\ IF Newest(node[n]) >= c.version
       THEN UNCHANGED fx
       ELSE /\ Len(fx) < MaxFx
            /\ fx' = Append(fx, [kind |-> "manifest", writer |-> n, version |-> c.version, cmdNext |-> c.cmdNext, chunk |-> c.chunk])
    /\ node' = [node EXCEPT ![n].capture = NoCapture]
    /\ UNCHANGED <<cmd, snap, cmdStart, fxStart, snapStart, trims, wnext, inflight, crashes>>

\* A node whose next record was trimmed loads the newest complete snapshot it can catch up from.
Restorable(s) ==
    {Rec(fx, i) : i \in {j \in fxStart .. Len(fx) - 1 :
        /\ Rec(fx, j).kind = "manifest"
        /\ Rec(fx, j).version >= Max(fxStart, s.version + 1)
        /\ First(snapStart, Rec(fx, j).chunk) = Rec(fx, j).chunk}}

Restore(n) ==
    LET s == node[n] IN
    /\ s.version < fxStart
    /\ fxStart < Len(fx)
    /\ \E m \in Restorable(s) :
        /\ \A o \in Restorable(s) : o.version <= m.version
        /\ node' = [node EXCEPT ![n] = [s EXCEPT !.version = m.version, !.db = Rec(snap, First(snapStart, m.chunk)).db,
                                                 !.cmdNext = m.cmdNext, !.known = <<m>>]]
    /\ UNCHANGED <<cmd, fx, snap, cmdStart, fxStart, snapStart, trims, wnext, inflight, crashes>>

\* A node restarts empty; its snapshot and trims in progress are lost.
Crash(n) ==
    /\ crashes < MaxCrashes
    /\ node' = [node EXCEPT ![n] = Fresh]
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<cmd, fx, snap, cmdStart, fxStart, snapStart, trims, wnext, inflight>>

-----------------------------------------------------------------------------

Init ==
    /\ cmd = <<>> /\ fx = <<>> /\ snap = <<>>
    /\ cmdStart = 0 /\ fxStart = 0 /\ snapStart = 0
    /\ trims = {}
    /\ node = [n \in Nodes |-> Fresh]
    /\ wnext = [w \in Writers |-> 1]
    /\ inflight = {}
    /\ crashes = 0

Next ==
    \/ \E w \in Writers : WriterAppend(w) \/ WriterUnsure(w)
    \/ \E a \in inflight : LateLand(a)
    \/ \E n \in Producers : Produce(n)
    \/ \E n \in Nodes : Apply(n) \/ WriteTrim(n) \/ Capture(n) \/ WriteChunk(n) \/ WriteManifest(n) \/ Restore(n) \/ Crash(n)
    \/ \E t \in trims : TrimTakesEffect(t)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Invariants. *)

\* Every effect follows the one before it and holds what executing its commands adds.
EffectsCorrect ==
    \A v \in 0 .. Len(fx) - 1 :
        LET r == Rec(fx, v) IN
        r.kind = "effect" => r.base = v /\ r.from = CmdNextAt(v) /\ DbAt(v) \o r.delta = Truth(r.to)

\* So the database runs each batch at most once, in command log order.
ExactlyOnce ==
    LET db == DbAt(Len(fx)) IN
    \A i, j \in DOMAIN db : i < j => db[i] # db[j]

\* Every node holds the state its version names.
NodesCorrect ==
    \A n \in Nodes : node[n].db = DbAt(node[n].version) /\ node[n].cmdNext = CmdNextAt(node[n].version)

\* A snapshot holds the state, and covers the commands, of its version.
SnapshotsCorrect ==
    \A i \in 0 .. Len(fx) - 1 :
        LET m == Rec(fx, i) IN
        m.kind = "manifest" =>
            /\ m.version <= i
            /\ Rec(snap, m.chunk).kind = "chunk"
            /\ Rec(snap, m.chunk).version = m.version
            /\ Rec(snap, m.chunk).db = DbAt(m.version)
            /\ m.cmdNext = CmdNextAt(m.version)

\* A node starting now can catch up: from the log, or from a complete snapshot.
Recoverable ==
    \/ fxStart = 0
    \/ Restorable(Fresh) # {}

\* A producer at the tail finds the commands it has to execute next.
CommandsRetained ==
    \A n \in Producers : node[n].version = Len(fx) => node[n].cmdNext >= cmdStart

\* A node holding the snapshot before the newest can catch up from the log: only nodes
\* further behind restore.
OnlyLaggardsRestore ==
    LET V == {Rec(fx, i).version : i \in {j \in 0 .. Len(fx) - 1 : Rec(fx, j).kind = "manifest"}}
    IN fxStart = 0 \/ \E a, b \in V : a < b /\ fxStart <= a

\* A restore leaves the node where it can continue from the log.
RestoresProgress == [][\A n \in Nodes : Restore(n) => node'[n].version >= fxStart]_vars

NodeSymmetry == Permutations(Nodes)

=============================================================================
