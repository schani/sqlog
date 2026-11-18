# S3Log.tla, annotated

The spec in [S3Log.tla](S3Log.tla), block by block. Code is verbatim; text after each block explains it.

## TLA+ in five lines

- A spec describes **states** (values of variables) and **steps** (pairs of states).
- An **action** is a formula about one step. `x` is the value before, `x'` the value after.
- `/\` is "and", `\/` is "or". A bulleted list of `/\` or `\/` lines is one big and/or; indentation groups them.
- A line without a primed variable is a **guard**: the action can only happen when it holds.
- `UNCHANGED <<a, b>>` means `a' = a /\ b' = b`. Every action must say what happens to every variable.

TLC, the model checker, starts from every initial state and explores every possible step, in every order, until it has seen every reachable state. It checks the invariants in each one.

---

```tla
-------------------------------- MODULE S3Log --------------------------------
(* Commands as S3 objects, a watermark object and an effect log of S3 objects.
   Command names are 1..N, standing for UUIDv7 names in key order. *)
EXTENDS Naturals, Sequences
```

- The module name must match the file name.
- `(* ... *)` is a comment.
- `EXTENDS` imports standard modules: `Naturals` for numbers and `..`, `Sequences` for `Len` and `Append`.

## What it assumes about S3

- A conditional PUT on one key is atomic: of two racing `If-None-Match: *` PUTs, one wins; `If-Match` loses no updates.
- Reads and writes of one key are linearizable: each takes effect at one instant between request and response, so once any client has read a value, no later read returns an older one. This covers the watermark while a raise is still in flight. AWS documents only read-after-write consistency; linearizability during an in-flight write is assumed, not documented.
- A LIST started after a PUT succeeded includes that key.

## Constants

```tla
CONSTANTS N, Producers
```

Parameters, fixed per run, set in [S3Log.cfg](S3Log.cfg):

- `N`: how many command names exist. Names are the numbers `1..N`. A real name is a UUIDv7; only its order matters, so a number stands in for it.
- `Producers`: a set of producer identities, e.g. `{p1, p2}`. Writers need no identity: each command name is its own writer.

## Variables

```tla
VARIABLES
    files,      \* command objects in the bucket
    acked,      \* commands whose writer was told they will be effected
    wm,         \* the watermark object
    effects,    \* the effect objects, in order
    prod        \* each producer's local state

vars == <<files, acked, wm, effects, prod>>
```

`\*` starts a line comment.

| Variable | Type | Meaning |
|---|---|---|
| `files` | set of names | Command objects that exist in S3. Only ever grows (no deletes in this model). |
| `acked` | set of names | Commands whose writer passed the second check. Not stored in S3; it is what writers know, kept so the invariants can talk about it. |
| `wm` | number | The `watermark` object. |
| `effects` | sequence of records | `effect-1`, `effect-2`, …. Each is `[ran ↦ set of names, last ↦ number]`: the commands it ran and its `last_effected`. Pages and results are left out. |
| `prod` | function from producer to record | Each producer's local memory; fields below. |

`vars` is a tuple of all variables, used by the fairness conditions and `Spec`.

## Helpers

```tla
Names == 1..N
Last == effects[Len(effects)].last
Effected == UNION {effects[i].ran : i \in 1..Len(effects)}
```

`==` defines a name; it is not assignment.

- `Names`: the set `{1, …, N}`.
- `Last`: `last_effected` of the newest effect. Sequences are indexed from 1, so `effects[Len(effects)]` is the last element.
- `Effected`: every command any effect ran. `{f(i) : i \in S}` builds a set; `UNION` merges a set of sets into one.

## Initial state

```tla
Init ==
    /\ files = {}
    /\ acked = {}
    /\ wm = 0
    /\ effects = <<[ran |-> {}, last |-> 0]>>
    /\ prod = [p \in Producers |-> [pc |-> "idle", base |-> 0, wm |-> 0, cursor |-> 0, seen |-> {}]]
```

In `Init`, `=` fixes a variable's starting value.

- No commands, nothing acked, watermark 0.
- `effects` starts with one dummy effect that ran nothing and has `last = 0`. This way `Last` always exists and no action needs a special case for an empty log. `<<a>>` is a one-element sequence; `[f |-> v]` is a record.
- `[p \in Producers |-> …]` is a function: one record per producer. The fields are:
  - `pc`: `"idle"` or `"scan"` (between `Read` and `Write`).
  - `base`: index of the effect it read; it will try to write `base + 1`.
  - `wm`: the watermark it read. A separate field from the global `wm`.
  - `cursor`: the last name it has listed.
  - `seen`: the names it found in the bucket.

## Writers

```tla
\* A writer's PUT lands, whenever: before or after the writer gave up.
Put(n) ==
    /\ n \notin files
    /\ files' = files \cup {n}
    /\ UNCHANGED <<acked, wm, effects, prod>>
```

`Put(n)` is an action with a parameter; `Next` below tries it for every name.

- Guard: `n` is not yet in the bucket. A name is written at most once (UUIDs are unique, and writers don't retry).
- Step: `n` appears in the bucket.
- Nothing ties this to the writer's progress. TLC may run `Put(n)` at any moment, which covers a PUT that lands long after the writer timed out.
- The first watermark check is not modelled. It only stops some writes from happening; allowing every write is a superset of behaviours, so whatever is safe here is safe with the check.

```tla
\* After its PUT succeeded, the writer reads the watermark.
Ack(n) ==
    /\ n \in files
    /\ n > wm
    /\ acked' = acked \cup {n}
    /\ UNCHANGED <<files, wm, effects, prod>>
```

The second check.

- Guard 1: the PUT has landed. A writer only learns of success after that.
- Guard 2: the name is above the watermark at this moment.
- Step: the writer reports "will be effected".
- A writer that sees `n <= wm` reports "unknown". The model needs no action for that: it is the same as never taking `Ack`.
- TLC may also run `Ack` long after `Put`, which covers a slow check.

## The watermark

```tla
\* Anyone raises the watermark, by compare-and-set.
Raise ==
    /\ \E w \in wm + 1 .. N : wm' = w
    /\ UNCHANGED <<files, acked, effects, prod>>
```

- `\E w \in S : P` is "for some `w` in `S`". Here it means the next `wm` is any value from `wm + 1` to `N`. TLC tries each.
- Because the raiser uses `If-Match`, read and write are one atomic step, so the watermark only grows. A raiser with a stale read gets 412 and nothing changes, which is a step the model doesn't need.
- "Any higher value" covers every raising policy: the newest name, now minus a lag, anything.

## Producers

```tla
\* A producer reads the latest effect and the watermark.
Read(p) ==
    /\ prod[p].pc = "idle"
    /\ Last < wm
    /\ prod' = [prod EXCEPT ![p] = [pc |-> "scan", base |-> Len(effects), wm |-> wm, cursor |-> Last, seen |-> {}]]
    /\ UNCHANGED <<files, acked, wm, effects>>
```

- Guard 1: the producer is between cycles.
- Guard 2: there is work: the watermark is above `last_effected`. Without this, a producer could start a cycle with nothing to list and nothing to write, and be stuck.
- `[prod EXCEPT ![p] = r]` is `prod` with `p`'s entry replaced by `r`; other producers are untouched.
- It records which effect it read (`base`), the watermark (`wm`), and starts listing after that effect's `last_effected` (`cursor`).
- Reading both in one step is a simplification. In S3 they are separate GETs, but a stale effect only makes the final conditional write fail, and `Last <= wm` always holds, so the order doesn't matter.

```tla
\* It lists the next name.
Scan(p) ==
    /\ prod[p].pc = "scan"
    /\ prod[p].cursor < prod[p].wm
    /\ LET n == prod[p].cursor + 1 IN
        prod' = [prod EXCEPT ![p].cursor = n,
                             ![p].seen = IF n \in files THEN @ \cup {n} ELSE @]
    /\ UNCHANGED <<files, acked, wm, effects>>
```

- Guard: listing stops at the watermark this producer read, not the current one. This is the "LIST only after reading W, only up to W" rule.
- `LET n == … IN` names a value for the formula after it.
- `![p].cursor = n` updates one field of `p`'s record. `@` stands for the old value of that field.
- Step: advance the cursor by one name and, if that name is in the bucket now, add it to `seen`.
- One name per step is the finest possible pagination. Commands can land between any two steps, so this covers every way a paginated LIST can interleave with PUTs.

```tla
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
```

- Guard: the producer has found at least one command, or has listed everything up to its watermark and found none.
- Having found a command, it may write after any number of further `Scan` steps: this is "the first N files".
- The effect is written only if it has commands, and only if `Len(effects) = base`. That is the conditional write: `effect-(base+1)` doesn't exist yet. The new effect records what was seen and the cursor as `last_effected`. If another producer won, nothing changes (412). If the producer found nothing, it writes nothing.
- `last_effected` is the cursor, which may be past the last command found. The protocol uses the last command found instead. That is the case of a producer that writes right after reaching it, so the model includes it, and what holds here holds for the protocol.
- Either way the producer goes back to `idle` and may start over.
- Producer crashes are not modelled. A crashed producer is one that stops taking steps. An effect PUT that lands late is the same as a producer that paused before `Write`.

## The whole system

```tla
Producer(p) == Read(p) \/ Scan(p) \/ Write(p)

Next ==
    \/ \E n \in Names : Put(n) \/ Ack(n)
    \/ Raise
    \/ \E p \in Producers : Producer(p)
```

`Next` is every possible step: any writer, the watermark, or any producer. TLC branches on each.

```tla
Spec == Init /\ [][Next]_vars /\ WF_vars(Raise) /\ WF_vars(Producer(CHOOSE p \in Producers : TRUE))
```

- `Init`: start in an initial state.
- `[][Next]_vars`: every step is a `Next` step or leaves all variables unchanged. Doing nothing is always allowed, so any process can stall at any point.
- `WF_vars(A)` (weak fairness): if `A` stays possible forever, it eventually happens. This rules out runs where everything stalls forever, which only matters for `Picked`:
  - the watermark keeps rising until it reaches `N`. The protocol has to provide this: something must raise the watermark even when no new commands arrive, or the last acked command waits forever.
  - one producer keeps working. `CHOOSE p \in Producers : TRUE` picks some fixed producer; which one doesn't matter, since they are interchangeable. The others get no fairness, so they may crash and stop forever at any point, including mid-scan.
- Writers get no fairness: a writer may stop forever after any step.

## Properties

```tla
\* No command is effected twice.
AtMostOnce == \A i, j \in 1..Len(effects) : i # j => effects[i].ran \cap effects[j].ran = {}
```

`\A` is "for all", `#` is "not equal", `=>` is "implies". No two effects share a command. This is the conditional effect write at work.

```tla
\* The effects never pass an acked command without running it.
NoSkip == \A n \in acked : n <= Last => n \in Effected
```

The core safety property. Once `last_effected` is at or above an acked name, that command is in some effect. It holds because:

1. The writer's PUT landed before its check saw `n > wm`.
2. A producer lists `n` only if it read a watermark `>= n`, so after that check, so after the PUT.
3. `last_effected` advances only through names the producer listed.

```tla
\* Every acked command is effected eventually.
Picked == \A n \in Names : n \in acked ~> n \in Effected
=============================================================================
```

- `P ~> Q` ("leads to"): whenever `P` holds, `Q` holds then or later.
- Liveness: it needs the fairness in `Spec`. Without fairness on `Raise`, the watermark could stay below `n` forever and the check fails.
- The `====` line ends the module.

## The config

```
SPECIFICATION Spec
CONSTANTS
    N = 4
    Producers = {p1, p2}
INVARIANTS AtMostOnce NoSkip
PROPERTY Picked
CHECK_DEADLOCK FALSE
```

- Four names, two producers. `p1` and `p2` are model values: opaque, distinct constants.
- `INVARIANTS` are checked in every reachable state, `PROPERTY` over whole runs.
- `CHECK_DEADLOCK FALSE`: once every name is written and effected, nothing can happen. That is the intended end, not a bug.
