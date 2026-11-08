---------------------------- MODULE SQLogWitness ----------------------------
(* One behavior of SQLog, step by step: three nodes snapshot versions 1, 2 and 3, but the
   chunk of version 1 lands last. Trimming the snapshot log to the previous snapshot's chunk
   then removes the chunks of versions 2 and 3, while the effect log is trimmed to version 2:
   a node starting now finds no snapshot to restore. *)
EXTENDS SQLog

CONSTANTS n1, n2, n3, w1

VARIABLE step

Last(log) == log[Len(log)]

\* Producer n1 publishes an effect of the commands before `to`.
ProduceTo(to) == Produce(n1) /\ Last(fx').to = to

\* Node n writes its trim of `log`.
Trim(n, log) ==
    /\ WriteTrim(n)
    /\ \E t \in node[n].toTrim : t.log = log /\ node'[n].toTrim = node[n].toTrim \ {t}
    /\ trims' # trims

TakeEffect(log) == \E t \in trims : t.log = log /\ TrimTakesEffect(t)

Steps == <<
    "append", "append", "append",
    "produce 1", "apply n1", "capture n1",          \* n1 captures version 1
    "produce 2", "apply n1", "produce 3",
    "apply n2", "apply n2", "capture n2",           \* n2 captures version 2
    "apply n3", "apply n3", "apply n3", "capture n3", \* n3 captures version 3
    "chunk n2", "chunk n3", "chunk n1",             \* version 1's chunk lands last
    "manifest n1", "manifest n2", "manifest n3",
    "apply n2", "apply n2", "apply n2",             \* n2 sees its snapshot announced
    "trim n2 snap", "take effect snap",
    "apply n3", "apply n3", "apply n3",             \* n3 sees its snapshot announced
    "trim n3 fx", "take effect fx"
>>

Act(s) ==
    CASE s = "append" -> WriterAppend(w1)
      [] s = "produce 1" -> ProduceTo(1)
      [] s = "produce 2" -> ProduceTo(2)
      [] s = "produce 3" -> ProduceTo(3)
      [] s = "apply n1" -> Apply(n1)
      [] s = "apply n2" -> Apply(n2)
      [] s = "apply n3" -> Apply(n3)
      [] s = "capture n1" -> Capture(n1)
      [] s = "capture n2" -> Capture(n2)
      [] s = "capture n3" -> Capture(n3)
      [] s = "chunk n1" -> WriteChunk(n1)
      [] s = "chunk n2" -> WriteChunk(n2)
      [] s = "chunk n3" -> WriteChunk(n3)
      [] s = "manifest n1" -> WriteManifest(n1)
      [] s = "manifest n2" -> WriteManifest(n2)
      [] s = "manifest n3" -> WriteManifest(n3)
      [] s = "trim n2 snap" -> Trim(n2, "snap")
      [] s = "trim n3 fx" -> Trim(n3, "fx")
      [] s = "take effect snap" -> TakeEffect("snap")
      [] s = "take effect fx" -> TakeEffect("fx")

WInit == Init /\ step = 1

WNext == step <= Len(Steps) /\ Act(Steps[step]) /\ step' = step + 1

WSpec == WInit /\ [][WNext]_<<vars, step>>

\* Violated once every step has run, which shows the behavior is one of SQLog's.
Unfinished == step <= Len(Steps)

=============================================================================
