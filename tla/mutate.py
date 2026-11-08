"""Writes copies of SQLog.tla with planted bugs: python3 mutate.py SQLog.tla <dir>."""
import pathlib
import sys

src = pathlib.Path(sys.argv[1]).read_text()
out = pathlib.Path(sys.argv[2])
mutants = {
    "UnconditionalEffect": [("    /\\ Len(fx) = s.version\n", "")],
    "NoDedup": [("IF r.kind = \"batch\" /\\ ~Ran(db, r) THEN", "IF r.kind = \"batch\" THEN")],
    "ApplyPastTrim": [("    /\\ i = s.version \\* otherwise the next record was trimmed: restore\n", "")],
    "ExecutePastTrim": [("    /\\ from = s.cmdNext \\* otherwise the next commands were trimmed\n", "")],
    "RestoreTrimmedChunk": [("\n        /\\ First(snapStart, Rec(fx, j).chunk) = Rec(fx, j).chunk}}", "}}")],
    "RestoreNoFloor": [("        /\\ Rec(fx, j).version >= Max(fxStart, s.version + 1)\n", "")],
    "RestoreAnyOld": [("        /\\ Rec(fx, j).version >= Max(fxStart, s.version + 1)\n", ""),
                      ("        /\\ \\A o \\in Restorable(s) : o.version <= m.version\n", "")],
    "RestoreCmdOffByOne": [("!.cmdNext = m.cmdNext, !.known", "!.cmdNext = m.cmdNext + 1, !.known")],
    "CaptureLabelLate": [("version |-> s.version, cmdNext |-> s.cmdNext, db |-> s.db, chunk |-> 0]",
                          "version |-> s.version + 1, cmdNext |-> s.cmdNext, db |-> s.db, chunk |-> 0]")],
    "CmdTrimOffByOne": [("seq |-> p.cmdNext, version", "seq |-> p.cmdNext + 1, version")],
    "FxTrimOffByOne": [("[log |-> \"fx\", seq |-> p.version,", "[log |-> \"fx\", seq |-> p.version + 1,")],
    "TrimToNewest": [("THEN LET p == prev[Len(prev)] IN", "THEN LET p == m IN")],
}
out.mkdir(parents=True, exist_ok=True)
for name, edits in mutants.items():
    t = src
    for a, b in edits:
        assert t.count(a) == 1, (name, a)
        t = t.replace(a, b)
    (out / f"{name}.tla").write_text(t.replace("MODULE SQLog ", f"MODULE {name} "))
