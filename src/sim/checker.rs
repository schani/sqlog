//! A black-box isolation checker for list-append histories, after Elle (Kingsbury & Alvaro,
//! VLDB 2020). Clients append unique values to lists and read lists back. Since a read shows
//! every append before it in order, reads reveal each key's version order, and from that the
//! dependencies between transactions. A serializable history's dependencies have no cycle.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use serde::Serialize;

#[derive(Clone, Debug, Serialize, PartialEq)]
pub enum Mop {
    Append { key: u64, val: u64 },
    /// `list` is what the read returned; `None` if the outcome isn't known.
    Read { key: u64, list: Option<Vec<u64>> },
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq)]
pub enum Status {
    Ok,
    Aborted,
    /// The client doesn't know: it may have taken effect, once, at any later time.
    Unknown,
}

/// What a read-only operation promises about the state it sees.
#[derive(Clone, Copy, Debug, Serialize, PartialEq)]
pub enum Order {
    /// Follows every transaction acknowledged before it started. Read-write transactions also
    /// follow every operation that finished before they started.
    Strict,
    /// Follows the client's own acknowledged transactions.
    Session,
    /// Follows only what its node has already shown. Applies to all queries in addition.
    Stale,
}

#[derive(Clone, Debug, Serialize)]
pub struct Op {
    pub client: u64,
    /// The node incarnation that served it.
    pub node: u64,
    /// A read-write transaction, as opposed to a query.
    pub txn: bool,
    pub order: Order,
    /// Positions in the order clients observed events.
    pub invoke: u64,
    pub complete: u64,
    pub status: Status,
    pub mops: Vec<Mop>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct Anomaly {
    pub kind: String,
    pub detail: String,
    pub ops: Vec<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Edge {
    Ww,
    Wr,
    Rw,
    Rt,
}

const GARBAGE: u64 = u64::MAX;

/// Parses a stored list; anything unparseable reads as a value nobody appended.
pub fn parse_list(s: &str) -> Vec<u64> {
    if s.is_empty() {
        return Vec::new();
    }
    s.split(',').map(|v| v.parse().unwrap_or(GARBAGE)).collect()
}

pub fn check(ops: &[Op]) -> Vec<Anomaly> {
    let mut out = Vec::new();
    let mut report = |kind: &str, detail: String, ops: Vec<usize>| out.push(Anomaly { kind: kind.into(), detail, ops });

    // Who appended each value, and whether it's that operation's last append to the key.
    let mut writers: BTreeMap<u64, (usize, bool)> = BTreeMap::new();
    for (i, op) in ops.iter().enumerate() {
        let mut last: BTreeMap<u64, u64> = BTreeMap::new();
        for m in &op.mops {
            if let Mop::Append { key, val } = m {
                if writers.insert(*val, (i, false)).is_some() {
                    report("duplicate-append", format!("value {val} appended twice"), vec![i]);
                }
                last.insert(*key, *val);
            }
        }
        for val in last.values() {
            writers.get_mut(val).unwrap().1 = true;
        }
    }

    let mut longest: BTreeMap<u64, &[u64]> = BTreeMap::new();
    let mut rejected: BTreeSet<(usize, usize)> = BTreeSet::new();
    let reads = || {
        ops.iter().enumerate().filter(|(_, op)| op.status == Status::Ok).flat_map(|(i, op)| {
            op.mops.iter().enumerate().filter_map(move |(j, m)| match m {
                Mop::Read { key, list: Some(list) } => Some((i, j, *key, list.as_slice())),
                _ => None,
            })
        })
    };
    for (i, _, key, list) in reads() {
        let l = longest.entry(key).or_default();
        if list.len() > l.len() {
            *l = list;
        }
        let mut seen = BTreeSet::new();
        for v in list {
            if !seen.insert(*v) {
                report("duplicate-element", format!("key {key} holds {v} twice: {list:?}"), vec![i]);
            }
            match writers.get(v) {
                None => report("garbage-read", format!("key {key} holds {v}, which nobody appended"), vec![i]),
                Some((w, _)) if ops[*w].status == Status::Aborted => {
                    report("G1a", format!("read of key {key} shows {v}, from an aborted transaction"), vec![*w, i])
                }
                _ => {}
            }
        }
        if let Some(v) = list.last()
            && let Some((w, false)) = writers.get(v)
            && *w != i
        {
            report("G1b", format!("read of key {key} ends in {v}, an intermediate append"), vec![*w, i]);
        }
    }
    for (i, j, key, list) in reads() {
        let l = longest[&key];
        if !l.starts_with(list) {
            report("incompatible-order", format!("key {key}: {list:?} isn't a prefix of {l:?}"), vec![i]);
            rejected.insert((i, j));
        }
    }

    // A transaction's reads must reflect its own earlier operations.
    for (i, op) in ops.iter().enumerate().filter(|(_, op)| op.status == Status::Ok) {
        let mut state: BTreeMap<u64, (Option<Vec<u64>>, Vec<u64>)> = BTreeMap::new();
        for m in &op.mops {
            match m {
                Mop::Append { key, val } => {
                    let (known, own) = state.entry(*key).or_default();
                    match known {
                        Some(l) => l.push(*val),
                        None => own.push(*val),
                    }
                }
                Mop::Read { key, list: Some(list) } => {
                    let (known, own) = state.entry(*key).or_default();
                    let fine = match known {
                        Some(l) => l == list,
                        None => list.ends_with(own),
                    };
                    if !fine {
                        report("internal", format!("read of key {key} is {list:?}, after its own operations"), vec![i]);
                    }
                    *known = Some(list.clone());
                }
                Mop::Read { .. } => {}
            }
        }
    }

    // Everything acknowledged must be there at the end, and a transaction whose outcome isn't
    // known must have taken effect entirely or not at all.
    let visible: BTreeSet<u64> = longest.values().flat_map(|l| l.iter().copied()).collect();
    for (i, op) in ops.iter().enumerate() {
        let appends: Vec<u64> = op.mops.iter().filter_map(|m| if let Mop::Append { val, .. } = m { Some(*val) } else { None }).collect();
        let seen = appends.iter().filter(|v| visible.contains(v)).count();
        match op.status {
            Status::Ok if seen < appends.len() => {
                let lost: Vec<_> = appends.iter().filter(|v| !visible.contains(v)).collect();
                report("lost-write", format!("acknowledged appends {lost:?} are in no read"), vec![i]);
            }
            Status::Unknown if seen > 0 && seen < appends.len() => {
                report("partial-effect", format!("{seen} of {} appends took effect", appends.len()), vec![i]);
            }
            _ => {}
        }
    }

    // Dependencies between operations.
    let mut graph: Vec<Vec<(usize, Edge)>> = vec![Vec::new(); ops.len()];
    for list in longest.values() {
        for pair in list.windows(2) {
            if let (Some((a, _)), Some((b, _))) = (writers.get(&pair[0]), writers.get(&pair[1]))
                && a != b
            {
                graph[*a].push((*b, Edge::Ww));
            }
        }
    }
    for (i, op) in ops.iter().enumerate().filter(|(_, op)| op.status == Status::Ok) {
        let mut touched = BTreeSet::new();
        for (j, m) in op.mops.iter().enumerate() {
            match m {
                Mop::Append { key, .. } => {
                    touched.insert(*key);
                }
                Mop::Read { key, list: Some(list) } if touched.insert(*key) && !rejected.contains(&(i, j)) => {
                    if let Some((w, _)) = list.last().and_then(|v| writers.get(v))
                        && *w != i
                    {
                        graph[*w].push((i, Edge::Wr));
                    }
                    if let Some((w, _)) = longest[key].get(list.len()).and_then(|v| writers.get(v))
                        && *w != i
                    {
                        graph[i].push((*w, Edge::Rw));
                    }
                }
                Mop::Read { .. } => {}
            }
        }
    }
    add_realtime(ops, &mut graph);

    for scc in sccs(&graph) {
        let (cycle, edges) = shortest_cycle(&graph, &scc);
        let rw = edges.iter().filter(|e| **e == Edge::Rw).count();
        let kind = match rw {
            0 if edges.contains(&Edge::Wr) => "G1c",
            0 if edges.contains(&Edge::Ww) => "G0",
            0 => "realtime",
            1 => "G-single",
            _ => "G2",
        };
        let kind = if edges.contains(&Edge::Rt) && kind != "realtime" { format!("{kind}-realtime") } else { kind.to_string() };
        let path: Vec<String> = cycle.iter().zip(&edges).map(|(o, e)| format!("{o} -{e:?}->")).collect();
        report(&kind, format!("{} {}", path.join(" "), cycle[0]), cycle);
    }
    out
}

/// Adds what the order clients observed implies. `Order` says which pairs are promised.
fn add_realtime(ops: &[Op], graph: &mut [Vec<(usize, Edge)>]) {
    let mut done: Vec<usize> = (0..ops.len()).filter(|i| ops[*i].status == Status::Ok).collect();
    done.sort_by_key(|i| ops[*i].complete);
    // The latest start among the transactions in `done[..=i]`. Whatever finished before that
    // transaction started needs no edge of its own to anything after that transaction.
    let mut floor = Vec::with_capacity(done.len());
    let mut latest = 0;
    for i in &done {
        if ops[*i].txn {
            latest = latest.max(ops[*i].invoke);
        }
        floor.push(latest);
    }
    let mut by_node: BTreeMap<u64, Vec<usize>> = BTreeMap::new();
    let mut by_client: BTreeMap<u64, Vec<usize>> = BTreeMap::new();
    for i in &done {
        by_node.entry(ops[*i].node).or_default().push(*i);
        if ops[*i].txn {
            by_client.entry(ops[*i].client).or_default().push(*i);
        }
    }
    for (b, op) in ops.iter().enumerate() {
        if op.status == Status::Aborted {
            continue;
        }
        if op.txn || op.order == Order::Strict {
            let n = done.partition_point(|i| ops[*i].complete < op.invoke);
            if n > 0 {
                for a in done[..n].iter().rev().take_while(|a| ops[**a].complete > floor[n - 1]) {
                    if *a != b && (op.txn || ops[*a].txn) {
                        graph[*a].push((b, Edge::Rt));
                    }
                }
            }
        }
        if !op.txn {
            if op.order == Order::Session {
                for a in by_client.get(&op.client).into_iter().flatten().filter(|a| ops[**a].complete < op.invoke) {
                    graph[*a].push((b, Edge::Rt));
                }
            }
            for a in by_node[&op.node].iter().filter(|a| ops[**a].complete < op.invoke) {
                graph[*a].push((b, Edge::Rt));
            }
        }
    }
}

/// Strongly connected components with more than one member (Tarjan's, without recursion).
fn sccs(graph: &[Vec<(usize, Edge)>]) -> Vec<Vec<usize>> {
    let n = graph.len();
    let (mut index, mut low, mut on_stack) = (vec![usize::MAX; n], vec![0; n], vec![false; n]);
    let (mut counter, mut stack, mut out) = (0, Vec::new(), Vec::new());
    for root in 0..n {
        if index[root] != usize::MAX {
            continue;
        }
        let mut work = vec![(root, 0)];
        while let Some((v, next)) = work.pop() {
            if next == 0 {
                index[v] = counter;
                low[v] = counter;
                counter += 1;
                stack.push(v);
                on_stack[v] = true;
            }
            if let Some((w, _)) = graph[v].get(next) {
                work.push((v, next + 1));
                if index[*w] == usize::MAX {
                    work.push((*w, 0));
                } else if on_stack[*w] {
                    low[v] = low[v].min(index[*w]);
                }
                continue;
            }
            if let Some((parent, _)) = work.last() {
                low[*parent] = low[*parent].min(low[v]);
            }
            if low[v] == index[v] {
                let mut scc = Vec::new();
                while let Some(w) = stack.pop() {
                    on_stack[w] = false;
                    scc.push(w);
                    if w == v {
                        break;
                    }
                }
                if scc.len() > 1 {
                    scc.sort();
                    out.push(scc);
                }
            }
        }
    }
    out
}

/// The shortest cycle through the component's first member, preferring dependencies that don't
/// rely on real time, and the edges along it.
fn shortest_cycle(graph: &[Vec<(usize, Edge)>], scc: &[usize]) -> (Vec<usize>, Vec<Edge>) {
    let members: BTreeSet<usize> = scc.iter().copied().collect();
    let search = |start: usize, data_only: bool| -> Option<(Vec<usize>, Vec<Edge>)> {
        let mut came: BTreeMap<usize, (usize, Edge)> = BTreeMap::new();
        let mut queue = VecDeque::from([start]);
        while let Some(v) = queue.pop_front() {
            for (w, e) in &graph[v] {
                if !members.contains(w) || (data_only && *e == Edge::Rt) {
                    continue;
                }
                if *w == start {
                    let (mut nodes, mut edges) = (vec![v], vec![*e]);
                    let mut at = v;
                    while at != start {
                        let (p, pe) = came[&at];
                        nodes.push(p);
                        edges.push(pe);
                        at = p;
                    }
                    nodes.reverse();
                    edges.reverse();
                    return Some((nodes, edges));
                }
                if !came.contains_key(w) {
                    came.insert(*w, (v, *e));
                    queue.push_back(*w);
                }
            }
        }
        None
    };
    scc.iter()
        .find_map(|s| search(*s, true))
        .or_else(|| search(scc[0], false))
        .expect("a strongly connected component has a cycle")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a(key: u64, val: u64) -> Mop {
        Mop::Append { key, val }
    }

    fn r(key: u64, list: &[u64]) -> Mop {
        Mop::Read { key, list: Some(list.to_vec()) }
    }

    /// Operations that overlap, so real time implies nothing about them.
    fn concurrent(txns: Vec<(Status, Vec<Mop>)>) -> Vec<Op> {
        let n = txns.len() as u64;
        txns.into_iter()
            .enumerate()
            .map(|(i, (status, mops))| Op { client: i as u64, node: i as u64, txn: true, order: Order::Strict, invoke: i as u64, complete: n + i as u64, status, mops })
            .collect()
    }

    fn ok(mops: Vec<Mop>) -> (Status, Vec<Mop>) {
        (Status::Ok, mops)
    }

    fn kinds(ops: &[Op]) -> Vec<String> {
        check(ops).into_iter().map(|a| a.kind).collect()
    }

    #[test]
    fn serial_history_passes() {
        let ops = concurrent(vec![ok(vec![a(1, 1)]), ok(vec![r(1, &[1]), a(1, 2), a(2, 3)]), ok(vec![r(1, &[1, 2]), r(2, &[3])])]);
        assert_eq!(check(&ops), vec![]);
    }

    #[test]
    fn reading_your_own_appends_passes() {
        let ops = concurrent(vec![ok(vec![a(1, 1), r(1, &[1]), a(1, 2), r(1, &[1, 2])]), ok(vec![r(1, &[1, 2])])]);
        assert_eq!(check(&ops), vec![]);
    }

    #[test]
    fn g0_write_cycle() {
        // T0 before T1 on key 1, T1 before T0 on key 2.
        let ops = concurrent(vec![ok(vec![a(1, 1), a(2, 2)]), ok(vec![a(1, 3), a(2, 4)]), ok(vec![r(1, &[1, 3]), r(2, &[4, 2])])]);
        assert_eq!(kinds(&ops), ["G0"]);
    }

    #[test]
    fn g1a_aborted_read() {
        let ops = concurrent(vec![(Status::Aborted, vec![a(1, 1)]), ok(vec![r(1, &[1])])]);
        assert_eq!(kinds(&ops), ["G1a"]);
    }

    #[test]
    fn g1b_intermediate_read() {
        let ops = concurrent(vec![ok(vec![a(1, 1), a(1, 2)]), ok(vec![r(1, &[1])]), ok(vec![r(1, &[1, 2])])]);
        assert!(kinds(&ops).contains(&"G1b".to_string()));
    }

    #[test]
    fn g1c_circular_information_flow() {
        let ops = concurrent(vec![
            ok(vec![a(1, 1), r(2, &[2])]),
            ok(vec![a(2, 2), r(1, &[1])]),
        ]);
        assert_eq!(kinds(&ops), ["G1c"]);
    }

    #[test]
    fn g_single_read_skew() {
        // T1 sees T0's write to key 2 but not its write to key 1.
        let ops = concurrent(vec![ok(vec![a(1, 1), a(2, 2)]), ok(vec![r(1, &[]), r(2, &[2])]), ok(vec![r(1, &[1])])]);
        assert_eq!(kinds(&ops), ["G-single"]);
    }

    #[test]
    fn g2_write_skew() {
        // Each read the other's key before the other wrote it.
        let ops = concurrent(vec![ok(vec![r(1, &[]), a(2, 2)]), ok(vec![r(2, &[]), a(1, 1)]), ok(vec![r(1, &[1]), r(2, &[2])])]);
        assert_eq!(kinds(&ops), ["G2"]);
    }

    #[test]
    fn lost_write() {
        let ops = concurrent(vec![ok(vec![a(1, 1)]), ok(vec![a(1, 2)]), ok(vec![r(1, &[2])])]);
        assert_eq!(kinds(&ops), ["lost-write"]);
    }

    #[test]
    fn unknown_outcomes_may_or_may_not_apply() {
        let applied = concurrent(vec![(Status::Unknown, vec![a(1, 1), a(2, 2)]), ok(vec![r(1, &[1]), r(2, &[2])])]);
        assert_eq!(check(&applied), vec![]);
        let not_applied = concurrent(vec![(Status::Unknown, vec![a(1, 1), a(2, 2)]), ok(vec![r(1, &[]), r(2, &[])])]);
        assert_eq!(check(&not_applied), vec![]);
        let partial = concurrent(vec![(Status::Unknown, vec![a(1, 1), a(2, 2)]), ok(vec![r(1, &[1]), r(2, &[])])]);
        assert_eq!(kinds(&partial), ["partial-effect"]);
    }

    #[test]
    fn incompatible_orders_and_bad_elements() {
        let ops = concurrent(vec![ok(vec![a(1, 1)]), ok(vec![a(1, 2)]), ok(vec![r(1, &[1, 2])]), ok(vec![r(1, &[2, 1])])]);
        assert_eq!(kinds(&ops), ["incompatible-order"]);
        assert_eq!(kinds(&concurrent(vec![ok(vec![r(1, &[7])])])), ["garbage-read"]);
        let twice = concurrent(vec![ok(vec![a(1, 1)]), ok(vec![r(1, &[1, 1])])]);
        assert!(kinds(&twice).contains(&"duplicate-element".to_string()));
    }

    #[test]
    fn internal_inconsistency() {
        let ops = concurrent(vec![ok(vec![a(1, 1)]), ok(vec![r(1, &[]), a(1, 2), r(1, &[1, 2])]), ok(vec![r(1, &[1, 2])])]);
        assert!(kinds(&ops).contains(&"internal".to_string()));
    }

    fn timed(invoke: u64, complete: u64, txn: bool, order: Order, mops: Vec<Mop>) -> Op {
        Op { client: invoke, node: invoke, txn, order, invoke, complete, status: Status::Ok, mops }
    }

    #[test]
    fn stale_transaction_breaks_real_time() {
        // T1 finished before T2 started, yet T2 doesn't see it.
        let ops = vec![
            timed(0, 1, true, Order::Strict, vec![a(1, 1)]),
            timed(2, 3, true, Order::Strict, vec![r(1, &[])]),
            timed(4, 5, true, Order::Strict, vec![r(1, &[1])]),
        ];
        assert_eq!(kinds(&ops), ["G-single-realtime"]);
    }

    #[test]
    fn query_order_promises() {
        let history = |order, client, node| {
            let mut stale = timed(2, 3, false, order, vec![r(1, &[])]);
            stale.client = client;
            stale.node = node;
            let mut write = timed(0, 1, true, Order::Strict, vec![a(1, 1)]);
            write.client = 9;
            write.node = 9;
            vec![write, stale, timed(4, 5, true, Order::Strict, vec![r(1, &[1])])]
        };
        // A stale query may miss a write from another client through another node.
        assert_eq!(check(&history(Order::Stale, 1, 1)), vec![]);
        assert_eq!(check(&history(Order::Session, 1, 1)), vec![]);
        // But not through its own node, not its client's own, and not if it promised to be strict.
        assert_eq!(kinds(&history(Order::Stale, 1, 9)), ["G-single-realtime"]);
        assert_eq!(kinds(&history(Order::Session, 9, 1)), ["G-single-realtime"]);
        assert_eq!(kinds(&history(Order::Strict, 1, 1)), ["G-single-realtime"]);
    }

    #[test]
    fn stale_query_may_be_followed_by_older_strict_query() {
        // Q0 on a node ahead sees a write nobody has been told about; Q1, strict, doesn't.
        let ops = vec![
            timed(0, 9, true, Order::Strict, vec![a(1, 1)]),
            timed(1, 2, false, Order::Stale, vec![r(1, &[1])]),
            timed(3, 4, false, Order::Strict, vec![r(1, &[])]),
        ];
        assert_eq!(check(&ops), vec![]);
    }

    #[test]
    fn realtime_chains_are_found_through_compression() {
        // T0 before T1 before T2 by real time, so T0's edge to T2 is implied through T1.
        let ops = vec![
            timed(0, 1, true, Order::Strict, vec![a(1, 1)]),
            timed(2, 3, true, Order::Strict, vec![a(2, 2)]),
            timed(4, 5, true, Order::Strict, vec![r(1, &[]), r(2, &[2])]),
            timed(6, 7, true, Order::Strict, vec![r(1, &[1])]),
        ];
        assert!(kinds(&ops).iter().any(|k| k.ends_with("-realtime")), "{:?}", kinds(&ops));
    }
}
