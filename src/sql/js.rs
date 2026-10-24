//! JavaScript transactions on QuickJS. The code runs as a strict-mode function body with
//! `args` and `db` in scope; what it returns, as JSON, is the transaction's result, and if it
//! throws, the transaction aborts.
//!
//! - `db.query(sql, params)` returns the rows as objects keyed by column name.
//! - `db.exec(sql, params)` returns the number of rows changed.
//!
//! Each thread keeps one context. After setup, everything reachable from it is frozen, so a
//! transaction can't leave state behind for the next. Promise jobs run before the transaction
//! ends; weak references, whose callbacks could run later, are left out. Limits on memory,
//! stack and operations make a runaway script abort its transaction instead of stalling
//! producers.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use rquickjs::context::intrinsic::{Date, Eval, Json as JsonIntrinsic, MapSet, Promise, Proxy, RegExp, RegExpCompiler, TypedArrays};
use rquickjs::{Context, Function, Runtime};
use rusqlite::Connection;
use serde_json::{Map, Value as Json};

use super::node::{json_to_sql, value_to_json};

/// QuickJS calls the interrupt handler about every 10,000 operations.
const MAX_INTERRUPTS: u32 = 10_000;
const MAX_MEMORY: usize = 64 << 20;
const MAX_STACK: usize = 1 << 20;

/// Native functions return `{"ok": …}` or `{"error": …}`; the wrappers throw on errors.
const PRELUDE: &str = r#"
(function () {
  const unwrap = (r) => {
    r = JSON.parse(r);
    if ("error" in r) throw new Error(r.error);
    return r.ok;
  };
  const query = __query, exec = __exec;
  const db = {
    query(sql, params) { return unwrap(query(sql, JSON.stringify(params ?? []))); },
    exec(sql, params) { return unwrap(exec(sql, JSON.stringify(params ?? []))); },
  };
  globalThis.__call = (f, args) => JSON.stringify(f(JSON.parse(args), db)) ?? "null";
  delete globalThis.__query;
  delete globalThis.__exec;

  // Freeze everything reachable, including prototypes only reachable from instances.
  const seen = new Set();
  const freeze = (o) => {
    if ((typeof o !== "object" && typeof o !== "function") || o === null || seen.has(o)) return;
    seen.add(o);
    Object.freeze(o);
    for (const k of Reflect.ownKeys(o)) {
      const d = Object.getOwnPropertyDescriptor(o, k);
      if ("value" in d) freeze(d.value);
      else { freeze(d.get); freeze(d.set); }
    }
    freeze(Object.getPrototypeOf(o));
  };
  const gen = function* () {};
  [globalThis, db, [].values(), new Map().entries(), new Set().values(), ""[Symbol.iterator](), /x/g[Symbol.matchAll](""),
   gen, gen(), new Uint8Array(0)].forEach(freeze);
})();
"#;

struct Js {
    rt: Runtime,
    ctx: Context,
    interrupts: Rc<Cell<u32>>,
}

thread_local! {
    static JS: RefCell<Option<Js>> = const { RefCell::new(None) };
    /// The connection of the transaction running on this thread.
    static CONN: Cell<*const Connection> = const { Cell::new(std::ptr::null()) };
}

fn setup() -> rquickjs::Result<Js> {
    let rt = Runtime::new()?;
    rt.set_memory_limit(MAX_MEMORY);
    rt.set_max_stack_size(MAX_STACK);
    let interrupts = Rc::new(Cell::new(0));
    let counter = interrupts.clone();
    rt.set_interrupt_handler(Some(Box::new(move || {
        counter.set(counter.get() + 1);
        counter.get() > MAX_INTERRUPTS
    })));
    let ctx = Context::builder()
        .with::<Eval>()
        .with::<JsonIntrinsic>()
        .with::<Date>()
        .with::<RegExpCompiler>()
        .with::<RegExp>()
        .with::<MapSet>()
        .with::<TypedArrays>()
        .with::<Proxy>()
        .with::<Promise>()
        .build(&rt)?;
    ctx.with(|ctx| {
        let native = |f: fn(&Connection, &str, &[Json]) -> Result<Json, String>| {
            Function::new(ctx.clone(), move |sql: String, params: String| -> String {
                let conn = CONN.get();
                assert!(!conn.is_null(), "JavaScript ran outside a transaction");
                let r = serde_json::from_str::<Vec<Json>>(&params)
                    .map_err(|e| e.to_string())
                    .and_then(|p| f(unsafe { &*conn }, &sql, &p));
                match r {
                    Ok(v) => serde_json::json!({ "ok": v }),
                    Err(e) => serde_json::json!({ "error": e }),
                }
                .to_string()
            })
        };
        ctx.globals().set("__query", native(query)?)?;
        ctx.globals().set("__exec", native(exec)?)?;
        ctx.eval::<(), _>(PRELUDE)
    })?;
    Ok(Js { rt, ctx, interrupts })
}

pub fn run(conn: &Connection, code: &str, args: &Json) -> Result<Json, String> {
    JS.with_borrow_mut(|js| {
        if js.is_none() {
            *js = Some(setup().map_err(|e| format!("setting up JavaScript: {e}"))?);
        }
        let Js { rt, ctx, interrupts } = js.as_ref().unwrap();
        interrupts.set(0);
        CONN.set(conn);
        let result = ctx.with(|ctx| {
            let call = || -> rquickjs::Result<String> {
                let f: Function = ctx.eval(format!("(function (args, db) {{\n'use strict';\n{code}\n}})"))?;
                let call: Function = ctx.globals().get("__call")?;
                call.call((f, args.to_string()))
            };
            call().map_err(|e| match e {
                rquickjs::Error::Exception => {
                    let e = ctx.catch();
                    let msg = e.as_exception().and_then(|x| x.message()).or_else(|| e.as_string().and_then(|s| s.to_string().ok()));
                    msg.unwrap_or_else(|| format!("{e:?}"))
                }
                e => e.to_string(),
            })
        });
        while rt.is_job_pending() && rt.execute_pending_job().is_ok() {}
        CONN.set(std::ptr::null());
        // A context that ran out of operations or memory, or whose jobs failed, may be left in
        // a bad state.
        let exhausted = interrupts.get() > MAX_INTERRUPTS;
        if exhausted || rt.is_job_pending() || result.as_ref().is_err_and(|e| e.contains("out of memory")) {
            *js = None;
        }
        if exhausted {
            return Err("JavaScript exceeded its operation budget".into());
        }
        serde_json::from_str(&result?).map_err(|e| e.to_string())
    })
}

fn query(conn: &Connection, sql: &str, params: &[Json]) -> Result<Json, String> {
    let mut st = conn.prepare_cached(sql).map_err(|e| e.to_string())?;
    let columns: Vec<String> = st.column_names().map(|c| c.to_string()).collect();
    let mut q = st.query(rusqlite::params_from_iter(params.iter().map(json_to_sql))).map_err(|e| e.to_string())?;
    let mut rows = Vec::new();
    while let Some(row) = q.next().map_err(|e| e.to_string())? {
        let mut obj = Map::new();
        for (i, c) in columns.iter().enumerate() {
            obj.insert(c.clone(), row.get_ref(i).map(value_to_json).map_err(|e| e.to_string())?);
        }
        rows.push(Json::Object(obj));
    }
    Ok(rows.into())
}

fn exec(conn: &Connection, sql: &str, params: &[Json]) -> Result<Json, String> {
    let mut st = conn.prepare_cached(sql).map_err(|e| e.to_string())?;
    Ok(st.execute(rusqlite::params_from_iter(params.iter().map(json_to_sql))).map_err(|e| e.to_string())?.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conn() -> Connection {
        let c = Connection::open_in_memory().unwrap();
        c.execute_batch("CREATE TABLE t(x INTEGER); INSERT INTO t VALUES (1), (2);").unwrap();
        c
    }

    #[test]
    fn state_does_not_leak_between_transactions() {
        let c = conn();
        for code in [
            "globalThis.leak = 1",
            "leak = 1",
            "Array.prototype.leak = 1",
            "Object.getPrototypeOf([].values()).leak = 1",
            "JSON.parse = () => 1",
            "db.query = () => 1",
            "Promise.resolve().then(() => { throw 1; }); globalThis.x = 1",
        ] {
            assert!(run(&c, code, &Json::Null).is_err(), "{code} succeeded");
        }
        let r = run(&c, "return [typeof leak, [].leak ?? null, JSON.parse('2'), db.query('SELECT 1 AS one')]", &Json::Null);
        assert_eq!(r.unwrap(), serde_json::json!(["undefined", null, 2, [{ "one": 1 }]]));
    }

    #[test]
    fn promise_jobs_run_inside_the_transaction() {
        let c = conn();
        let code = "Promise.resolve().then(() => db.exec('INSERT INTO t VALUES (10)')); return 'queued';";
        assert_eq!(run(&c, code, &Json::Null).unwrap(), Json::from("queued"));
        let n: i64 = c.query_row("SELECT count(*) FROM t WHERE x = 10", [], |r| r.get(0)).unwrap();
        assert_eq!(n, 1);
    }

    #[test]
    fn rebuilt_after_exhaustion() {
        let c = conn();
        assert!(run(&c, "for (;;) {}", &Json::Null).unwrap_err().contains("budget"));
        assert!(run(&c, "const a = []; for (;;) a.push('x'.repeat(10000));", &Json::Null).is_err());
        assert_eq!(run(&c, "return db.query('SELECT sum(x) AS s FROM t')[0].s + args", &Json::from(1)).unwrap(), Json::from(4));
    }

    #[test]
    #[ignore]
    fn cost_per_transaction() {
        let c = conn();
        let code = "const [{ s }] = db.query('SELECT sum(x) AS s FROM t'); db.exec('UPDATE t SET x = x + ?1 WHERE x < 0', [args]); return s;";
        let t = std::time::Instant::now();
        for _ in 0..10_000 {
            run(&c, code, &Json::from(1)).unwrap();
        }
        println!("{:?} per transaction", t.elapsed() / 10_000);
    }
}
