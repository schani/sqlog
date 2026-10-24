//! A versioned in-memory page store, exposed to SQLite as a VFS.
//!
//! Version `v` of a store is the database after its first `v` effects. The executor
//! connection (`exec` view) reads the latest version and writes into a pending overlay;
//! committing the overlay creates the next version. Query connections (`snap` view) pin the
//! latest version when SQLite takes its shared lock, so a read transaction sees one version
//! even while new ones are installed.

use std::collections::{BTreeMap, HashMap};
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::ptr;
use std::sync::{Arc, LazyLock, Mutex, Once};

use rusqlite::ffi;
use xxhash_rust::xxh3::Xxh3;

pub const PAGE_SIZE: usize = 4096;
pub const VFS_NAME: &str = "sqlog";

pub type Page = Arc<[u8]>;

struct Inner {
    /// Page number (1-based) to versions, ascending.
    versions: HashMap<u32, Vec<(u64, Page)>>,
    /// Database size in pages per version, ascending.
    sizes: Vec<(u64, u32)>,
    latest: u64,
    pending: HashMap<u32, Page>,
    pending_size: Option<u32>,
    /// Pinned version to number of pins.
    pins: BTreeMap<u64, usize>,
}

pub struct PageStore {
    inner: Mutex<Inner>,
}

impl Inner {
    fn page_at(&self, pgno: u32, v: u64) -> Option<&Page> {
        let vs = self.versions.get(&pgno)?;
        let i = vs.partition_point(|(s, _)| *s <= v);
        (i > 0).then(|| &vs[i - 1].1)
    }

    fn size_at(&self, v: u64) -> u32 {
        let i = self.sizes.partition_point(|(s, _)| *s <= v);
        self.sizes[i - 1].1
    }

    /// `None` is the executor's view: pending overlay over the latest version.
    fn page(&self, pgno: u32, view: Option<u64>) -> Option<&Page> {
        match view {
            None => self.pending.get(&pgno).or_else(|| self.page_at(pgno, self.latest)),
            Some(v) => self.page_at(pgno, v),
        }
    }

    fn size(&self, view: Option<u64>) -> u32 {
        match view {
            None => self.pending_size.unwrap_or_else(|| self.size_at(self.latest)),
            Some(v) => self.size_at(v),
        }
    }
}

impl PageStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner {
                versions: HashMap::new(),
                sizes: vec![(0, 0)],
                latest: 0,
                pending: HashMap::new(),
                pending_size: None,
                pins: BTreeMap::new(),
            }),
        })
    }

    pub fn latest(&self) -> u64 {
        self.inner.lock().unwrap().latest
    }

    /// The latest committed version of a page.
    pub fn latest_page(&self, pgno: u32) -> Option<Page> {
        let s = self.inner.lock().unwrap();
        s.page_at(pgno, s.latest).cloned()
    }

    /// Reads `out.len()` bytes at `offset`. Returns false if the read extends past the end
    /// of the database; missing bytes are zero.
    fn read(&self, view: Option<u64>, offset: usize, out: &mut [u8]) -> bool {
        let s = self.inner.lock().unwrap();
        let size_bytes = s.size(view) as usize * PAGE_SIZE;
        let mut pos = 0;
        while pos < out.len() {
            let at = offset + pos;
            let pgno = (at / PAGE_SIZE) as u32 + 1;
            let within = at % PAGE_SIZE;
            let n = (PAGE_SIZE - within).min(out.len() - pos);
            match s.page(pgno, view).filter(|_| at < size_bytes) {
                Some(p) => out[pos..pos + n].copy_from_slice(&p[within..within + n]),
                None => out[pos..pos + n].fill(0),
            }
            pos += n;
        }
        offset + out.len() <= size_bytes
    }

    fn write(&self, offset: usize, data: &[u8]) {
        let mut s = self.inner.lock().unwrap();
        let mut size = s.size(None);
        let mut pos = 0;
        while pos < data.len() {
            let at = offset + pos;
            let pgno = (at / PAGE_SIZE) as u32 + 1;
            let within = at % PAGE_SIZE;
            let n = (PAGE_SIZE - within).min(data.len() - pos);
            let mut page = if within == 0 && n == PAGE_SIZE {
                vec![0; PAGE_SIZE]
            } else {
                s.page(pgno, None).map(|p| p.to_vec()).unwrap_or_else(|| vec![0; PAGE_SIZE])
            };
            page[within..within + n].copy_from_slice(&data[pos..pos + n]);
            s.pending.insert(pgno, page.into());
            size = size.max(pgno);
            pos += n;
        }
        s.pending_size = Some(size);
    }

    fn truncate(&self, pages: u32) {
        let mut s = self.inner.lock().unwrap();
        s.pending.retain(|&p, _| p <= pages);
        s.pending_size = Some(pages);
    }

    fn file_size(&self, view: Option<u64>) -> u64 {
        self.inner.lock().unwrap().size(view) as u64 * PAGE_SIZE as u64
    }

    fn pin(&self) -> u64 {
        let mut s = self.inner.lock().unwrap();
        let v = s.latest;
        *s.pins.entry(v).or_default() += 1;
        v
    }

    fn unpin(&self, v: u64) {
        let mut s = self.inner.lock().unwrap();
        if let Some(n) = s.pins.get_mut(&v) {
            *n -= 1;
            if *n == 0 {
                s.pins.remove(&v);
            }
        }
    }

    /// The executor's uncommitted pages, sorted, and the resulting database size.
    pub fn pending(&self) -> (Vec<(u32, Page)>, u32) {
        let s = self.inner.lock().unwrap();
        let mut pages: Vec<_> = s.pending.iter().map(|(&p, d)| (p, d.clone())).collect();
        pages.sort_by_key(|(p, _)| *p);
        (pages, s.size(None))
    }

    pub fn pending_bytes(&self) -> usize {
        self.inner.lock().unwrap().pending.len() * PAGE_SIZE
    }

    pub fn discard_pending(&self) {
        let mut s = self.inner.lock().unwrap();
        s.pending.clear();
        s.pending_size = None;
    }

    /// Commits the pending overlay as the next version.
    pub fn commit_pending(&self) -> u64 {
        let (pages, size) = self.pending();
        self.discard_pending();
        self.install(pages, size)
    }

    /// Installs `pages` as the next version and drops versions no snapshot can see.
    pub fn install(&self, pages: Vec<(u32, Page)>, size: u32) -> u64 {
        let mut s = self.inner.lock().unwrap();
        let v = s.latest + 1;
        s.latest = v;
        s.sizes.push((v, size));
        let floor = s.pins.keys().next().copied().unwrap_or(v).min(v);
        for (pgno, page) in pages {
            let vs = s.versions.entry(pgno).or_default();
            vs.push((v, page));
            let keep_from = vs.partition_point(|(s, _)| *s <= floor).saturating_sub(1);
            vs.drain(..keep_from);
        }
        let keep_from = s.sizes.partition_point(|(s, _)| *s <= floor).saturating_sub(1);
        s.sizes.drain(..keep_from);
        v
    }

    /// Digest of the whole database at the latest version.
    pub fn digest(&self) -> u64 {
        let s = self.inner.lock().unwrap();
        let size = s.size_at(s.latest);
        let mut h = Xxh3::new();
        h.update(&size.to_le_bytes());
        for pgno in 1..=size {
            match s.page_at(pgno, s.latest) {
                Some(p) => h.update(p),
                None => h.update(&[0; PAGE_SIZE]),
            }
        }
        h.digest()
    }
}

// --- VFS -------------------------------------------------------------------------------

static STORES: LazyLock<Mutex<HashMap<String, Arc<PageStore>>>> = LazyLock::new(Default::default);
static REGISTER: Once = Once::new();
const PREFIX: &str = "sqlog:";

/// Makes `store` openable as `path(name, exec)` through the `sqlog` VFS.
pub fn register_store(name: &str, store: Arc<PageStore>) {
    register_vfs();
    STORES.lock().unwrap().insert(name.to_string(), store);
}

pub fn path(name: &str, exec: bool) -> String {
    format!("{PREFIX}{name}:{}", if exec { "exec" } else { "snap" })
}

fn register_vfs() {
    REGISTER.call_once(|| unsafe {
        let default = ffi::sqlite3_vfs_find(ptr::null());
        assert!(!default.is_null(), "no default VFS");
        let mut vfs = *default;
        vfs.zName = CString::new(VFS_NAME).unwrap().into_raw();
        vfs.szOsFile = (*default).szOsFile.max(size_of::<File>() as c_int);
        vfs.pAppData = default as *mut c_void;
        vfs.pNext = ptr::null_mut();
        vfs.xOpen = Some(x_open);
        vfs.xDelete = Some(x_delete);
        vfs.xAccess = Some(x_access);
        vfs.xFullPathname = Some(x_full_pathname);
        let rc = ffi::sqlite3_vfs_register(Box::into_raw(Box::new(vfs)), 0);
        assert_eq!(rc, ffi::SQLITE_OK, "registering VFS");
    });
}

#[repr(C)]
struct File {
    base: ffi::sqlite3_file,
    store: *const PageStore,
    exec: bool,
    pin: Option<u64>,
    lock: c_int,
}

static IO_METHODS: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 1,
    xClose: Some(x_close),
    xRead: Some(x_read),
    xWrite: Some(x_write),
    xTruncate: Some(x_truncate),
    xSync: Some(x_sync),
    xFileSize: Some(x_file_size),
    xLock: Some(x_lock),
    xUnlock: Some(x_unlock),
    xCheckReservedLock: Some(x_check_reserved_lock),
    xFileControl: Some(x_file_control),
    xSectorSize: Some(x_sector_size),
    xDeviceCharacteristics: Some(x_device_characteristics),
    xShmMap: None,
    xShmLock: None,
    xShmBarrier: None,
    xShmUnmap: None,
    xFetch: None,
    xUnfetch: None,
};

unsafe fn ours(name: *const c_char) -> Option<&'static str> {
    if name.is_null() {
        return None;
    }
    unsafe { CStr::from_ptr(name) }.to_str().ok().filter(|n| n.starts_with(PREFIX)).map(|n| unsafe { &*(n as *const str) })
}

unsafe fn default_vfs(vfs: *mut ffi::sqlite3_vfs) -> *mut ffi::sqlite3_vfs {
    unsafe { (*vfs).pAppData as *mut ffi::sqlite3_vfs }
}

unsafe extern "C" fn x_open(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    file: *mut ffi::sqlite3_file,
    flags: c_int,
    out_flags: *mut c_int,
) -> c_int {
    unsafe {
        if let Some(n) = ours(name).filter(|_| flags & ffi::SQLITE_OPEN_MAIN_DB != 0) {
            let rest = &n[PREFIX.len()..];
            let Some((store_name, view)) = rest.rsplit_once(':') else { return ffi::SQLITE_CANTOPEN };
            let Some(store) = STORES.lock().unwrap().get(store_name).cloned() else { return ffi::SQLITE_CANTOPEN };
            ptr::write(
                file as *mut File,
                File {
                    base: ffi::sqlite3_file { pMethods: &IO_METHODS },
                    store: Arc::into_raw(store),
                    exec: view == "exec",
                    pin: None,
                    lock: ffi::SQLITE_LOCK_NONE,
                },
            );
            if !out_flags.is_null() {
                *out_flags = flags;
            }
            return ffi::SQLITE_OK;
        }
        let d = default_vfs(vfs);
        ((*d).xOpen.unwrap())(d, name, file, flags, out_flags)
    }
}

unsafe extern "C" fn x_delete(vfs: *mut ffi::sqlite3_vfs, name: *const c_char, sync_dir: c_int) -> c_int {
    unsafe {
        if ours(name).is_some() {
            return ffi::SQLITE_OK;
        }
        let d = default_vfs(vfs);
        ((*d).xDelete.unwrap())(d, name, sync_dir)
    }
}

unsafe extern "C" fn x_access(vfs: *mut ffi::sqlite3_vfs, name: *const c_char, flags: c_int, res: *mut c_int) -> c_int {
    unsafe {
        if ours(name).is_some() {
            // Journals and WAL files of our databases never exist.
            *res = 0;
            return ffi::SQLITE_OK;
        }
        let d = default_vfs(vfs);
        ((*d).xAccess.unwrap())(d, name, flags, res)
    }
}

unsafe extern "C" fn x_full_pathname(vfs: *mut ffi::sqlite3_vfs, name: *const c_char, n_out: c_int, out: *mut c_char) -> c_int {
    unsafe {
        if ours(name).is_some() {
            let bytes = CStr::from_ptr(name).to_bytes_with_nul();
            if bytes.len() > n_out as usize {
                return ffi::SQLITE_CANTOPEN;
            }
            ptr::copy_nonoverlapping(bytes.as_ptr() as *const c_char, out, bytes.len());
            return ffi::SQLITE_OK;
        }
        let d = default_vfs(vfs);
        ((*d).xFullPathname.unwrap())(d, name, n_out, out)
    }
}

unsafe fn file<'a>(f: *mut ffi::sqlite3_file) -> &'a mut File {
    unsafe { &mut *(f as *mut File) }
}

unsafe fn store<'a>(f: &File) -> &'a PageStore {
    unsafe { &*f.store }
}

fn view(f: &File) -> Option<u64> {
    if f.exec { None } else { Some(f.pin.unwrap_or_else(|| unsafe { store(f) }.latest())) }
}

unsafe extern "C" fn x_close(f: *mut ffi::sqlite3_file) -> c_int {
    unsafe {
        let f = file(f);
        if let Some(v) = f.pin.take() {
            store(f).unpin(v);
        }
        drop(Arc::from_raw(f.store));
    }
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_read(f: *mut ffi::sqlite3_file, buf: *mut c_void, amt: c_int, off: i64) -> c_int {
    unsafe {
        let f = file(f);
        let out = std::slice::from_raw_parts_mut(buf as *mut u8, amt as usize);
        if store(f).read(view(f), off as usize, out) { ffi::SQLITE_OK } else { ffi::SQLITE_IOERR_SHORT_READ }
    }
}

unsafe extern "C" fn x_write(f: *mut ffi::sqlite3_file, buf: *const c_void, amt: c_int, off: i64) -> c_int {
    unsafe {
        let f = file(f);
        if !f.exec {
            return ffi::SQLITE_READONLY;
        }
        store(f).write(off as usize, std::slice::from_raw_parts(buf as *const u8, amt as usize));
    }
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_truncate(f: *mut ffi::sqlite3_file, size: i64) -> c_int {
    unsafe {
        let f = file(f);
        if !f.exec {
            return ffi::SQLITE_READONLY;
        }
        store(f).truncate((size as usize).div_ceil(PAGE_SIZE) as u32);
    }
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_sync(_: *mut ffi::sqlite3_file, _: c_int) -> c_int {
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_file_size(f: *mut ffi::sqlite3_file, size: *mut i64) -> c_int {
    unsafe {
        let f = file(f);
        *size = store(f).file_size(view(f)) as i64;
    }
    ffi::SQLITE_OK
}

/// Taking the shared lock starts a read transaction: pin the snapshot it will see.
unsafe extern "C" fn x_lock(f: *mut ffi::sqlite3_file, level: c_int) -> c_int {
    unsafe {
        let f = file(f);
        if !f.exec && f.lock == ffi::SQLITE_LOCK_NONE && level >= ffi::SQLITE_LOCK_SHARED {
            f.pin = Some(store(f).pin());
        }
        f.lock = f.lock.max(level);
    }
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_unlock(f: *mut ffi::sqlite3_file, level: c_int) -> c_int {
    unsafe {
        let f = file(f);
        if level == ffi::SQLITE_LOCK_NONE
            && let Some(v) = f.pin.take()
        {
            store(f).unpin(v);
        }
        f.lock = level;
    }
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_check_reserved_lock(_: *mut ffi::sqlite3_file, res: *mut c_int) -> c_int {
    unsafe { *res = 0 };
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_file_control(_: *mut ffi::sqlite3_file, _: c_int, _: *mut c_void) -> c_int {
    ffi::SQLITE_NOTFOUND
}

unsafe extern "C" fn x_sector_size(_: *mut ffi::sqlite3_file) -> c_int {
    PAGE_SIZE as c_int
}

unsafe extern "C" fn x_device_characteristics(_: *mut ffi::sqlite3_file) -> c_int {
    0
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::{Connection, OpenFlags};

    fn exec(name: &str) -> Connection {
        let c = Connection::open_with_flags_and_vfs(path(name, true), OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE, VFS_NAME).unwrap();
        c.execute_batch("PRAGMA page_size=4096; PRAGMA journal_mode=MEMORY;").unwrap();
        c
    }

    fn snap(name: &str) -> Connection {
        Connection::open_with_flags_and_vfs(path(name, false), OpenFlags::SQLITE_OPEN_READ_ONLY, VFS_NAME).unwrap()
    }

    #[test]
    fn snapshots_see_committed_versions_only() {
        let store = PageStore::new();
        register_store("t1", store.clone());
        let w = exec("t1");
        w.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (1);").unwrap();
        assert_eq!(store.commit_pending(), 1);

        let r = snap("t1");
        let count = |c: &Connection| c.query_row("SELECT count(*) FROM t", [], |row| row.get::<_, i64>(0)).unwrap();
        assert_eq!(count(&r), 1);

        w.execute_batch("INSERT INTO t VALUES (2);").unwrap();
        assert_eq!(count(&r), 1, "pending pages are invisible to snapshots");
        store.commit_pending();
        assert_eq!(count(&r), 2, "next read transaction sees the new version");

        // A read transaction keeps its snapshot while new versions are installed.
        r.execute_batch("BEGIN").unwrap();
        assert_eq!(count(&r), 2);
        w.execute_batch("INSERT INTO t VALUES (3);").unwrap();
        store.commit_pending();
        assert_eq!(count(&r), 2);
        r.execute_batch("COMMIT").unwrap();
        assert_eq!(count(&r), 3);
    }

    #[test]
    fn discarded_pending_disappears() {
        let store = PageStore::new();
        register_store("t2", store.clone());
        let w = exec("t2");
        w.execute_batch("CREATE TABLE t(x);").unwrap();
        store.commit_pending();
        w.execute_batch("INSERT INTO t VALUES (1);").unwrap();
        store.discard_pending();
        drop(w);
        let w = exec("t2");
        assert_eq!(w.query_row("SELECT count(*) FROM t", [], |row| row.get::<_, i64>(0)).unwrap(), 0);
    }
}
